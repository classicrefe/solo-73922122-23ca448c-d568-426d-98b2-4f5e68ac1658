"""一次性扩容端到端测试。

使用独立的小容量卷（初始容量 4，深度 2；扩容后 8，深度 3），
完整覆盖：
  * 未满拒绝扩容、发满后第 5 件 409；
  * 扩容只生效一次（并发交错下仍只翻倍一次）、不占序号不写检查点；
  * 扩容后从第 5 件续发，旧检查点条数/根摘要/旧证明保持有效；
  * 跨扩容追加证明可离线核验，条目数 O(log n)；
  * 扩容事务失败时容量与历史不变、可重试；
  * 重启（新 Database 实例挂同卷）后容量状态保留、自检通过。
"""
from __future__ import annotations

import io
import json
import os
import tempfile
import threading
from pathlib import Path

import pytest

from app import merkle
from app.config import Settings
from app.db import ConflictError, Database
from app.storage import BlobStore
from app.verify import verify_append_proof, verify_release_bytes

INITIAL_CAPACITY = 4
EXPANDED_CAPACITY = 8


def _csv(seed: str) -> bytes:
    return f"station,time,ph\n{seed},2026-11-16T08:00,7.2\n".encode()


@pytest.fixture()
def volume(tmp_path):
    data_dir = tmp_path / "vol"
    data_dir.mkdir()
    settings = Settings(
        data_dir=data_dir,
        db_path=data_dir / "release.db",
        blob_dir=data_dir / "blobs",
        temp_dir=data_dir / "tmp",
        max_file_bytes=64 * 1024,
        max_releases=INITIAL_CAPACITY,
        tree_depth=2,
    )
    store = BlobStore(settings)
    db = Database(settings)
    yield settings, store, db, data_dir


def _publish(db: Database, store: BlobStore, idx: int) -> dict:
    data = _csv(f"S{idx:03d}")
    digest, _ = store.store(data)
    record, reused = db.publish(
        f"ST{idx:03d}", "2026-11-16", f"ORD-{idx:03d}", digest, len(data)
    )
    assert reused is False
    return record


def test_capacity_state_and_refuse_when_not_full(volume):
    settings, store, db, _ = volume
    state = db.get_capacity_state()
    assert state == {
        "capacity": INITIAL_CAPACITY,
        "initial_capacity": INITIAL_CAPACITY,
        "tree_depth": 2,
        "release_count": 0,
    }
    _publish(db, store, 1)
    # 未满即扩容 -> 冲突，容量与历史保持原状。
    with pytest.raises(ConflictError) as exc:
        db.expand_capacity()
    assert "未满" in str(exc.value)
    assert db.get_capacity_state()["capacity"] == INITIAL_CAPACITY
    assert len(db.list_releases()) == 1


def test_full_then_expand_once_and_continue(volume):
    settings, store, db, _ = volume
    # 发满 4 件。
    for i in range(1, INITIAL_CAPACITY + 1):
        _publish(db, store, i)
    assert db.get_capacity_state()["release_count"] == 4

    # 发满后第 5 件在扩容前被拒绝。
    data5 = _csv("S005")
    digest5, _ = store.store(data5)
    with pytest.raises(ConflictError):
        db.publish("ST005", "2026-11-16", "ORD-005", digest5, len(data5))

    # 扩容前留存：旧检查点（条数与根）与旧纳入证明。
    cp_old = db.get_checkpoint(4)
    assert cp_old["tree_depth"] == 2
    leaves_before = db.leaf_hashes()
    old_path = [
        s.to_dict() for s in merkle.inclusion_path(0, leaves_before, 2)
    ]
    checkpoint_count_before = len(db.list_checkpoints())

    result = db.expand_capacity()
    assert result == {
        "capacity": EXPANDED_CAPACITY,
        "previous_capacity": INITIAL_CAPACITY,
        "tree_depth": 3,
        "release_count": 4,
        "expanded": True,
    }

    # 扩容不占序号、不写检查点：检查点数量不变，4 号旧检查点原样保留。
    assert len(db.list_checkpoints()) == checkpoint_count_before
    cp_old_after = db.get_checkpoint(4)
    assert cp_old_after == cp_old
    assert db.get_capacity_state() == {
        "capacity": EXPANDED_CAPACITY,
        "initial_capacity": INITIAL_CAPACITY,
        "tree_depth": 3,
        "release_count": 4,
    }

    # 一次性：再次扩容被拒，状态不变。
    with pytest.raises(ConflictError) as exc:
        db.expand_capacity()
    assert "不可重复" in str(exc.value)
    assert db.get_capacity_state()["capacity"] == EXPANDED_CAPACITY

    # 从第 5 件续发，序号稠密接续。
    record5, reused = db.publish(
        "ST005", "2026-11-16", "ORD-005", digest5, len(data5)
    )
    assert reused is False and record5["id"] == 5
    cp5 = db.get_checkpoint(5)
    assert cp5["tree_depth"] == 3 and cp5["seq"] == 5

    # 旧检查点根摘要在新树下仍可按深度 2 复算（旧证明持续有效）。
    leaves_now = db.leaf_hashes()
    assert (
        merkle.root_from_leaves(leaves_now[:4], 2).hex() == cp_old["root_hex"]
    )
    # 扩容前留存的纳入证明仍能离线核验（深度 2 绑定在证明里）。
    old_proof = {
        "index": 0,
        "checkpoint_seq": 4,
        "checkpoint_root": cp_old["root_hex"],
        "tree_depth": 2,
        "leaf_record": {
            "v": merkle.LEAF_VERSION,
            "station_id": "ST001",
            "sample_date": "2026-11-16",
            "order_no": "ORD-001",
            "blob_sha256": merkle.sha256_hex(_csv("S001")),
            "size": len(_csv("S001")),
        },
        "leaf_hash": leaves_now[0].hex(),
        "path": old_path,
    }
    ok, msg = verify_release_bytes(old_proof, _csv("S001"))
    assert ok, msg

    # 扩容后新叶子（第 5 件）的纳入证明为深度 3，可离线核验。
    new_path = [
        s.to_dict() for s in merkle.inclusion_path(4, leaves_now, 3)
    ]
    assert len(new_path) == 3
    new_proof = {
        "index": 4,
        "checkpoint_seq": 5,
        "checkpoint_root": cp5["root_hex"],
        "tree_depth": 3,
        "leaf_record": {
            "v": merkle.LEAF_VERSION,
            "station_id": "ST005",
            "sample_date": "2026-11-16",
            "order_no": "ORD-005",
            "blob_sha256": digest5,
            "size": len(data5),
        },
        "leaf_hash": leaves_now[4].hex(),
        "path": new_path,
    }
    ok, msg = verify_release_bytes(new_proof, data5)
    assert ok, msg


def test_cross_expansion_append_proof_offline(volume):
    settings, store, db, _ = volume
    for i in range(1, INITIAL_CAPACITY + 1):
        _publish(db, store, i)
    cp4 = db.get_checkpoint(4)
    db.expand_capacity()
    # 续发 3 件，使新树深度 3 下条数达到 7。
    for i in range(5, 8):
        _publish(db, store, i)
    leaves = db.leaf_hashes()
    latest = db.latest_checkpoint()
    assert latest["seq"] == 7 and latest["tree_depth"] == 3

    # 跨扩容追加证明：旧检查点深度 2 -> 新检查点深度 3。
    entries = merkle.append_proof(leaves, 4, 3)
    proof = {
        "old_seq": 4,
        "new_seq": 7,
        "old_root": cp4["root_hex"],
        "new_root": latest["root_hex"],
        "tree_depth": 3,
        "old_tree_depth": 2,
        "entries": entries,
    }
    ok, msg = verify_append_proof(cp4, latest, proof)
    assert ok, msg
    # 条目数保持 O(log n)：上界 2 * 新深度。
    assert len(entries) <= 2 * 3

    # 纯函数层直接复核跨深度关系。
    assert merkle.verify_append(
        old_root=bytes.fromhex(cp4["root_hex"]),
        new_root=bytes.fromhex(latest["root_hex"]),
        old_count=4,
        new_count=7,
        entries=entries,
        depth=3,
        old_depth=2,
    )
    # 篡改旧根 -> 离线拒绝。
    forged = dict(cp4)
    forged["root_hex"] = "00" * 32
    ok2, _ = verify_append_proof(forged, latest, proof)
    assert not ok2

    # 旧检查点 0/1/2/3（都在扩容前深度 2）到最新的证明同样跨深度可验。
    for old_seq in range(4):
        old_cp = db.get_checkpoint(old_seq)
        e = merkle.append_proof(leaves, old_seq, 3)
        p = {**proof, "old_seq": old_seq, "old_root": old_cp["root_hex"],
             "entries": e, "old_tree_depth": 2}
        ok, msg = verify_append_proof(old_cp, latest, p)
        assert ok, f"old_seq={old_seq}: {msg}"

    # 扩容后同深度（深度 3 -> 深度 3）的追加证明仍走原路径。
    cp5 = db.get_checkpoint(5)
    e_same = merkle.append_proof(leaves, 5, 3)
    p_same = {
        "old_seq": 5, "new_seq": 7,
        "old_root": cp5["root_hex"], "new_root": latest["root_hex"],
        "tree_depth": 3, "old_tree_depth": 3, "entries": e_same,
    }
    ok, msg = verify_append_proof(cp5, latest, p_same)
    assert ok, msg

    # 扩容点边界：旧检查点（深度 2、条数 4）与新视角同条数、深度 3。
    e_point = merkle.append_proof(leaves[:4], 4, 3, old_depth=2)
    assert e_point  # 携带锚点条目而非空证明
    root_new_same = merkle.root_from_leaves(leaves[:4], 3)
    assert merkle.verify_append(
        old_root=bytes.fromhex(cp4["root_hex"]),
        new_root=root_new_same,
        old_count=4,
        new_count=4,
        entries=e_point,
        depth=3,
        old_depth=2,
    )


def test_concurrent_expand_only_takes_effect_once(volume):
    settings, store, db, data_dir = volume
    for i in range(1, INITIAL_CAPACITY + 1):
        _publish(db, store, i)

    # 两个独立 Database 实例（模拟两个服务进程）挂同一卷，并发扩容。
    db_a = Database(settings)
    db_b = Database(settings)
    outcomes: list[dict | str] = []
    barrier = threading.Barrier(2)

    def worker(which: Database):
        barrier.wait()
        try:
            outcomes.append(("ok", which.expand_capacity()))
        except ConflictError as exc:
            outcomes.append(("conflict", str(exc)))

    t1 = threading.Thread(target=worker, args=(db_a,))
    t2 = threading.Thread(target=worker, args=(db_b,))
    t1.start(); t2.start(); t1.join(); t2.join()

    statuses = sorted(o[0] for o in outcomes)
    assert statuses == ["conflict", "ok"]
    ok_result = next(o[1] for o in outcomes if o[0] == "ok")
    assert ok_result["capacity"] == EXPANDED_CAPACITY
    # 卷上只有一次翻倍，没有任何检查点/序号副作用。
    assert db.get_capacity_state()["capacity"] == EXPANDED_CAPACITY
    assert db.latest_checkpoint()["seq"] == INITIAL_CAPACITY
    assert len(db.list_checkpoints()) == INITIAL_CAPACITY + 1


def test_failed_expansion_keeps_state_and_is_retryable(volume, monkeypatch):
    settings, store, db, _ = volume
    for i in range(1, INITIAL_CAPACITY + 1):
        _publish(db, store, i)
    releases_before = db.list_releases()
    cp_before = db.list_checkpoints()
    capacity_before = db.get_capacity_state()

    # 强制 UPDATE 抛错，模拟扩容事务中途失败：必须整体回滚。
    import sqlite3
    from contextlib import contextmanager

    real_connect = db.connect

    class FailingConn:
        """仅让 UPDATE meta 语句失败，其余语句透传的连接包装。"""

        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *args, **kwargs):
            if sql.lstrip().upper().startswith("UPDATE META"):
                raise sqlite3.OperationalError("simulated I/O error")
            return self._inner.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    @contextmanager
    def failing_connect(*_args, **_kwargs):
        with real_connect() as inner:
            yield FailingConn(inner)

    monkeypatch.setattr(db, "connect", failing_connect)
    with pytest.raises(sqlite3.OperationalError):
        db.expand_capacity()
    monkeypatch.undo()

    # 容量、发布历史、检查点全部保持原状。
    assert db.get_capacity_state() == capacity_before
    assert db.list_releases() == releases_before
    assert db.list_checkpoints() == cp_before

    # 可原样重试并成功。
    result = db.expand_capacity()
    assert result["expanded"] is True
    assert db.get_capacity_state()["capacity"] == EXPANDED_CAPACITY


def test_restart_recovers_expanded_capacity_and_passes_integrity(volume):
    settings, store, db, data_dir = volume
    for i in range(1, INITIAL_CAPACITY + 1):
        _publish(db, store, i)
    db.expand_capacity()
    _publish(db, store, 5)
    _publish(db, store, 6)

    # 模拟实例重启：全新 Settings/Database/BlobStore 挂同一共享卷。
    restarted_settings = Settings(
        data_dir=data_dir,
        db_path=data_dir / "release.db",
        blob_dir=data_dir / "blobs",
        temp_dir=data_dir / "tmp",
        max_file_bytes=64 * 1024,
        max_releases=INITIAL_CAPACITY,  # 环境变量仍是初始值
        tree_depth=2,
    )
    restarted_store = BlobStore(restarted_settings)
    restarted_db = Database(restarted_settings)

    state = restarted_db.get_capacity_state()
    assert state["capacity"] == EXPANDED_CAPACITY
    assert state["tree_depth"] == 3
    assert state["release_count"] == 6
    # 自检通过：旧检查点按深度 2、新检查点按深度 3 全部复算一致。
    assert restarted_db.integrity_problems(restarted_store.verify_present) == []
    # 重启后可继续追加到扩容上限（第 7、8 件）。
    _publish(restarted_db, restarted_store, 7)
    _publish(restarted_db, restarted_store, 8)
    final_state = restarted_db.get_capacity_state()
    assert final_state["capacity"] - final_state["release_count"] == 0
    with pytest.raises(ConflictError):
        data9 = _csv("S009")
        d9, _ = restarted_store.store(data9)
        restarted_db.publish("ST009", "2026-11-16", "ORD-009", d9, len(data9))
    # 重启视角下再次扩容仍是一次性、被拒绝。
    with pytest.raises(ConflictError):
        restarted_db.expand_capacity()


def test_merkle_cross_depth_proof_size_is_logarithmic():
    # 128 -> 256 的真实规模：证明条目数仍随条数对数增长（上界 2*深度）。
    depth_old, depth_new = 7, 8
    leaves = [
        merkle.leaf_hash(f"S{i:03d}", "2026-11-16", f"ORD-{i}", os.urandom(8).hex(), 10)
        for i in range(200)
    ]
    roots_old = merkle.root_from_leaves(leaves[:128], depth_old)
    roots_new = merkle.root_from_leaves(leaves, depth_new)
    entries = merkle.append_proof(leaves, 128, depth_new)
    assert len(entries) <= 2 * depth_new
    assert merkle.verify_append(
        old_root=roots_old,
        new_root=roots_new,
        old_count=128,
        new_count=200,
        entries=entries,
        depth=depth_new,
        old_depth=depth_old,
    )
    # 满 256 槽、旧条数为 0 的极端情形也成立。
    full = [
        merkle.leaf_hash(f"F{i:03d}", "2026-11-16", f"O{i}", os.urandom(4).hex(), 1)
        for i in range(256)
    ]
    e0 = merkle.append_proof(full, 0, depth_new)
    assert merkle.verify_append(
        old_root=merkle.empty_root(depth_old),
        new_root=merkle.root_from_leaves(full, depth_new),
        old_count=0,
        new_count=256,
        entries=e0,
        depth=depth_new,
        old_depth=depth_old,
    )
    assert len(e0) <= 2 * depth_new
