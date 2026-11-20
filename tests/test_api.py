"""API 端到端测试：发布、幂等、冲突拒绝、限额、取件、检查点、两类证明、离线核验。"""
from __future__ import annotations

import hashlib
import io

from app import merkle
from app.verify import verify_append_proof, verify_release_bytes


def _csv(seed: str, size: int | None = None) -> bytes:
    payload = f"station,time,ph,do\n{seed},2026-11-16T08:00,7.2,8.1\n".encode()
    if size is not None:
        payload = payload + b"#" * max(0, size - len(payload))
    return payload


def _publish(client, station, day, order, data, **extra):
    return client.post(
        "/releases",
        data={"station_id": station, "sample_date": day, "order_no": order, **extra},
        files={"file": ("day.csv", io.BytesIO(data), "text/csv")},
    )


def test_health_and_zero_checkpoint(app_client):
    client, _, settings = app_client
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"
    cp = client.get("/checkpoints/0").json()
    assert cp["seq"] == 0
    assert cp["root_hex"] == merkle.empty_root(settings.tree_depth).hex()


def test_publish_creates_release_and_checkpoint(app_client):
    client, _, settings = app_client
    data = _csv("S1")
    r = _publish(client, "ST01", "2026-11-16", "ORD-1", data)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["reused"] is False
    assert body["release"]["id"] == 1
    assert body["release"]["blob_sha256"] == hashlib.sha256(data).hexdigest()
    assert body["checkpoint"]["seq"] == 1
    assert client.get("/checkpoints/latest").json()["seq"] == 1
    assert len(client.get("/checkpoints").json()) == 2  # 0 号 + 1 号


def test_idempotent_retry_same_input(app_client):
    client, _, _ = app_client
    data = _csv("S2")
    r1 = _publish(client, "ST02", "2026-11-16", "ORD-2", data)
    r2 = _publish(client, "ST02", "2026-11-16", "ORD-2", data)
    assert r1.status_code == 201 and r2.status_code == 201
    assert r1.json()["reused"] is False and r2.json()["reused"] is True
    assert r1.json()["release"] == r2.json()["release"]
    assert client.get("/releases").json().__len__() == 2  # 累计不重复计数


def test_conflicting_input_rejected(app_client):
    client, _, _ = app_client
    _publish(client, "ST03", "2026-11-16", "ORD-3", _csv("a"))
    r = _publish(client, "ST03", "2026-11-16", "ORD-3", _csv("b-different"))
    assert r.status_code == 409
    # 原记录未被改写。
    rec = client.get("/releases/ST03/2026-11-16/ORD-3").json()
    assert rec["blob_sha256"] == hashlib.sha256(_csv("a")).hexdigest()


def test_size_limits_and_validation(app_client):
    client, _, settings = app_client
    # 超 1 MiB 等价配置（测试环境上限 64 KiB）。
    too_big = b"x" * (settings.max_file_bytes + 1)
    r = _publish(client, "ST04", "2026-11-16", "ORD-4", too_big)
    assert r.status_code == 413
    # 空文件。
    assert _publish(client, "ST04", "2026-11-16", "ORD-4", b"").status_code == 422
    # 非法日期与标识。
    assert _publish(
        client, "ST04", "2026-02-30", "ORD-4", _csv("x")
    ).status_code == 422
    assert _publish(
        client, "bad station!", "2026-11-16", "ORD-4", _csv("x")
    ).status_code == 422


def test_download_matches_original_and_404(app_client):
    client, _, _ = app_client
    data = _csv("DOWNLOAD", size=2048)
    _publish(client, "STD", "2026-11-16", "ORD-DL", data)
    r = client.get("/releases/STD/2026-11-16/ORD-DL/download")
    assert r.status_code == 200
    assert r.content == data
    assert hashlib.sha256(r.content).hexdigest() == hashlib.sha256(data).hexdigest()
    assert client.get("/releases/NOPE/2026-11-16/X/download").status_code == 404


def test_inclusion_proof_and_offline_verification(app_client, tmp_path):
    client, _, settings = app_client
    data = _csv("PROOF", size=512)
    _publish(client, "STP", "2026-11-16", "ORD-P", data)
    r = client.get("/releases/STP/2026-11-16/ORD-P/inclusion-proof")
    assert r.status_code == 200
    proof = r.json()
    assert len(proof["path"]) == settings.tree_depth
    # 路径大小 O(log n)：128 件规模下恒为 7 步。
    # 离线核验：只凭证明 + 原件字节。
    ok, msg = verify_release_bytes(proof, data)
    assert ok, msg
    # 篡改原件必须被拒绝。
    tampered = data + b"altered"
    ok2, msg2 = verify_release_bytes(proof, tampered)
    assert not ok2 and "SHA-256" in msg2
    # 篡改证明中的站日归属字段也要被拒绝。
    forged = dict(proof)
    forged["leaf_record"] = {**proof["leaf_record"], "station_id": "FAKE"}
    ok3, _ = verify_release_bytes(forged, data)
    assert not ok3
    # 留存证明 JSON 文件可被 CLI 场景复用。
    proof_file = tmp_path / "inclusion.json"
    proof_file.write_text(__import__("json").dumps(proof))
    raw_file = tmp_path / "original.csv"
    raw_file.write_bytes(data)
    import subprocess

    proc = subprocess.run(
        [
            "python3",
            "-m",
            "app.verify",
            "inclusion",
            "--proof",
            str(proof_file),
            "--file",
            str(raw_file),
        ],
        capture_output=True,
        text=True,
        cwd=str(__import__("pathlib").Path(__file__).resolve().parents[1]),
    )
    assert proc.returncode == 0 and "通过" in proc.stdout


def test_append_proof_chain_and_offline_verification(app_client):
    client, _, settings = app_client
    # 再发布两件，使检查点继续增长。
    _publish(client, "STAP1", "2026-11-17", "ORD-A1", _csv("ap1"))
    _publish(client, "STAP2", "2026-11-17", "ORD-A2", _csv("ap2"))
    latest = client.get("/checkpoints/latest").json()
    for old_seq in (0, 1, 3, latest["seq"] - 1):
        old_cp = client.get(f"/checkpoints/{old_seq}").json()
        r = client.get(f"/checkpoints/{old_seq}/append-proof")
        assert r.status_code == 200
        proof = r.json()
        assert proof["old_seq"] == old_seq and proof["new_seq"] == latest["seq"]
        assert len(proof["entries"]) <= 2 * settings.tree_depth
        ok, msg = verify_append_proof(old_cp, latest, proof)
        assert ok, msg
    # 篡改旧检查点根 -> 离线拒绝。
    old_cp = client.get("/checkpoints/1").json()
    proof = client.get("/checkpoints/1/append-proof").json()
    forged = dict(old_cp)
    forged["root_hex"] = "00" * 32
    ok, _ = verify_append_proof(forged, latest, proof)
    assert not ok

def test_capacity_enforced(app_client, tmp_path):
    """发满 -> 409 -> 一次性扩容 -> 续发，使用独立临时卷，不污染会话数据。"""
    from fastapi.testclient import TestClient

    from app.config import Settings
    from app.db import Database
    from app.main import app
    from app.storage import BlobStore

    data_dir = tmp_path / "cap-vol"
    data_dir.mkdir()
    cap = 8  # 小容量快速发满，深度 3；扩容后 16，深度 4
    local_settings = Settings(
        data_dir=data_dir,
        db_path=data_dir / "release.db",
        blob_dir=data_dir / "blobs",
        temp_dir=data_dir / "tmp",
        max_file_bytes=app_client[2].max_file_bytes,
        max_releases=cap,
        tree_depth=3,
    )
    local_store = BlobStore(local_settings)
    local_db = Database(local_settings)

    # 直接通过 DB/存储层发满，避免起第二套 HTTP 应用。
    def db_publish(idx: int):
        data = _csv(f"cap-{idx}")
        digest, _ = local_store.store(data)
        return local_db.publish(
            f"CP{idx:02d}", "2026-11-16", f"ORD-C{idx:02d}", digest, len(data)
        )

    for i in range(1, cap + 1):
        record, reused = db_publish(i)
        assert reused is False and record["id"] == i
    assert local_db.get_capacity_state()["release_count"] == cap

    # 发满后下一件 409。
    overflow = _csv("overflow")
    d, _ = local_store.store(overflow)
    import pytest as _pytest

    from app.db import ConflictError

    with _pytest.raises(ConflictError) as exc:
        local_db.publish("CPX", "2026-11-16", "ORD-CX", d, len(overflow))
    assert "上限" in str(exc.value)

    # 未满拒绝由 test_expansion 覆盖；这里发满状态扩容成功 8 -> 16。
    result = local_db.expand_capacity()
    assert result["expanded"] is True
    assert result["capacity"] == 16 and result["previous_capacity"] == 8
    assert result["tree_depth"] == 4

    # 一次性：再次扩容被拒。
    with _pytest.raises(ConflictError) as exc2:
        local_db.expand_capacity()
    assert "不可重复" in str(exc2.value)

    # 扩容不占序号、不写检查点：最新检查点 seq 仍为 cap。
    assert local_db.latest_checkpoint()["seq"] == cap
    assert len(local_db.list_checkpoints()) == cap + 1

    # 扩容后从第 cap+1 件续发，新检查点深度为 4。
    record9, reused = db_publish(cap + 1)
    assert reused is False and record9["id"] == cap + 1
    cp9 = local_db.get_checkpoint(cap + 1)
    assert cp9["tree_depth"] == 4

    # 容量状态端点语义（通过独立 HTTP 应用验证 /capacity 与 /health）。
    import app.main as main_mod

    orig_store, orig_db = main_mod.blob_store, main_mod.database
    main_mod.blob_store, main_mod.database = local_store, local_db
    try:
        with TestClient(app) as client:
            state = client.get("/capacity").json()
            assert state == {
                "capacity": 16,
                "tree_depth": 4,
                "release_count": cap + 1,
                "expanded": True,
                "remaining": 16 - (cap + 1),
            }
            assert client.get("/health").json()["capacity"] == 16
            # HTTP 层扩容再次调用仍被拒。
            again = client.post("/capacity/expand")
            assert again.status_code == 409
    finally:
        main_mod.blob_store, main_mod.database = orig_store, orig_db

