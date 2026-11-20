"""SQLite 持久层：目录与摘要（不存原件字节）。

并发模型
--------
* WAL 模式 + ``busy_timeout``：两个实例共享同一库文件，读者不阻塞写者；
* 发布与扩容事务使用 ``BEGIN IMMEDIATE``，跨实例写操作被数据库锁串行化，
  后到者阻塞等待后重检唯一键/容量状态，因此不会分叉、不会互相覆盖。
* 无任何 UPDATE/DELETE 业务路径（除一次性扩容更新 ``meta`` 容量外），
  发布后记录不可改、不可删。

容量与树深度
------------
当前容量持久化在 ``meta`` 表（键 ``capacity``），初始等于环境变量
``MAX_RELEASES``（默认 128，深度 7）。一次性扩容把容量翻倍（256，深度 8），
状态落在共享卷上：任一实例重启都从卷上恢复，扩容不产生检查点、不占发布序号。
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from . import merkle
from .config import Settings, depth_for_capacity

SCHEMA = """
CREATE TABLE IF NOT EXISTS releases (
    id           INTEGER PRIMARY KEY,          -- 1 起稠密序号，叶子下标 = id - 1
    station_id   TEXT NOT NULL,
    sample_date  TEXT NOT NULL,
    order_no     TEXT NOT NULL,
    blob_sha256  TEXT NOT NULL,
    size         INTEGER NOT NULL,
    created_at   TEXT NOT NULL,
    UNIQUE(station_id, sample_date, order_no)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    seq         INTEGER PRIMARY KEY,            -- 该检查点对应的条数
    root_hex    TEXT NOT NULL,
    tree_depth  INTEGER NOT NULL,               -- 该检查点生成时的固定树深度
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
"""

CAPACITY_KEY = "capacity"
MAX_CAPACITY_KEY = "max_capacity"
INITIAL_CAPACITY_KEY = "initial_capacity"


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ConflictError(Exception):
    """业务冲突：异输入重试、超出累计上限或扩容前置条件不满足。"""


class Database:
    def __init__(self, settings: Settings):
        self._settings = settings
        settings.ensure_dirs()
        self._init_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(
            self._settings.db_path,
            timeout=15.0,
            isolation_level=None,  # 手动事务，显式 BEGIN/COMMIT
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 15000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executescript(SCHEMA)
                # 旧版本库迁移：checkpoints 补 tree_depth 列，历史检查点均为深度 7。
                columns = {
                    row["name"]
                    for row in conn.execute("PRAGMA table_info(checkpoints)")
                }
                if "tree_depth" not in columns:
                    conn.execute(
                        "ALTER TABLE checkpoints ADD COLUMN tree_depth "
                        "INTEGER NOT NULL DEFAULT 7"
                    )
                # 当前容量持久化；仅全新卷采用环境变量，已存在的卷不被覆盖。
                existing = conn.execute(
                    "SELECT value FROM meta WHERE key = ?", (CAPACITY_KEY,)
                ).fetchone()
                if existing is None:
                    conn.execute(
                        "INSERT INTO meta(key, value) VALUES (?, ?)",
                        (CAPACITY_KEY, str(self._settings.max_releases)),
                    )
                    # 初始容量随卷持久化，作为“是否已扩容”的稳定判据。
                    conn.execute(
                        "INSERT INTO meta(key, value) VALUES (?, ?)",
                        (
                            INITIAL_CAPACITY_KEY,
                            str(self._settings.max_releases),
                        ),
                    )
                    # 一次性扩容的硬性上限 = 初始容量翻倍，随卷持久化，
                    # 重启后环境变量变化也不能抬高它。
                    conn.execute(
                        "INSERT INTO meta(key, value) VALUES (?, ?)",
                        (
                            MAX_CAPACITY_KEY,
                            str(self._settings.max_releases * 2),
                        ),
                    )
                # 零号检查点（空树根），幂等创建，深度取当前卷容量对应深度。
                capacity = self._capacity_locked(conn)
                depth = depth_for_capacity(capacity)
                empty = merkle.empty_root(depth).hex()
                conn.execute(
                    "INSERT OR IGNORE INTO checkpoints(seq, root_hex, tree_depth, "
                    "created_at) VALUES (0, ?, ?, ?)",
                    (empty, depth, _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _capacity_locked(conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "SELECT value FROM meta WHERE key = ?", (CAPACITY_KEY,)
        ).fetchone()
        if row is None:  # pragma: no cover - 初始化后不可能缺失
            raise RuntimeError("容量状态缺失，数据库未正确初始化")
        return int(row["value"])

    @staticmethod
    def _max_capacity_locked(conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "SELECT value FROM meta WHERE key = ?", (MAX_CAPACITY_KEY,)
        ).fetchone()
        if row is None:  # pragma: no cover - 初始化后不可能缺失
            raise RuntimeError("扩容上限缺失，数据库未正确初始化")
        return int(row["value"])

    @staticmethod
    def _meta_int_locked(
        conn: sqlite3.Connection, key: str, fallback: int
    ) -> int:
        row = conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return int(row["value"]) if row is not None else fallback

    # ------------------------------------------------------------------ #
    # 容量状态与一次性扩容
    # ------------------------------------------------------------------ #
    def get_capacity_state(self) -> dict:
        """读取卷上持久化的当前容量、树深度、初始容量与已发布条数。"""
        with self.connect() as conn:
            capacity = self._capacity_locked(conn)
            initial = self._meta_int_locked(conn, INITIAL_CAPACITY_KEY, capacity)
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM releases"
            ).fetchone()["c"]
        return {
            "capacity": capacity,
            "initial_capacity": initial,
            "tree_depth": depth_for_capacity(capacity),
            "release_count": count,
        }

    def expand_capacity(self) -> dict:
        """一次性扩容：仅当已发满当前容量时把上限翻倍（128 -> 256）。

        串行化保证两个实例同时请求只有一个生效：
        * 未满当前容量 -> ConflictError（容量与历史保持原状，可重试）；
        * 已扩容过（再次请求）-> ConflictError，原样返回当前状态；
        * 成功 -> 仅更新 meta 容量，不写检查点、不占发布序号。
        """
        for attempt in range(3):
            try:
                return self._expand_once()
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise
        raise RuntimeError("unreachable")  # pragma: no cover

    def _expand_once(self) -> dict:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                capacity = self._capacity_locked(conn)
                max_capacity = self._max_capacity_locked(conn)
                count = conn.execute(
                    "SELECT COUNT(*) AS c FROM releases"
                ).fetchone()["c"]
                depth = depth_for_capacity(capacity)

                # 一次性：当前容量已达卷上持久化的硬性上限，不可再次扩容。
                if capacity >= max_capacity:
                    conn.rollback()
                    raise ConflictError(
                        f"容量已扩容至 {capacity} 件，一次性扩容不可重复执行"
                    )

                if count < capacity:
                    conn.rollback()
                    raise ConflictError(
                        f"当前已发布 {count} 件，未满容量 {capacity} 件，"
                        "扩容接口仅在发满后可用"
                    )

                new_capacity = capacity * 2
                if new_capacity > max_capacity:  # pragma: no cover - 2 的幂下不会触发
                    conn.rollback()
                    raise ConflictError("新容量超过一次性扩容上限")
                new_depth = depth_for_capacity(new_capacity)
                conn.execute(
                    "UPDATE meta SET value = ? WHERE key = ?",
                    (str(new_capacity), CAPACITY_KEY),
                )
                expanded = True
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return {
            "capacity": new_capacity,
            "previous_capacity": capacity,
            "tree_depth": new_depth,
            "release_count": count,
            "expanded": expanded,
        }

    # ------------------------------------------------------------------ #
    # 发布
    # ------------------------------------------------------------------ #
    def publish(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
    ) -> tuple[dict, bool]:
        """串行化发布。返回 (发布记录, 是否复用既有记录)。

        调用方必须已把原件原子落盘成功后再调用本方法，
       从而保证“库中有记录 => 卷上有完整原件”。
        """
        # 双实例并发时 BEGIN IMMEDIATE 可能撞上写锁；busy_timeout 之外再有限重试。
        for attempt in range(3):
            try:
                return self._publish_once(
                    station_id, sample_date, order_no, blob_sha256, size
                )
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise
        raise RuntimeError("unreachable")  # pragma: no cover

    def _publish_once(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
    ) -> tuple[dict, bool]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM releases "
                    "WHERE station_id = ? AND sample_date = ? AND order_no = ?",
                    (station_id, sample_date, order_no),
                ).fetchone()
                if row is not None:
                    record = dict(row)
                    same = (
                        record["blob_sha256"] == blob_sha256
                        and record["size"] == size
                    )
                    conn.rollback()  # 只读复用，不产生写入
                    if not same:
                        raise ConflictError(
                            "同一站号/日期/发布单号已存在且原件内容不同，禁止改写"
                        )
                    return record, True

                count = conn.execute(
                    "SELECT COUNT(*) AS c FROM releases"
                ).fetchone()["c"]
                capacity = self._capacity_locked(conn)
                depth = depth_for_capacity(capacity)
                if count >= capacity:
                    conn.rollback()
                    raise ConflictError(
                        f"累计发布已达上限 {capacity} 件，请发满后调用一次性扩容接口"
                    )

                cursor = conn.execute(
                    "INSERT INTO releases(station_id, sample_date, order_no, "
                    "blob_sha256, size, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        station_id,
                        sample_date,
                        order_no,
                        blob_sha256,
                        size,
                        _utcnow(),
                    ),
                )
                release_id = cursor.lastrowid
                # 无删除路径，rowid 必然稠密且等于新条数。
                assert release_id == count + 1

                leaves = self._leaf_hashes_locked(conn)
                root = merkle.root_from_leaves(leaves, depth)
                conn.execute(
                    "INSERT INTO checkpoints(seq, root_hex, tree_depth, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (len(leaves), root.hex(), depth, _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        record = self.get_release_by_id(release_id)
        return record, False

    @staticmethod
    def _leaf_hashes_locked(conn: sqlite3.Connection) -> list[bytes]:
        rows = conn.execute(
            "SELECT station_id, sample_date, order_no, blob_sha256, size "
            "FROM releases ORDER BY id ASC"
        ).fetchall()
        return [
            merkle.leaf_hash(
                r["station_id"],
                r["sample_date"],
                r["order_no"],
                r["blob_sha256"],
                r["size"],
            )
            for r in rows
        ]

    def leaf_hashes(self) -> list[bytes]:
        with self.connect() as conn:
            return self._leaf_hashes_locked(conn)

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_release(self, station_id: str, sample_date: str, order_no: str):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE station_id = ? AND sample_date = ? "
                "AND order_no = ?",
                (station_id, sample_date, order_no),
            ).fetchone()
            return dict(row) if row else None

    def get_release_by_id(self, release_id: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE id = ?", (release_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_releases(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM releases ORDER BY id ASC").fetchall()
            return [dict(r) for r in rows]

    def latest_checkpoint(self) -> dict:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            return dict(row)

    def get_checkpoint(self, seq: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE seq = ?", (seq,)
            ).fetchone()
            return dict(row) if row else None

    def list_checkpoints(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # 启动自检：不得带着缺件记录对外服务
    # ------------------------------------------------------------------ #
    def integrity_problems(self, blob_verifier) -> list[str]:
        problems: list[str] = []
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, blob_sha256 FROM releases ORDER BY id"
            ).fetchall()
            expected_ids = list(range(1, len(rows) + 1))
            actual_ids = [r["id"] for r in rows]
            if actual_ids != expected_ids:
                problems.append("releases 主键不稠密，存在缺口或乱序")
            for row in rows:
                if not blob_verifier(row["blob_sha256"]):
                    problems.append(
                        f"release id={row['id']} 的原件缺失或摘要不符 "
                        f"(sha256={row['blob_sha256'][:12]}...)"
                    )

            capacity = self._capacity_locked(conn)
            current_depth = depth_for_capacity(capacity)
            if len(rows) > capacity:
                problems.append(
                    f"发布条数 {len(rows)} 超过当前容量 {capacity}"
                )

            leaves = self._leaf_hashes_locked(conn)
            cp_rows = conn.execute(
                "SELECT seq, root_hex, tree_depth FROM checkpoints ORDER BY seq"
            ).fetchall()
            cp_seqs = [r["seq"] for r in cp_rows]
            if cp_seqs != list(range(len(cp_seqs))):
                problems.append("checkpoints 序号不连续")

            # 每个检查点按其生成时的树深度独立复算：扩容前深度 7 的旧根
            # 必须与旧检查点留存摘要逐字节一致，扩容后检查点用深度 8。
            # 深度序列只允许“同深持续、在扩容点跃升一次”，不允许回退。
            prev_depth = None
            for row in cp_rows:
                depth = row["tree_depth"]
                if prev_depth is not None and depth < prev_depth:
                    problems.append(
                        f"检查点 seq={row['seq']} 的树深度回退，扩容历史异常"
                    )
                    break
                prev_depth = depth
                rebuilt = merkle.root_from_leaves(
                    leaves[: row["seq"]], depth
                ).hex()
                if rebuilt != row["root_hex"]:
                    problems.append(
                        f"检查点 seq={row['seq']} 的根摘要无法按深度 {depth} 复算"
                    )

            latest = cp_rows[-1]
            if latest["seq"] != len(leaves):
                problems.append("最新检查点条数与实际叶子数不一致")
            if latest["tree_depth"] != current_depth:
                problems.append(
                    "最新检查点树深度与当前容量不一致，扩容状态异常"
                )
        return problems
