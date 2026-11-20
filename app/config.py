"""运行期配置。

所有路径均指向共享卷（Docker Compose 下两个实例挂载同一卷），
保证任一实例重启后仍能读到全部已提交原件与检查点。

容量说明
--------
``MAX_RELEASES`` 只是**首次建库时**的初始上限（默认 128，对应固定深度 7
的 Merkle 树）。运行期的实际上限持久化在共享卷 SQLite 的 ``meta`` 表中，
一次性扩容接口把它翻倍到 256（树深度 8），因此重启后仍以卷上记录的容量
为准，环境变量不会覆盖已扩容的卷。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} 必须为正整数，实际为 {value!r}")
    return value


def depth_for_capacity(capacity: int) -> int:
    """容量（叶子槽位数）对应的固定树深度。容量必须是 2 的幂。"""
    if capacity <= 0 or capacity & (capacity - 1) != 0:
        raise ValueError(f"容量必须是 2 的幂，实际为 {capacity!r}")
    return capacity.bit_length() - 1


@dataclass
class Settings:
    data_dir: Path
    db_path: Path
    blob_dir: Path
    temp_dir: Path
    max_file_bytes: int
    # 仅用于全新空卷的初始上限；已存在的卷以数据库 meta 表为准。
    max_releases: int
    # 初始固定深度完整二叉树：深度 7 -> 128 个叶子槽位。
    tree_depth: int

    @property
    def tree_capacity(self) -> int:
        return 1 << self.tree_depth

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = Path(os.environ.get("DATA_DIR", "/data")).resolve()
        max_releases = _env_int("MAX_RELEASES", 128)
        tree_depth = depth_for_capacity(max_releases)
        settings = cls(
            data_dir=data_dir,
            db_path=data_dir / "release.db",
            blob_dir=data_dir / "blobs",
            temp_dir=data_dir / "tmp",
            max_file_bytes=_env_int("MAX_FILE_BYTES", 1 * 1024 * 1024),
            max_releases=max_releases,
            tree_depth=tree_depth,
        )
        return settings

    def ensure_dirs(self) -> None:
        for path in (self.data_dir, self.blob_dir, self.temp_dir):
            path.mkdir(parents=True, exist_ok=True)


settings = Settings.from_env()
