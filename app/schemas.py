"""HTTP 响应模型。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class ReleaseOut(BaseModel):
    id: int = Field(..., description="叶子下标 = id - 1")
    station_id: str
    sample_date: str
    order_no: str
    blob_sha256: str
    size: int
    created_at: str


class PublishResult(BaseModel):
    reused: bool = Field(..., description="True 表示同单同输入重试，复用既有记录")
    release: ReleaseOut
    checkpoint: "CheckpointOut"


class CheckpointOut(BaseModel):
    seq: int = Field(..., description="截至该检查点的发布条数")
    root_hex: str
    tree_depth: int = Field(
        ..., description="该检查点生成时的固定 Merkle 树深度（扩容前 7、扩容后 8）"
    )
    created_at: str


class CapacityStateOut(BaseModel):
    capacity: int = Field(..., description="当前累计发布上限（128，扩容后 256）")
    tree_depth: int = Field(..., description="当前容量对应的固定树深度")
    release_count: int = Field(..., description="已发布条数")
    expanded: bool = Field(..., description="是否已执行过一次性扩容")
    remaining: int = Field(..., description="剩余可发布槽位")


class ExpansionResultOut(BaseModel):
    expanded: bool = Field(..., description="本次请求是否实际执行了扩容")
    previous_capacity: int
    capacity: int
    previous_tree_depth: int
    tree_depth: int
    release_count: int = Field(..., description="扩容时刻的已发布条数（= 旧容量）")
    message: str


class LeafRecordOut(BaseModel):
    v: int
    station_id: str
    sample_date: str
    order_no: str
    blob_sha256: str
    size: int


class InclusionProofOut(BaseModel):
    index: int
    checkpoint_seq: int
    checkpoint_root: str
    tree_depth: int
    leaf_record: LeafRecordOut
    leaf_hash: str
    path: list[dict]


class AppendProofOut(BaseModel):
    old_seq: int
    new_seq: int
    old_root: str
    new_root: str
    tree_depth: int = Field(..., description="新检查点的树深度")
    old_tree_depth: int = Field(
        ..., description="旧检查点的树深度；跨扩容时与 tree_depth 不同"
    )
    entries: list[dict] = Field(
        ..., description="完美子树条目 (start, height, hash)，数量 O(log n)"
    )


class HealthOut(BaseModel):
    status: str
    release_count: int
    capacity: int
    tree_depth: int
    problems: list[str]


PublishResult.model_rebuild()
