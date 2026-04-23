from dataclasses import dataclass, field

"""把“问题路由 + 检索结果 + 兜底状态”统一封装成一个标准对象，在各模块之间传递"""
@dataclass
class ChatRouteModel:
    requested_merchant_id: str | None = None
    merchant_id: str | None = None
    merchant_name: str | None = None
    dataset_id: str | None = None
    dataset_name: str | None = None
    system_prompt: str | None = None
    route_type: str = "chat"
    confidence: float = 0.0
    docs: list[dict] = field(default_factory=list)
    fallback_reason: str = ""
    rag_hit: bool = False
    final_answer: str | None = None
