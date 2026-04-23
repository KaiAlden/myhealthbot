from data.merchant_registry import get_merchant


def build_messages(history: list[dict], user_input: str, route_result, max_rounds: int = 8) -> list[dict]:
    history = _trim_history(history, max_rounds)
    merchant = get_merchant(route_result.merchant_id) if route_result else None
    merchant_name = route_result.merchant_name if route_result else None
    system_prompt = route_result.system_prompt if route_result else None

    if merchant:
        merchant_name = merchant_name or merchant.get("merchant_name")
        system_prompt = system_prompt or merchant.get("system_prompt")

    system_parts = [
        "你是一个多商家商品导购助手。",
        "请优先参考系统提供的商品资料回答。商品推荐必须严格来自商品参考资料。不允许推荐资料中没有出现的品牌、商品名或外部平台产品。如果用户描述的是症状，请先根据资料判断可能对应的体质，再推荐资料中明确适用该体质的商品。如果资料不足以推荐具体商品，请说明目前资料不足，不要用通用知识补充商品。",
        (
            "回答风格要求：请像真实导购或客服一样自然交流，先直接回应用户的问题，再补充必要依据。"
            "不要每次都套用固定模板，不要滥用标题、编号、分隔线或 emoji。"
            "如果答案很简单，用 1-3 个自然段即可；只有需要对比多个商品或列出注意事项时，才使用简洁列表。"
            "表达要口语化但专业，避免“根据您提供的信息源”“以下是相关建议”这类生硬开头。"
            "如果资料不足，请用自然方式说明“目前资料里没有看到……”，并给出可执行的下一步建议。"
            "如果资料中包含 image_url 或 Markdown 图片链接，不要回答没有图片；文字中可说明“已附上对应商品图”，图片由接口 images 字段返回。"
        ),
    ]

    if route_result and route_result.merchant_id and merchant_name:
        system_parts.append(f"当前服务商家：{merchant_name} ({route_result.merchant_id})")
    if system_prompt:
        system_parts.append(system_prompt)

    if route_result and route_result.docs:
        rag_lines = []
        for index, doc in enumerate(route_result.docs, start=1):
            title = doc.get("title") or f"资料{index}"
            content = doc.get("content") or ""
            score = doc.get("score")
            score_text = f" (score={score})" if score is not None else ""
            rag_lines.append(f"[资料{index}] {title}{score_text}\n{content}")
        system_parts.append("商品参考资料：\n" + "\n\n".join(rag_lines))
    else:
        system_parts.append("当前没有命中可用的商品资料，请按普通问答处理。")

    messages = [{"role": "system", "content": "\n\n".join(system_parts)}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_input})
    return messages


def _trim_history(history: list[dict], max_rounds: int) -> list[dict]:
    if max_rounds <= 0:
        return []
    return history[-(max_rounds * 2):]
