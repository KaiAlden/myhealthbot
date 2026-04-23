from __future__ import annotations

import json
import uuid
from urllib import error, parse, request


class DifyAPIError(Exception):
    """Dify 接口统一异常，保存状态码与响应内容，便于上层统一处理。"""

    def __init__(self, status_code: int, payload):
        super().__init__(str(payload))
        self.status_code = status_code
        self.payload = payload


class DifyKnowledgeBaseClient:
    """Dify 知识库接口客户端，封装常见数据集/文档/分段操作。"""

    def __init__(self, base_url: str, api_key: str, timeout: int = 15):
        # 统一处理末尾斜杠，避免拼接 URL 时出现重复 /
        self.base_url = (base_url or "").rstrip("/")
        self.api_key = api_key or ""
        # urllib 请求超时时间（秒）
        self.timeout = timeout

    def get_dataset(self, dataset_id: str):
        """获取单个数据集详情。"""
        return self._request("GET", f"/datasets/{self._quote(dataset_id)}")

    def update_dataset(self, dataset_id: str, payload: dict):
        """更新数据集配置。"""
        return self._request("PATCH", f"/datasets/{self._quote(dataset_id)}", json_body=payload)

    def list_documents(self, dataset_id: str, query: dict | None = None):
        """查询数据集下的文档列表，可传分页/过滤参数。"""
        return self._request("GET", f"/datasets/{self._quote(dataset_id)}/documents", query=query)

    def update_document_by_text(self, dataset_id: str, document_id: str, payload: dict):
        """通过纯文本内容更新指定文档。"""
        return self._request(
            "POST",
            f"/datasets/{self._quote(dataset_id)}/documents/{self._quote(document_id)}/update-by-text",
            json_body=payload,
        )

    def update_document_by_file(self, dataset_id: str, document_id: str, file_storage, fields: dict):
        """通过文件和表单字段更新文档，使用 multipart/form-data 上传。"""
        file_name = getattr(file_storage, "filename", "") or "document"
        content_type = getattr(file_storage, "mimetype", "") or "application/octet-stream"
        body, content_type_header = self._build_multipart_body(
            fields=fields,
            file_field="file",
            file_name=file_name,
            file_content_type=content_type,
            file_content=file_storage.read(),
        )
        return self._request(
            "POST",
            f"/datasets/{self._quote(dataset_id)}/documents/{self._quote(document_id)}/update-by-file",
            data=body,
            headers={"Content-Type": content_type_header},
        )

    def get_indexing_status(self, dataset_id: str, batch: str):
        """查询文档索引任务状态。"""
        return self._request(
            "GET",
            f"/datasets/{self._quote(dataset_id)}/documents/{self._quote(batch)}/indexing-status",
        )

    def update_segment(self, dataset_id: str, document_id: str, segment_id: str, payload: dict):
        """更新文档分段内容或分段元数据。"""
        return self._request(
            "POST",
            (
                f"/datasets/{self._quote(dataset_id)}/documents/{self._quote(document_id)}"
                f"/segments/{self._quote(segment_id)}"
            ),
            json_body=payload,
        )

    def _request(
        self,
        method: str,
        path: str,
        json_body: dict | None = None,
        query: dict | None = None,
        data: bytes | None = None,
        headers: dict | None = None,
    ):
        """统一 HTTP 请求入口，负责鉴权、参数编码、响应解析与异常转换。"""
        if not self.base_url or not self.api_key:
            raise DifyAPIError(500, {"message": "dify is not configured"})

        url = f"{self.base_url}{path}"
        query_string = self._encode_query(query)
        if query_string:
            url = f"{url}?{query_string}"

        # 默认带上 Bearer Token，调用方可追加额外请求头
        request_headers = {
            "Authorization": f"Bearer {self.api_key}",
        }
        if headers:
            request_headers.update(headers)

        body = data
        if json_body is not None:
            # 仅在传入 json_body 时按 JSON 编码请求体
            body = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
            request_headers["Content-Type"] = "application/json"

        req = request.Request(url=url, data=body, headers=request_headers, method=method)
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                return self._decode_response(response.read())
        except error.HTTPError as exc:
            raise DifyAPIError(exc.code, self._decode_response(exc.read()))
        except DifyAPIError:
            raise
        except Exception as exc:
            raise DifyAPIError(500, {"message": str(exc)})

    @staticmethod
    def _decode_response(body: bytes):
        """优先按 JSON 解析响应；解析失败时退化为 message 文本。"""
        if not body:
            return {}
        text = body.decode("utf-8", errors="ignore")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"message": text}

    @staticmethod
    def _encode_query(query: dict | None) -> str:
        """编码 query 参数，并过滤掉 None/空字符串值。"""
        if not query:
            return ""
        clean_query = {}
        for key, value in query.items():
            if value is None or value == "":
                continue
            clean_query[key] = value
        return parse.urlencode(clean_query, doseq=True)

    @staticmethod
    def _quote(value: str) -> str:
        """对路径变量做 URL 安全编码，避免特殊字符导致请求路径错误。"""
        return parse.quote(str(value or "").strip(), safe="")

    @staticmethod
    def _build_multipart_body(
        fields: dict,
        file_field: str,
        file_name: str,
        file_content_type: str,
        file_content: bytes,
    ) -> tuple[bytes, str]:
        """手动构造 multipart 请求体，返回 (body, Content-Type 头)。"""
        # 使用随机 boundary，降低与文件内容冲突概率
        boundary = f"----aibot-{uuid.uuid4().hex}"
        chunks: list[bytes] = []

        # 先写入普通表单字段
        for key, value in (fields or {}).items():
            if value is None:
                continue
            chunks.extend(
                [
                    f"--{boundary}\r\n".encode("utf-8"),
                    f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                    str(value).encode("utf-8"),
                    b"\r\n",
                ]
            )

        # 再写入文件字段并收尾 boundary
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                (
                    f'Content-Disposition: form-data; name="{file_field}"; '
                    f'filename="{file_name}"\r\n'
                ).encode("utf-8"),
                f"Content-Type: {file_content_type}\r\n\r\n".encode("utf-8"),
                file_content or b"",
                b"\r\n",
                f"--{boundary}--\r\n".encode("utf-8"),
            ]
        )
        return b"".join(chunks), f"multipart/form-data; boundary={boundary}"
