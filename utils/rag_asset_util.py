import re
from urllib.parse import quote


MARKDOWN_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\((?:<([^>]+)>|([^)]+))\)")
IMAGE_URL_RE = re.compile(r"(?im)^\s*image_url\s*[:：]\s*(\S+)\s*$")
IMAGE_ID_RE = re.compile(r"(?im)^\s*image_id\s*[:：]\s*(\S+)\s*$")
IMAGE_CAPTION_RE = re.compile(r"(?im)^\s*image_caption\s*[:：]\s*(.+?)\s*$")


def normalize_asset_url(url: str, asset_base_url: str = "") -> str:
    url = str(url or "").strip().strip("<>")
    if not url:
        return ""
    if url.startswith(("http://", "https://", "/")):
        return url
    if asset_base_url and url.startswith("assets/"):
        filename = url.split("/", 1)[1]
        return f"{asset_base_url.rstrip('/')}/{quote(filename)}"
    return url


def extract_images_from_docs(docs: list[dict], asset_base_url: str = "") -> list[dict]:
    images = []
    seen = set()

    for doc_index, doc in enumerate(docs or [], start=1):
        content = str(doc.get("content") or "")
        title = doc.get("title") or f"doc_{doc_index}"
        current_image_id = ""
        current_caption = ""

        for line in content.splitlines():
            image_id_match = IMAGE_ID_RE.match(line)
            if image_id_match:
                current_image_id = image_id_match.group(1)
                current_caption = ""
                continue

            caption_match = IMAGE_CAPTION_RE.match(line)
            if caption_match:
                current_caption = caption_match.group(1).strip()
                continue

            url_match = IMAGE_URL_RE.match(line)
            if not url_match:
                continue
            url = normalize_asset_url(url_match.group(1), asset_base_url)
            if not url or url in seen:
                continue
            seen.add(url)
            images.append(
                {
                    "image_id": current_image_id,
                    "url": url,
                    "caption": current_caption or title,
                    "source_title": title,
                    "source_index": doc_index,
                }
            )

        for match_index, match in enumerate(MARKDOWN_IMAGE_RE.finditer(content), start=1):
            alt_text, angle_url, plain_url = match.groups()
            url = normalize_asset_url(angle_url or plain_url, asset_base_url)
            if url and url not in seen:
                seen.add(url)
                images.append(
                    {
                        "image_id": "",
                        "url": url,
                        "caption": alt_text or title,
                        "source_title": title,
                        "source_index": doc_index,
                        "match_index": match_index,
                    }
                )

    return images


def build_references(docs: list[dict]) -> list[dict]:
    references = []
    for index, doc in enumerate(docs or [], start=1):
        references.append(
            {
                "index": index,
                "title": doc.get("title"),
                "score": doc.get("score"),
            }
        )
    return references
