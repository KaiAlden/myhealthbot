from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote


DEFAULT_CONFIG_FILE = Path("config.json")


@dataclass
class FileReport:
    source: str
    source_type: str
    status: str = "success"
    markdown_files: list[str] = field(default_factory=list)
    images: int = 0
    sheets: int = 0
    skipped_sheets: list[dict[str, str]] = field(default_factory=list)
    pdf_pages: int = 0
    possible_scanned_pages: list[int] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def import_or_fail(module_name: str, package_hint: str):
    try:
        return __import__(module_name)
    except ImportError as exc:
        raise RuntimeError(
            f"Missing dependency '{package_hint}'. Run: pip install -r requirements.txt"
        ) from exc


def slugify_filename(value: str, max_length: int = 90) -> str:
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f()]', "_", str(value)).strip()
    value = re.sub(r"\s+", "_", value).strip("._ ")
    return (value or "untitled")[:max_length].rstrip("._ ")


def normalize_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def compact_text(value: Any) -> str:
    text = normalize_cell(value)
    text = re.sub(r"\s*\n\s*", " ", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def md_escape(value: Any) -> str:
    text = normalize_cell(value)
    return text.replace("\\", "\\\\").replace("|", "\\|").replace("\n", "<br>")


def yaml_escape(value: Any) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def yaml_frontmatter(metadata: dict[str, Any]) -> str:
    lines = ["---"]
    for key, value in metadata.items():
        lines.append(f"{key}: {value}" if isinstance(value, (int, float)) else f"{key}: {yaml_escape(value)}")
    lines.append("---")
    return "\n".join(lines)


def table_to_markdown(rows: list[list[Any]]) -> str:
    if not rows:
        return ""
    rows = [[md_escape(cell) for cell in row] for row in rows]
    max_cols = max(len(row) for row in rows)
    rows = [row + [""] * (max_cols - len(row)) for row in rows]
    header = rows[0] if any(rows[0]) else [f"列{i}" for i in range(1, max_cols + 1)]
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(["---"] * max_cols) + " |",
    ]
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def public_image_url(asset_base_url: str, relative_path: str) -> str:
    if not asset_base_url:
        return relative_path
    filename = relative_path.split("/", 1)[1] if relative_path.startswith("assets/") else relative_path
    return f"{asset_base_url.rstrip('/')}/{quote(filename)}"


def default_asset_base_url() -> str:
    if not DEFAULT_CONFIG_FILE.exists():
        return ""
    try:
        config = json.loads(DEFAULT_CONFIG_FILE.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return ""
    return str(config.get("rag_asset_base_url") or "").strip()


def make_image_record(
    source_file: Path,
    source_type: str,
    relative_path: str,
    asset_base_url: str,
    image_id: str,
    caption: str,
    sheet_name: str = "",
    anchor: str = "",
    page: int | None = None,
    markdown_file: str = "",
    bbox: tuple[float, float, float, float] | None = None,
) -> dict:
    record = {
        "image_id": image_id,
        "image_url": public_image_url(asset_base_url, relative_path),
        "image_path": relative_path,
        "caption": caption,
        "source_file": source_file.name,
        "source_type": source_type,
        "sheet_name": sheet_name,
        "anchor": anchor,
        "page": page,
        "markdown_file": markdown_file,
    }
    if bbox is not None:
        record["bbox"] = [round(float(value), 2) for value in bbox]
    return record


def image_resource_markdown(records: list[dict], heading: str = "图片资源") -> str:
    if not records:
        return ""
    lines = [f"## {heading}", ""]
    for record in records:
        lines.extend(
            [
                "### 关联图片",
                "",
                f"image_id: {record['image_id']}",
                f"image_path: {record['image_path']}",
                f"image_caption: {record['caption']}",
                f"image_url: {record['image_url']}",
                f"source_file: {record['source_file']}",
            ]
        )
        if record.get("sheet_name"):
            lines.append(f"sheet_name: {record['sheet_name']}")
        if record.get("anchor"):
            lines.append(f"anchor: {record['anchor']}")
        if record.get("page"):
            lines.append(f"page: {record['page']}")
        lines.extend(["", f"![{record['caption']}](<{record['image_url']}>)", ""])
    return "\n".join(lines)


def find_header_column(header: list[Any], *keywords: str) -> int | None:
    for index, value in enumerate(header):
        text = normalize_cell(value)
        if all(keyword in text for keyword in keywords):
            return index
    return None


def row_value(row: list[Any], index: int | None) -> str:
    if index is None or index >= len(row):
        return ""
    return normalize_cell(row[index])


def write_md(path: Path, parts: list[str]) -> None:
    path.write_text("\n".join(part for part in parts if part is not None), encoding="utf-8")
