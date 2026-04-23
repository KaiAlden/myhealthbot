from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from converters.common import (
    FileReport,
    compact_text,
    image_resource_markdown,
    import_or_fail,
    make_image_record,
    normalize_cell,
    slugify_filename,
    write_md,
    yaml_frontmatter,
)


def clean_pdf_table(table: list[list[Any]]) -> list[list[str]]:
    rows = []
    for row in table or []:
        if not row:
            continue
        cleaned = [compact_text(cell) for cell in row]
        if any(cleaned):
            rows.append(cleaned)
    return rows


def is_sequence_cell(value: str) -> bool:
    return bool(re.fullmatch(r"\d+", compact_text(value or "")))


def clean_cjk_spaces(value: str) -> str:
    text = compact_text(value)
    return re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", text)


def clean_constitution(value: str) -> str:
    text = compact_text(value)
    text = re.sub(r"\s+", "、", text)
    return clean_cjk_spaces(text)


def pdf_product_fields(row: list[str], previous: dict[str, str]) -> dict[str, str] | None:
    cells = row + [""] * max(0, 7 - len(row))

    if is_sequence_cell(cells[0]) and len(cells) >= 6:
        service_type = cells[1] or previous.get("服务类型", "")
        category = cells[2] or previous.get("商品/服务大类", "")
        name = cells[3]
        constitution = cells[4]
        intro = cells[5]
    else:
        row_text = " ".join(cells)
        if any(keyword in row_text for keyword in ("商品/服务名称", "商品适合的体质", "商品主图")):
            return None
        return None

    if not name:
        return None

    fields = {
        "服务类型": clean_cjk_spaces(service_type),
        "商品/服务大类": clean_cjk_spaces(category),
        "商品/服务名称": clean_cjk_spaces(name),
        "适合体质": clean_constitution(constitution),
        "介绍": clean_cjk_spaces(intro),
    }
    if fields["服务类型"]:
        previous["服务类型"] = fields["服务类型"]
    if fields["商品/服务大类"]:
        previous["商品/服务大类"] = fields["商品/服务大类"]
    return fields


def extract_pdf_products(tables: list[list[list[str]]], table_row_bboxes: list[list[tuple[float, float, float, float] | None]]) -> list[dict]:
    tables, table_row_bboxes = merge_split_pdf_tables(tables, table_row_bboxes)
    products = []
    previous: dict[str, str] = {}
    for table_index, table in enumerate(tables, start=1):
        bboxes = table_row_bboxes[table_index - 1] if table_index - 1 < len(table_row_bboxes) else []
        for row_index, row in enumerate(table):
            fields = pdf_product_fields(row, previous)
            if not fields:
                continue
            products.append(
                {
                    "fields": fields,
                    "table_index": table_index,
                    "row_index": row_index + 1,
                    "bbox": bboxes[row_index] if row_index < len(bboxes) else None,
                    "images": [],
                }
            )
    return products


def merge_split_pdf_tables(
    tables: list[list[list[str]]],
    table_row_bboxes: list[list[tuple[float, float, float, float] | None]],
) -> tuple[list[list[list[str]]], list[list[tuple[float, float, float, float] | None]]]:
    if len(tables) < 3:
        return tables, table_row_bboxes

    first, second, third = tables[0], tables[1], tables[2]
    if len(first) != 1 or len(first[0]) < 2:
        return tables, table_row_bboxes
    if not second or not third or len(second) != len(third):
        return tables, table_row_bboxes
    if not all(len(row) == 1 and is_sequence_cell(row[0]) for row in second):
        return tables, table_row_bboxes
    if not all(len(row) >= 3 for row in third):
        return tables, table_row_bboxes

    service_type = first[0][0]
    category = first[0][1]
    merged = [
        [seq_row[0], service_type, category] + detail_row
        for seq_row, detail_row in zip(second, third)
    ]
    detail_bboxes = table_row_bboxes[2] if len(table_row_bboxes) > 2 else []
    return [merged] + tables[3:], [detail_bboxes] + table_row_bboxes[3:]


def image_center_y(record: dict) -> float | None:
    bbox = record.get("bbox")
    if not bbox or len(bbox) < 4:
        return None
    return (float(bbox[1]) + float(bbox[3])) / 2


def product_center_y(product: dict) -> float | None:
    bbox = product.get("bbox")
    if not bbox or len(bbox) < 4:
        return None
    return (float(bbox[1]) + float(bbox[3])) / 2


def assign_pdf_images_to_products(products: list[dict], image_records: list[dict]) -> list[dict]:
    if not products:
        return image_records

    unassigned = []
    for image_index, record in enumerate(image_records):
        center_y = image_center_y(record)
        target = None
        if center_y is not None:
            for product in products:
                bbox = product.get("bbox")
                if bbox and len(bbox) >= 4 and float(bbox[1]) <= center_y <= float(bbox[3]):
                    target = product
                    break
            if target is None:
                candidates = [
                    (abs(center_y - product_y), product)
                    for product in products
                    if (product_y := product_center_y(product)) is not None
                ]
                if candidates:
                    target = min(candidates, key=lambda item: item[0])[1]

        if target is None and image_index < len(products):
            target = products[image_index]

        if target is None:
            unassigned.append(record)
            continue

        fields = target["fields"]
        caption = semantic_pdf_image_caption(fields, len(target["images"]) + 1)
        record["caption"] = caption
        record["associated_product_name"] = fields.get("商品/服务名称", "")
        record["associated_constitution"] = fields.get("适合体质", "")
        target["images"].append(record)
    return unassigned


def semantic_pdf_image_caption(fields: dict[str, str], image_index: int) -> str:
    name = fields.get("商品/服务名称") or f"商品图片{image_index}"
    category = fields.get("商品/服务大类") or ""
    constitution = fields.get("适合体质") or ""
    pieces = [f"{name}产品图"]
    if category:
        pieces.append(category)
    if constitution:
        pieces.append(f"{constitution}适用")
    return "，".join(pieces)


def pdf_product_markdown(page_number: int, products: list[dict]) -> str:
    if not products:
        return ""

    lines = [f"## 第 {page_number} 页商品资料（含图片）", ""]
    for product in products:
        fields = product["fields"]
        name = fields.get("商品/服务名称") or "未命名商品"
        lines.extend([f"### {name}", ""])
        for label in ("服务类型", "商品/服务大类", "适合体质", "介绍"):
            value = fields.get(label)
            if value:
                lines.append(f"{label}: {value}")

        if product["images"]:
            lines.append("")
            lines.append("商品图片:")
            for record in product["images"]:
                lines.extend(
                    [
                        f"image_id: {record['image_id']}",
                        f"image_caption: {record['caption']}",
                        f"image_url: {record['image_url']}",
                        f"![{record['caption']}](<{record['image_url']}>)",
                    ]
                )
        lines.append("")
    return "\n".join(lines)


def table_row_bboxes(pdfplumber_page) -> list[list[tuple[float, float, float, float] | None]]:
    result = []
    try:
        tables = pdfplumber_page.find_tables()
    except Exception:
        return result

    for table in tables:
        row_bboxes = []
        for row in getattr(table, "rows", []):
            cells = [cell for cell in getattr(row, "cells", []) if cell]
            if not cells:
                row_bboxes.append(None)
                continue
            heights = [cell[3] - cell[1] for cell in cells]
            min_height = min(heights)
            row_cells = [
                cell
                for cell in cells
                if (cell[3] - cell[1]) <= max(min_height * 1.5, min_height + 1)
            ] or cells
            x0 = min(cell[0] for cell in row_cells)
            y0 = min(cell[1] for cell in row_cells)
            x1 = max(cell[2] for cell in row_cells)
            y1 = max(cell[3] for cell in row_cells)
            row_bboxes.append((x0, y0, x1, y1))
        result.append(row_bboxes)
    return result


def save_pdf_images(page, document, source_file: Path, assets_dir: Path, asset_base_url: str, page_number: int, output_name: str) -> list[dict]:
    records = []
    image_infos = page.get_image_info(xrefs=True)
    seen_xrefs = set()

    for image_index, image_info in enumerate(image_infos, start=1):
        xref = image_info.get("xref")
        if not xref or xref in seen_xrefs:
            continue
        seen_xrefs.add(xref)
        image_data = document.extract_image(xref)
        ext = image_data.get("ext", "png")
        stem = f"{slugify_filename(source_file.stem)}__page{page_number:03d}__image{len(records) + 1:03d}"
        filename = f"{stem}.{ext}"
        relative_path = f"assets/{filename}"
        (assets_dir / filename).write_bytes(image_data["image"])
        records.append(
            make_image_record(
                source_file=source_file,
                source_type="pdf",
                relative_path=relative_path,
                asset_base_url=asset_base_url,
                image_id=f"{slugify_filename(source_file.stem, 40)}_p{page_number:03d}_{len(records) + 1:03d}",
                caption=f"{source_file.stem} 第 {page_number} 页图片 {len(records) + 1}",
                page=page_number,
                markdown_file=output_name,
                bbox=image_info.get("bbox"),
            )
        )
    return records


def convert_pdf(source_file: Path, output_dir: Path, assets_dir: Path, generated_at: str, asset_base_url: str, image_index: list[dict]) -> FileReport:
    fitz = import_or_fail("fitz", "pymupdf")
    pdfplumber = import_or_fail("pdfplumber", "pdfplumber")
    report = FileReport(source_file.name, "pdf")

    page_tables: dict[int, list[list[list[str]]]] = {}
    page_row_bboxes: dict[int, list[list[tuple[float, float, float, float] | None]]] = {}
    with pdfplumber.open(source_file) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            try:
                page_tables[page_index] = [clean_pdf_table(table) for table in (page.extract_tables() or [])]
                page_tables[page_index] = [table for table in page_tables[page_index] if table]
                page_row_bboxes[page_index] = table_row_bboxes(page)
            except Exception:
                page_tables[page_index] = []
                page_row_bboxes[page_index] = []

    document = fitz.open(source_file)
    report.pdf_pages = document.page_count
    output_name = f"{slugify_filename(source_file.stem)}.md"
    content = [
        yaml_frontmatter({"source_file": source_file.name, "source_type": "pdf", "generated_at": generated_at}),
        "",
        f"# {source_file.stem}",
        "",
    ]

    for page_index in range(document.page_count):
        page_number = page_index + 1
        page = document.load_page(page_index)
        text = normalize_cell(page.get_text("text"))
        if len(text) < 20:
            report.possible_scanned_pages.append(page_number)

        image_records = save_pdf_images(page, document, source_file, assets_dir, asset_base_url, page_number, output_name)
        tables = page_tables.get(page_number, [])
        products = extract_pdf_products(tables, page_row_bboxes.get(page_number, []))
        unassigned_images = assign_pdf_images_to_products(products, image_records)

        for record in image_records:
            image_index.append(record)
        report.images += len(image_records)

        if products:
            content.append(pdf_product_markdown(page_number, products))
            if unassigned_images:
                content.append(image_resource_markdown(unassigned_images, heading=f"第 {page_number} 页未关联图片资源"))
            continue

        content.extend([f"## 第 {page_number} 页", "", text or "> 该页未提取到明显文本，可能是扫描页或图片页。", ""])
        if unassigned_images:
            content.append(image_resource_markdown(unassigned_images))

    document.close()
    output_path = output_dir / output_name
    write_md(output_path, content)
    report.markdown_files.append(output_name)
    return report
