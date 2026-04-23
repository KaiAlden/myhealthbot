from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from converters.common import FileReport, default_asset_base_url
from converters.excel_to_markdown import convert_excel
from converters.pdf_to_markdown import convert_pdf


SUPPORTED_EXCEL = {".xlsx"}
SUPPORTED_PDF = {".pdf"}


def convert_file(
    source_file: Path,
    output_dir: Path,
    assets_dir: Path,
    rows_per_file: int,
    generated_at: str,
    asset_base_url: str,
    image_index: list[dict],
) -> FileReport:
    if source_file.name.startswith("~$"):
        return FileReport(source_file.name, "temporary", status="skipped", errors=["Skipped Office temporary lock file."])

    suffix = source_file.suffix.lower()
    if suffix in SUPPORTED_EXCEL:
        return convert_excel(source_file, output_dir, assets_dir, rows_per_file, generated_at, asset_base_url, image_index)
    if suffix in SUPPORTED_PDF:
        return convert_pdf(source_file, output_dir, assets_dir, generated_at, asset_base_url, image_index)
    return FileReport(source_file.name, suffix.lstrip(".") or "unknown", status="skipped", errors=[f"Unsupported file type: {suffix}"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert business Excel/PDF documents to RAG-friendly Markdown.")
    parser.add_argument("--input", default="src/yuanshuju", help="Input directory.")
    parser.add_argument("--output", default="src/markdata", help="Output directory.")
    parser.add_argument("--rows-per-file", type=int, default=100, help="Excel data rows per Markdown part.")
    parser.add_argument("--asset-base-url", default=default_asset_base_url(), help="Public base URL for assets, e.g. http://192.168.44.1:9901/rag-assets.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    assets_dir = output_dir / "assets"
    if not input_dir.exists():
        print(f"Input directory does not exist: {input_dir}", file=sys.stderr)
        return 2
    if args.rows_per_file <= 0:
        print("--rows-per-file must be greater than 0", file=sys.stderr)
        return 2

    output_dir.mkdir(parents=True, exist_ok=True)
    assets_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    reports = []
    image_index = []

    for source_file in sorted([path for path in input_dir.iterdir() if path.is_file()], key=lambda path: path.name.lower()):
        print(f"Converting: {source_file.name}")
        try:
            report = convert_file(source_file, output_dir, assets_dir, args.rows_per_file, generated_at, args.asset_base_url, image_index)
            print(f"  {report.status}: {len(report.markdown_files)} markdown, {report.images} images")
            for skipped_sheet in report.skipped_sheets:
                print(f"    skipped sheet: {skipped_sheet['sheet_name']} ({skipped_sheet['reason']})")
        except Exception as exc:
            report = FileReport(source_file.name, source_file.suffix.lower().lstrip(".") or "unknown", status="failed", errors=[f"{type(exc).__name__}: {exc}"])
            print(f"  failed: {report.errors[0]}")
        reports.append(report)

    summary = {
        "files_total": len(reports),
        "converted": sum(1 for report in reports if report.status == "success"),
        "failed": sum(1 for report in reports if report.status == "failed"),
        "skipped": sum(1 for report in reports if report.status == "skipped"),
        "skipped_sheets": sum(len(report.skipped_sheets) for report in reports),
        "markdown_files": sum(len(report.markdown_files) for report in reports),
        "images": sum(report.images for report in reports),
    }
    (output_dir / "image_index.json").write_text(json.dumps(image_index, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "convert_report.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "input_dir": str(input_dir),
                "output_dir": str(output_dir),
                "rows_per_file": args.rows_per_file,
                "asset_base_url": args.asset_base_url,
                "summary": summary,
                "files": [asdict(report) for report in reports],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Done: {summary['converted']} converted, {summary['failed']} failed, {summary['markdown_files']} markdown files, {summary['images']} images.")
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
