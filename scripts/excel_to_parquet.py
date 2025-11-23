#!/usr/bin/env python3
"""Convert the first worksheet of an Excel workbook to Parquet.

Usage:
  python scripts/excel_to_parquet.py --input path/to/workbook.xlsx --output data/out.parquet

The script reads the first sheet by default (sheet index 0) and writes a Parquet file.
It uses `pandas.read_excel` for reading and `DataFrame.to_parquet` for writing.
"""
from pathlib import Path
import argparse
import sys

import pandas as pd


def excel_to_parquet(input_path: Path, output_path: Path, sheet=0, engine: str | None = None):
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read the specified sheet (default first sheet). Let pandas infer dtypes and parse dates.
    read_kwargs = {"sheet_name": sheet}
    if engine:
        read_kwargs["engine"] = engine

    df = pd.read_excel(input_path, **read_kwargs)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write parquet. pandas will use pyarrow or fastparquet if available.
    df.to_parquet(output_path, index=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Convert first worksheet of Excel to Parquet")
    parser.add_argument("--input", "-i", required=True, help="Path to input Excel workbook")
    parser.add_argument("--output", "-o", required=True, help="Path to output Parquet file")
    parser.add_argument("--sheet", "-s", default=0, help="Sheet name or zero-based index (default: 0)")
    parser.add_argument("--engine", "-e", default=None, help="Optional pandas Excel engine (openpyxl, xlrd, odf)")

    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_path = Path(args.output)

    # Try to interpret sheet as int index if possible
    sheet_arg = args.sheet
    try:
        sheet = int(sheet_arg)
    except Exception:
        sheet = sheet_arg

    try:
        excel_to_parquet(input_path, output_path, sheet=sheet, engine=args.engine)
        print(f"Wrote Parquet to {output_path} from sheet={sheet}")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
