#!/usr/bin/env python3
"""Generate a sample Parquet file of insurance exposures for a fleet of vessels.

Usage: python scripts/generate_exposure_parquet.py --rows 300 --output data/exposures.parquet

This script creates a table with realistic-ish columns and random values.
"""
import argparse
import random
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker


def generate_vessel_row(i, faker, seed=None):
    vessel_id = f"VSL-{i:05d}"
    vessel_name = faker.company() + " " + faker.word().title()
    # IMO numbers are 7-digit numeric identifiers (we'll generate a 7-digit int)
    build_year = random.randint(1970, 2023)
    vessel_type = random.choice([
        "Bulk Carrier",
        "Container",
        "Tanker",
        "Passenger",
        "General Cargo",
        "Chemical Tanker",
        "LNG Carrier",
        "Offshore Support",
    ])
    operator = faker.company()

    # Insured value in USD between 1M and 200M
    insured_value = round(float(np.random.uniform(1_000_000, 200_000_000)), 2)

    tonnage = round(float(np.random.uniform(1000, 300000)), 2)
    length_m = round(float(np.random.uniform(50, 400)), 2)
    cargo_type = random.choice([
        "General Cargo",
        "Dry Bulk",
        "Oil",
        "Chemicals",
        "Containers",
        "Liquified Gas",
    ])

    risk_score = round(float(np.random.beta(2, 8) * 100), 2)

    # Simple premium estimate: insured_value * base_rate * (1 + risk_adj)
    base_rate = 0.002  # 0.2% as baseline
    premium_estimate = round(insured_value * base_rate * (1 + risk_score / 200.0), 2)

    return {
        "vesselId": vessel_id,
        "vessel": vessel_name,
        "operator": operator,
        "year": build_year,
        "type": vessel_type,
        "value": insured_value,
        "tonnage": tonnage,
        "length": length_m,
        "cargoType": cargo_type,
        "premium": premium_estimate,
    }


def generate_dataframe(rows=50, seed=None):
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)

    faker = Faker()
    if seed is not None:
        Faker.seed(seed)

    records = [generate_vessel_row(i + 1, faker, seed=seed) for i in range(rows)]
    df = pd.DataFrame.from_records(records)

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate sample vessel insurance exposures and write Parquet.")
    parser.add_argument("--rows", type=int, default=100,
                        help="Number of rows to generate (default: 100)")
    parser.add_argument("--output", type=str, default="data/exposures.parquet", help="Output Parquet file path")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for reproducibility")

    args = parser.parse_args()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = generate_dataframe(rows=args.rows, seed=args.seed)

    # Write Parquet (pandas will use pyarrow or fastparquet if available)
    df.to_parquet(out_path, index=False)

    print(f"Wrote {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
