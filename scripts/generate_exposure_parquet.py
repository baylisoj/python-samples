#!/usr/bin/env python3
"""Generate a sample Parquet file of insurance exposures for a fleet of vessels.

Usage: python scripts/generate_exposure_parquet.py --rows 300 --output data/exposures.parquet

This script creates a table with realistic-ish columns and random values.
"""
import argparse
import random
import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker


def random_coordinates(ocean_only=False):
    # Simple global random lat/lon; if ocean_only is True we still sample globally
    lat = random.uniform(-60.0, 60.0)
    lon = random.uniform(-180.0, 180.0)
    return lat, lon


def generate_vessel_row(i, faker, seed=None):
    vessel_id = f"VSL-{i:05d}"
    vessel_name = faker.company() + " " + faker.word().title()
    # IMO numbers are 7-digit numeric identifiers (we'll generate a 7-digit int)
    imo_number = random.randint(1000000, 9999999)
    build_year = random.randint(1970, 2023)
    vessel_type = random.choice([
        "Bulk Carrier",
        "Container",
        "Tanker",
        "RoRo",
        "General Cargo",
        "Chemical Tanker",
        "LNG Carrier",
        "Offshore Support",
    ])
    operator = faker.company()

    # Insured value in USD between 1M and 200M
    insured_value = round(float(np.random.uniform(1_000_000, 200_000_000)), 2)

    # Exposure period: start sometime in past 5 years, duration up to 365 days
    start_date = faker.date_between(start_date='-5y', end_date='today')
    duration_days = random.randint(30, 365)
    end_date = start_date + datetime.timedelta(days=duration_days)

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

    voyage_count_year = random.randint(0, 50)
    risk_score = round(float(np.random.beta(2, 8) * 100), 2)
    claims_past_5y = random.randint(0, 5)
    deductible_rate = round(float(np.random.choice([0.01, 0.02, 0.05, 0.1])), 4)

    # Simple premium estimate: insured_value * base_rate * (1 + risk_adj)
    base_rate = 0.002  # 0.2% as baseline
    premium_estimate = round(insured_value * base_rate * (1 + risk_score / 200.0), 2)

    lat, lon = random_coordinates()

    return {
        "vessel_id": vessel_id,
        "vessel_name": vessel_name,
        "imo_number": imo_number,
        "build_year": build_year,
        "vessel_type": vessel_type,
        "operator": operator,
        "insured_value": insured_value,
        "exposure_start_date": pd.Timestamp(start_date),
        "exposure_end_date": pd.Timestamp(end_date),
        "tonnage": tonnage,
        "length_m": length_m,
        "cargo_type": cargo_type,
        "voyage_count_year": voyage_count_year,
        "risk_score": risk_score,
        "claims_past_5y": claims_past_5y,
        "deductible_rate": deductible_rate,
        "premium_estimate": premium_estimate,
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
    }


def generate_dataframe(rows=300, seed=None):
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)

    faker = Faker()
    if seed is not None:
        Faker.seed(seed)

    records = [generate_vessel_row(i + 1, faker, seed=seed) for i in range(rows)]
    df = pd.DataFrame.from_records(records)

    # Ensure datetime columns preserve timezone-naive timestamps
    df["exposure_start_date"] = pd.to_datetime(df["exposure_start_date"]).dt.tz_localize(None)
    df["exposure_end_date"] = pd.to_datetime(df["exposure_end_date"]).dt.tz_localize(None)

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate sample vessel insurance exposures and write Parquet.")
    parser.add_argument("--rows", type=int, default=300, help="Number of rows to generate (default: 300)")
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
