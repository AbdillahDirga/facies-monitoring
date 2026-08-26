"""
Data exploration script for the well log facies dataset.
Run this before building the MQTT replayer to understand the dataset's
structure, size, and data quality issues.

Usage:
    python explore_data.py --data ../data/raw/facies_vectors.csv
"""

import argparse

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Explore the well log facies dataset")
    parser.add_argument(
        "--data",
        type=str,
        default="../data/raw/facies_vectors.csv",
        help="Path to the facies CSV file (default: ../data/raw/facies_vectors.csv)",
    )
    return parser.parse_args()


def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} rows and {len(df.columns)} columns from {path}\n")
    return df


def show_structure(df: pd.DataFrame) -> None:
    print("=== COLUMN STRUCTURE ===")
    print(df.dtypes)
    print()


def preview_rows(df: pd.DataFrame, n: int = 5) -> None:
    print(f"=== FIRST {n} ROWS ===")
    print(df.head(n))
    print()


def show_wells_summary(df: pd.DataFrame, well_column: str = "Well Name") -> None:
    print("=== ROWS PER WELL ===")
    print(df[well_column].value_counts())
    print()


def show_missing_values(df: pd.DataFrame) -> None:
    print("=== MISSING VALUES PER COLUMN ===")
    missing = df.isnull().sum()
    missing_pct = (missing / len(df) * 100).round(2)
    summary = pd.DataFrame({"missing_count": missing, "missing_pct": missing_pct})
    print(summary[summary["missing_count"] > 0])
    print()


def show_facies_distribution(df: pd.DataFrame, facies_column: str = "Facies") -> None:
    print("=== FACIES CLASS DISTRIBUTION ===")
    print(df[facies_column].value_counts().sort_index())
    print()


def show_value_ranges(df: pd.DataFrame) -> None:
    print("=== VALUE RANGES (numeric columns) ===")
    print(df.describe())
    print()


def main():
    args = parse_args()
    df = load_data(args.data)
    show_structure(df)
    preview_rows(df)
    show_wells_summary(df)
    show_missing_values(df)
    show_facies_distribution(df)
    show_value_ranges(df)


if __name__ == "__main__":
    main()