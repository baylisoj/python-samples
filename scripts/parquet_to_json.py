import pandas as pd
import sys
from pathlib import Path

def parquet_to_json(input_file):
    """
    Convert a Parquet file to JSON format.
    
    Args:
        input_file: Path to the input Parquet file
    """
    input_path = Path(input_file)
    
    # Check if input file exists
    if not input_path.exists():
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    
    # Check if input file is a Parquet file
    if input_path.suffix.lower() != '.parquet':
        print("Error: Input file must be a Parquet file (.parquet extension).")
        sys.exit(1)
    
    # Generate output filename with .json extension
    output_path = input_path.with_suffix('.json')
    
    # Read Parquet file and convert to JSON
    df = pd.read_parquet(input_file)
    df.to_json(output_path, orient='records', lines=False)
    
    print(f"Successfully converted '{input_file}' to '{output_path}'")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python parquet_to_json.py <input_parquet_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    parquet_to_json(input_file)
