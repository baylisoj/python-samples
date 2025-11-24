import pandas as pd
from pathlib import Path


def parquet_to_csv(input_file: str) -> str:
    """
    Convert a parquet file to CSV format.
    
    Args:
        input_file: Path to the input parquet file
        
    Returns:
        Path to the output CSV file
    """
    input_path = Path(input_file)
    output_path = input_path.with_suffix('.csv')
    
    df = pd.read_parquet(input_path)
    df.to_csv(output_path, index=False)
    
    return str(output_path)


if __name__ == '__main__':
    # Example usage
    output_file = parquet_to_csv(r'.\data\exposures.parquet')
    print(f"Converted to: {output_file}")
