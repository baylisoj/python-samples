
import pandas as pd
df = pd.read_parquet('.\data\exposures.parquet')
df.to_csv('exposures.csv', index=False)
