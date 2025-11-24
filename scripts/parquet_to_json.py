
import pandas as pd
df = pd.read_parquet('.\data\exposures.parquet')
#df.to_csv('exposures.csv', index=False)

# Option 1: Each row as a JSON object in a list
df.to_json('.\data\exposures.json', orient='records', lines=False)

