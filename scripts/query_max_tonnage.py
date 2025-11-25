import duckdb

# Query the parquet file directly
print('Columns and sample data:')
print(duckdb.sql("SELECT * FROM './scripts/data/exposures.parquet' LIMIT 10"))

# Get total record count
count_result = duckdb.sql("SELECT COUNT(*) as total FROM './scripts/data/exposures.parquet'")
print(f'Total records: {count_result}')

# # Query for the vessel with the highest tonnage
print(duckdb.sql("""SELECT vessel, operator, year, tonnage 
                 FROM './scripts/data/exposures.parquet'
                 ORDER BY tonnage DESC
                 LIMIT 1
                 """))
