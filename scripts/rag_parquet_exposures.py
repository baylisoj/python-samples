import os

import azure.identity
import duckdb
import openai
from dotenv import load_dotenv

# Setup the Azure OpenAI client
load_dotenv(override=True)

token_provider = azure.identity.get_bearer_token_provider(
    azure.identity.DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
)
client = openai.AzureOpenAI(
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    azure_ad_token_provider=token_provider,
    api_version="2024-08-01-preview"
)
MODEL_NAME = os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT"]

# Connect to DuckDB and load parquet file metadata
con = duckdb.connect()
parquet_path = os.path.join(os.path.dirname(__file__), "data", "exposures.parquet")
record_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
columns = con.execute(f"DESCRIBE SELECT * FROM '{parquet_path}'").df()['column_name'].tolist()
print(f"Loaded {record_count} exposure records from parquet file")
print(f"Columns: {', '.join(columns)}")

# System message for the AI assistant
SYSTEM_MESSAGE = """
You are a helpful assistant that answers questions about vessel insurance exposures based on an exposures data set.
You must use the data set to answer the questions, you should not provide any info that is not in the provided sources.
"""

# Loop to handle multiple questions
while True:
    # Get the user question
    user_question = input("\nEnter your question about vessel insurance exposures (or 'quit' to exit): ")
    
    if user_question.lower() in ['quit', 'exit', 'q']:
        print("Goodbye!")
        break
    
    if not user_question.strip():
        continue
    
    # Use DuckDB to search across all columns
    # Build a SQL query that searches all text columns for keywords
    search_query = f"""
    SELECT *
    FROM '{parquet_path}'
    WHERE 
        CAST(vesselId AS VARCHAR) ILIKE '%{user_question}%' OR
        CAST(vessel AS VARCHAR) ILIKE '%{user_question}%' OR
        CAST(operator AS VARCHAR) ILIKE '%{user_question}%' OR
        CAST(year AS VARCHAR) ILIKE '%{user_question}%' OR
        CAST(type AS VARCHAR) ILIKE '%{user_question}%' OR
        CAST(value AS VARCHAR) ILIKE '%{user_question}%' OR
        CAST(tonnage AS VARCHAR) ILIKE '%{user_question}%' OR
        CAST(length AS VARCHAR) ILIKE '%{user_question}%' OR
        CAST(cargoType AS VARCHAR) ILIKE '%{user_question}%' OR
        CAST(premium AS VARCHAR) ILIKE '%{user_question}%'
    LIMIT 10
    """
    
    matching_df = con.execute(search_query).df()
    
    # Format as a markdown table
    if len(matching_df) > 0:
        matches_table = matching_df.to_markdown(index=False)
    else:
        matches_table = "No matching records found."
    
    print("\nFound matches:")
    print(matches_table)
    
    # Now we can use the matches to generate a response
    response = client.chat.completions.create(
        model=MODEL_NAME,
        temperature=0.3,
        messages=[
            {"role": "system", "content": SYSTEM_MESSAGE},
            {"role": "user", "content": f"{user_question}\nSources: {matches_table}"},
        ],
    )
    
    print("\nResponse from Azure OpenAI:\n")
    print(response.choices[0].message.content)

# Close DuckDB connection
con.close()
