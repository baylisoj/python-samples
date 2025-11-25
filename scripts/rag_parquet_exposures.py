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
schema_info = con.execute(f"DESCRIBE SELECT * FROM '{parquet_path}'").df()
columns = schema_info['column_name'].tolist()
column_types = schema_info[['column_name', 'column_type']].to_dict('records')
print(f"Loaded {record_count} exposure records from parquet file")
print(f"Columns: {', '.join(columns)}")

# Schema description for SQL generation
SCHEMA_DESCRIPTION = f"""
Database Schema:
Table: exposures (stored in parquet file at '{parquet_path}')
Columns:
{chr(10).join([f"  - {col['column_name']}: {col['column_type']}" for col in column_types])}

Sample data types:
- vesselId: unique identifier for vessels (e.g., VSL-00001)
- vessel: name of the vessel
- operator: company operating the vessel
- year: year of operation
- type: vessel type
- value: vessel value in currency
- tonnage: vessel tonnage
- length: vessel length in meters
- cargoType: type of cargo
- premium: insurance premium amount
"""

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
    
    # Use Azure OpenAI to convert natural language to SQL
    sql_generation_prompt = f"""You are a SQL expert. Convert the user's natural language question into a DuckDB SQL query.

{SCHEMA_DESCRIPTION}

Important rules:
1. Use the exact path '{parquet_path}' in the FROM clause
2. Return ONLY the SQL query, no explanations or markdown
3. Limit results to 10 rows unless the user asks for specific aggregations
4. Use ILIKE for case-insensitive text matching
5. For "highest", "largest", "most" use ORDER BY DESC
6. For "lowest", "smallest", "least" use ORDER BY ASC
7. Always use proper SQL syntax for DuckDB

User question: {user_question}

SQL Query:"""

    try:
        # Generate SQL from natural language
        sql_response = client.chat.completions.create(
            model=MODEL_NAME,
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a SQL expert that converts natural language to DuckDB SQL queries. Return only the SQL query without markdown formatting or explanations."},
                {"role": "user", "content": sql_generation_prompt}
            ]
        )

        search_query = sql_response.choices[0].message.content.strip()
        # Clean up any markdown code blocks
        search_query = search_query.replace(
            "```sql", "").replace("```", "").strip()

        print(f"\nGenerated SQL Query:\n{search_query}\n")

        # Execute the generated SQL query
        matching_df = con.execute(search_query).df()
    except Exception as e:
        print(f"\nError executing query: {e}")
        print("Falling back to keyword search...\n")

        # Fallback to simple keyword search
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
