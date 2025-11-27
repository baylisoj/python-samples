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

# Parquet file path
parquet_path = os.path.join(os.path.dirname(__file__), "data", "exposures.parquet")


def get_schema_info():
    """Load parquet file metadata and return schema information."""
    with duckdb.connect() as con:
        record_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
        schema_info = con.execute(f"DESCRIBE SELECT * FROM '{parquet_path}'").df()
        columns = schema_info['column_name'].tolist()
        column_types = schema_info[['column_name', 'column_type']].to_dict('records')
    return record_count, columns, column_types

def get_schema_description():
    """Generate schema description for SQL generation."""
    _, _, column_types = get_schema_info()
    return f"""
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


def get_response_for_chatbot(user_question: str, conversation_history: list = None) -> dict:
    """
    Process a user question and return structured message data for chatbot integration.
    
    Args:
        user_question: The user's question about vessel insurance exposures
        conversation_history: Optional list of previous messages in the format:
                             [{"role": "user|assistant|system", "content": "..."}]
    
    Returns:
        dict with keys:
            - "messages": list of all messages including system, user, and assistant responses
            - "sql_query": the generated SQL query (for debugging/logging)
            - "data": the raw dataframe as dict (for programmatic access)
            - "success": boolean indicating if the query was successful
            - "error": error message if success is False
    """
    if conversation_history is None:
        conversation_history = []
    
    schema_description = get_schema_description()
    
    # Use Azure OpenAI to convert natural language to SQL
    sql_generation_prompt = f"""You are a SQL expert. Convert the user's natural language question into a DuckDB SQL query.

{schema_description}

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
        search_query = search_query.replace("```sql", "").replace("```", "").strip()

        # Execute the generated SQL query with its own connection
        with duckdb.connect() as con:
            matching_df = con.execute(search_query).df()

    except Exception:
        # Fallback to simple keyword search using parameterized query
        search_query = f"""
        SELECT *
        FROM '{parquet_path}'
        WHERE 
            CAST(vesselId AS VARCHAR) ILIKE ? OR
            CAST(vessel AS VARCHAR) ILIKE ? OR
            CAST(operator AS VARCHAR) ILIKE ? OR
            CAST(year AS VARCHAR) ILIKE ? OR
            CAST(type AS VARCHAR) ILIKE ? OR
            CAST(value AS VARCHAR) ILIKE ? OR
            CAST(tonnage AS VARCHAR) ILIKE ? OR
            CAST(length AS VARCHAR) ILIKE ? OR
            CAST(cargoType AS VARCHAR) ILIKE ? OR
            CAST(premium AS VARCHAR) ILIKE ?
        LIMIT 10
        """
        # Create parameter list with wildcards for ILIKE pattern matching
        search_pattern = f'%{user_question}%'
        params = [search_pattern] * 10  # One parameter for each column
        try:
            with duckdb.connect() as con:
                matching_df = con.execute(search_query, params).df()
        except Exception as fallback_error:
            return {
                "messages": conversation_history + [
                    {"role": "system", "content": SYSTEM_MESSAGE},
                    {"role": "user", "content": user_question},
                    {"role": "assistant",
                        "content": f"I encountered an error processing your question: {str(fallback_error)}"}
                ],
                "sql_query": search_query,
                "data": None,
                "success": False,
                "error": str(fallback_error)
            }
    
    # Format as a markdown table
    if len(matching_df) > 0:
        matches_table = matching_df.to_markdown(index=False)
    else:
        matches_table = "No matching records found."
    
    # Generate the final response using the data
    response = client.chat.completions.create(
        model=MODEL_NAME,
        temperature=0.3,
        messages=[
            {"role": "system", "content": SYSTEM_MESSAGE},
            {"role": "user", "content": f"{user_question}\nSources: {matches_table}"},
        ],
    )
    
    assistant_response = response.choices[0].message.content

    # Build the complete message history
    # Always ensure system message is at the start
    if not conversation_history or conversation_history[0].get("role") != "system":
        messages = [{"role": "system", "content": SYSTEM_MESSAGE}]
        if conversation_history:
            messages.extend(conversation_history)
    else:
        messages = conversation_history.copy()

    # Append the new user and assistant messages
    messages.extend([
        {"role": "user", "content": user_question},
        {"role": "assistant", "content": assistant_response}
    ])

    return {
        "messages": messages,
        "sql_query": search_query,
        "data": matching_df.to_dict('records') if len(matching_df) > 0 else [],
        "success": True,
        "error": None
    }


# Example CLI interface (can be removed when integrating with a chatbot)
if __name__ == "__main__":
    # Display initial schema information
    record_count, columns, _ = get_schema_info()
    print(f"Loaded {record_count} exposure records from parquet file")
    print(f"Columns: {', '.join(columns)}")
    
    # Maintain conversation history across the session
    conversation_history = []

    # Loop to handle multiple questions
    while True:
        # Get the user question
        user_question = input(
            "\nEnter your question about vessel insurance exposures (or 'quit' to exit): ")

        if user_question.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break

        if not user_question.strip():
            continue

        # Call the chatbot function with conversation history
        result = get_response_for_chatbot(user_question, conversation_history)

        # Update the conversation history for the next turn
        conversation_history = result["messages"]

        print("\n--- Result ---")
        print(f"Total messages in history: {len(conversation_history)}")

        # Display results
        if result["success"]:
            print(f"\nGenerated SQL Query:\n{result['sql_query']}\n")
            print(f"Found {len(result['data'])} records\n")
            print("Response from Azure OpenAI:\n")
            print(result["messages"][-1]["content"])
        else:
            print(f"\nError: {result['error']}")
