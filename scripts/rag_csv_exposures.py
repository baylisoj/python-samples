import csv
import os

import azure.identity
import openai
from dotenv import load_dotenv
from lunr import lunr

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

csv_path = os.path.join(os.path.dirname(__file__), "data", "exposures.csv")

# Index the data from the CSV
with open(csv_path) as file:
    reader = csv.reader(file)
    rows = list(reader)
documents = [{"id": (i + 1), "body": " ".join(row)} for i, row in enumerate(rows[1:])]
index = lunr(ref="id", fields=["body"], documents=documents)

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
    
    # Search the index for the user question
    results = index.search(user_question)
    matching_rows = [rows[int(result["ref"])] for result in results]
    
    # Format as a markdown table, since language models understand markdown
    matches_table = " | ".join(rows[0]) + "\n" + " | ".join(" --- " for _ in range(len(rows[0]))) + "\n"
    matches_table += "\n".join(" | ".join(row) for row in matching_rows)
    
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
