import pandas as pd
import json

# Function to convert Excel rows to JSON
def convert_xlsx_to_json(xlsx_file, output_file):
    # Read the Excel file (assuming the first two columns are Question and Query)
    df = pd.read_excel(xlsx_file, engine='openpyxl', header=None)

    # Define the system message once
    system_message = {
        "role": "system",
        "content": "You are a sql expert trying to create the SQL query to answer the user's question."
    }

    json_list = []

    # Iterate through each row in the Excel file
    for index, row in df.iterrows():
        # Create the JSON structure for each row
        conversation = {
            "messages": [
                system_message,
                {"role": "user", "content": row[0]},  # First column: Question
                {"role": "assistant", "content": row[1]}  # Second column: Query
            ]
        }
        json_list.append(conversation)

    # Write the output to a file or print it
    with open(output_file, 'w') as json_file:
        for conversation in json_list:
            json_file.write(json.dumps(conversation) + '\n')

    print(f"JSON conversion completed and saved to {output_file}")

# Usage
xlsx_file = 'C9GPT Examples.xlsx'  # Your Excel file
output_file = 'finetuning.jsonl'  # Desired output file

convert_xlsx_to_json(xlsx_file, output_file)