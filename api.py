# app.py
from flask import Flask, request, jsonify
import sqlite3
from dotenv import load_dotenv
import os
from agent import run_gpt_agent

load_dotenv()

app = Flask(__name__)

# Function to connect to the SQLite database
def get_db_connection():
    conn = sqlite3.connect(os.getenv('DATABASE_NAME'))  # Update with your actual database name
    conn.row_factory = sqlite3.Row
    return conn

# Endpoint to execute SQL queries
@app.route('/execute_query', methods=['POST'])
def execute_query():
    print("Executing query")
    # Get the query from the request
    data = request.json
    print(data)
    user_query = data.get('user_query')

    if not user_query:
        return jsonify({"error": "No user query provided"}), 400

    try:
        print(f"User query: {user_query}")
        # Send the query to the GPT agent
        result = run_gpt_agent(user_query)

        # Stream back the results
        response = ""
        for chunk in result:
            response += chunk
        return jsonify({"summary": response}), 200

    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"error": "Server error occurred"}), 500

# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
