# app.py
from flask import Flask, request, jsonify
import sqlite3
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

# Function to connect to the SQLite database
def get_db_connection():
    print(os.getenv('DATABASE_NAME'))
    conn = sqlite3.connect(os.getenv('DATABASE_NAME'))  # Update with your actual database name
    conn.row_factory = sqlite3.Row
    return conn

# Endpoint to execute SQL queries
@app.route('/execute_query', methods=['POST'])
def execute_query():
    print("Received requests")
    data = request.json
    sql_query = data.get('sql_query')

    if not sql_query:
        return jsonify({"error": "No SQL query provided"}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        conn.close()

        # Convert the results to a list of dictionaries
        results_list = [dict(row) for row in results]

        # Return a single dictionary if there is only one result
        if len(results_list) == 1:
            return jsonify(results_list[0]), 200
        
        # If multiple results, wrap them in a dictionary with a key
        return jsonify({"results": results_list}), 200

    except sqlite3.OperationalError as e:
        print(f"SQLite Operational Error: {e}")  # More specific SQLite error
        return jsonify({"error": f"SQLite Operational Error: {e}"}), 500

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")  # General SQLite error
        return jsonify({"error": f"SQLite error: {e}"}), 500

    except Exception as e:
        print(f"General error: {e}")  # General error
        return jsonify({"error": "Server error occurred"}), 500

# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
