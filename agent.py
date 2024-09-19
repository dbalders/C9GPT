from typing_extensions import TypedDict, Optional
from langgraph.graph import StateGraph, START, END
from langsmith import traceable
from langsmith.wrappers import wrap_openai
import sqlite3
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import StateGraph
import openai
from litellm import completion
from dotenv import load_dotenv
import os
from fuzzywuzzy import process

load_dotenv()

conn = sqlite3.connect('my_persistence.sqlite', check_same_thread=False)
saver = SqliteSaver(conn)

openai_client = wrap_openai(openai.Client())

class AgentState(TypedDict):
    originalQuery: str
    pathDecision: str
    name: Optional[str]
    bestNameMatch: Optional[str]
    nameExists: Optional[bool]
    sqlQuery: Optional[str]
    sqlQueryResults: Optional[str]
    sqlError: Optional[str]
    summary: Optional[str]
    sqlRetryCount: int
    currentDate: Optional[str]

workflow_graph = StateGraph(AgentState)
memory = MemorySaver()

def get_compact_sqlite_schema(database_path):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    schema = ""
    
    for table in tables:
        table_name = table[0]
        schema += f"{table_name}: "

        # Get the column information for each table
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        column_descriptions = []
        for column in columns:
            col_name = column[1]
            col_type = column[2]
            col_is_pk = "PK" if column[5] == 1 else ""
            column_descriptions.append(f"{col_name} {col_type} {col_is_pk}".strip())
        
        schema += ", ".join(column_descriptions) + "\n"

    conn.close()

    return schema

player_names = "Official Player Names (Adjust to these): Ax1le, Interz, Boombl4, HeavyGod, Icy"

schema = get_compact_sqlite_schema(os.getenv('DATABASE_NAME'))

examples = "Examples:\
1. User Query: What are the results of past matches?\
   SQL Query: `SELECT date, format_type, team1_name, team1_id, team2_name, team2_id, winnerTeam_name, winnerTeam_id, vetoes, maps, players FROM matches WHERE status = 'Over' ORDER BY date DESC LIMIT 5;`\
2. User Query: What are Ax1Le's most recent stats?\
   SQL Query: `SELECT kills, headshots, deaths, kdRatio, damagePerRound, mapsPlayed, roundsPlayed, killsPerRound, assistsPerRound, deathsPerRound, rating2, openingKillRatio, openingKillRating FROM player_stats WHERE ign = 'Ax1Le' COLLATE NOCASE LIMIT 3;`\
3. User Query: How many kills per round does Ax1Le get?\
   SQL Query: `SELECT kills, roundsPlayed, killsPerRound FROM player_stats WHERE ign = 'Ax1Le' COLLATE NOCASE LIMIT 3;`\
4. User Query: What are the upcoming matches?\
   SQL Query: `SELECT title, event, date, team1_name , team2_name , status FROM matches WHERE status = 'Scheduled' ORDER BY date ASC;`\
5. User Query: What are Cloud9's stats for the last 3 months?\
    SQL Query: `Select * from team_stats WHERE name = 'Cloud9' COLLATE NOCASE ORDER BY year DESC, month DESC limit 3;`"

@traceable
def query_local(user_query):
    response = completion(
        model="ollama/llama3.1",  # Specify the model you've downloaded
        messages=[{"role": "user", "content": user_query}],  # User query
        api_base="http://localhost:11434"  # Point to your local Ollama server
    )
    if response and response.choices:
        return response.choices[0].message.content  # Return just the message content

    return None

@traceable
def decide_request(state: AgentState) -> AgentState:
    context = (f"You need to decide if this request needs an API call or if it is referring to data it already has and it just asking for a follow up."
               f"If there is only one question, most likely needs API, or asking for more data or stats or about matches."
                f"Respond with just 'API' or 'Follow Up'. \n\nUser Query: {state["originalQuery"]}\n\nCurrent Data: {state["sqlQueryResults"]}")

    decision = query_local(context)
    print("State", state)
    print("Decision: ", decision)
    print(state)
    return {"pathDecision": decision}

@traceable
def get_name_from_query(state: AgentState) -> AgentState:
    print("in get_name_from_query")
    context = (f"Extract the name from the user's query. The name could be a player name or a team name. Return the name as a string and only the name.\n\n{state['originalQuery']}")
    name = query_local(context)
    return {"name": name}

@traceable
def generate_sql_query(state: AgentState) -> AgentState:
    print("in generate_sql_query")

    context = (f"Your task is to interpret the user's query, generate the appropriate SQLite query that will be used for execution."
        f"Do not make up any fields sql, only use the ones given for any request. Double check them to make sure you are using a true field that exists in the table. Return just the sql query and DO NOT format the query using triple backticks or code blocks."
         f"\n\n{schema}\n\n{player_names}\n\n{state['originalQuery']}")
    

    sql_query = openai_client.chat.completions.create(
        messages=[{"role": "user", "content": context}],
        model=os.getenv('FINE_TUNED_MODEL')
    )
    print("SQL Query: ", sql_query.choices[0].message.content)
    return {"sqlQuery": sql_query.choices[0].message.content}

@traceable
def check_name_exists(state: AgentState) -> AgentState:
    print("in check_name_exists")
    name = state["name"]
    print("Name: ", name)
    conn = sqlite3.connect(os.getenv('DATABASE_NAME'))
    cursor = conn.cursor()
    query = f"SELECT name FROM players WHERE name = '{name}' COLLATE NOCASE"
    cursor.execute(query)
    result = cursor.fetchone()

    print("Name Exists: ", result)

    #if name exists, return nameExists as True
    if result:
        return {"nameExists": True}
    else:
        #check teams now
        query = f"SELECT name FROM teams WHERE name = '{name}' COLLATE NOCASE"
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()
        if result:
            return {"nameExists": True}
        else:
            return {"nameExists": False}

@traceable
def check_name_similarity(state: AgentState) -> AgentState:
    print("in check_name_similarity")
    conn = sqlite3.connect(os.getenv('DATABASE_NAME'))
    cursor = conn.cursor()
    name = state["name"]

    cursor.execute("SELECT name FROM players")
    all_names = cursor.fetchall()
    all_names_list = [name[0] for name in all_names]
    best_name_match = process.extractOne(name, all_names_list)

    cursor.execute("SELECT name FROM teams")
    all_teams = cursor.fetchall()
    all_teams_list = [team[0] for team in all_teams]
    best_team_match = process.extractOne(name, all_teams_list)

    conn.close()

    if best_name_match[1] > best_team_match[1]:
        return {"bestNameMatch": best_name_match[0]}
    else:
        return {"bestNameMatch": best_team_match[0]}

@traceable
def adjust_sql_name(state: AgentState) -> AgentState:
    print("in adjust_sql_name")
    name = state["name"]
    best_name_match = state["bestNameMatch"]
    sql_query = state["sqlQuery"]
    
    adjusted_sql_query = sql_query.replace(name, best_name_match)
    return {"sqlQuery": adjusted_sql_query}

@traceable
def execute_query(state: AgentState) -> AgentState:
    print("in execute_query")
    print("SQL Query: ", state["sqlQuery"])
    try:
        conn = sqlite3.connect(os.getenv('DATABASE_NAME'))
        cursor = conn.cursor()

        cursor.execute(state["sqlQuery"])
        results = cursor.fetchall()
        conn.close()
        print("Results: ", results)

        return {"sqlQueryResults": results, "error": None}
    
    except sqlite3.Error as e:
        return {"sqlQueryResults": None, "error": str(e)}

@traceable
def fix_query_error(state: AgentState) -> AgentState:
    sqlRetryCount = state["sqlRetryCount"] + 1
    context = (f"An error has occured in this SQLite query. Adjust the query to fix the error so it can be run again. Use the table schema and examples to help solve it. Make sure all columns actually exist."
    f"\n\n{schema}\n\n{examples}\n\nSQL Error: {state["sqlError"]}\n\nSQL Query: {state["sqlQuery"]}\n\n Return only the adjusted query."
    f"Return it as a string and DO NOT format the query using triple backticks or code blocks.")
    
    sql_query = query_local(context)
    return {"sqlQuery": sql_query, "sqlError": None, "sqlQueryResults": None, "sqlRetryCount": sqlRetryCount}

@traceable
def summarize_results(state: AgentState) -> AgentState:
    context = (f"Summarize the results of the query for the user based on their question and the query result."
    f"\n\nOriginal Query: {state["originalQuery"]}\n\nQuery Results: {state["sqlQueryResults"]}")
    
    summary = query_local(context)
    return {"summary": summary}

@traceable
def summarize_follow_up(state: AgentState) -> AgentState:
    context = (f"Summarize the follow up question using the original query and the state from the query."
    f"\n\nOriginal Query: {state["originalQuery"]}\n\nQuery Results: {state["sqlQueryResults"]}")
    
    summary = query_local(context)
    return {"summary": summary}


# Nodes
workflow_graph.add_node("Decide Request Type", decide_request)
workflow_graph.add_node("Get Name From Query", get_name_from_query)
workflow_graph.add_node("Generate SQL Query", generate_sql_query)

workflow_graph.add_node("Check Name Exists", check_name_exists)
workflow_graph.add_node("Check Name Similarity", check_name_similarity)
workflow_graph.add_node("Adjust SQL Name", adjust_sql_name)

workflow_graph.add_node("Execute Query", execute_query)
workflow_graph.add_node("Fix Query Error", fix_query_error)

workflow_graph.add_node("Summarize Results", summarize_results)

workflow_graph.add_node("Summarize Follow Up", summarize_follow_up)

#Edges
workflow_graph.add_edge(START, "Decide Request Type")
workflow_graph.add_conditional_edges(
    "Decide Request Type",
    lambda state: "API" if state["pathDecision"] == "API" else "Follow Up",  
    {
        "API": "Get Name From Query",
        "Follow Up": "Summarize Follow Up"
    }  
)

#API Path
workflow_graph.add_edge("Get Name From Query", "Generate SQL Query")
workflow_graph.add_edge("Generate SQL Query", "Check Name Exists")

#If name exists, execute query, else check name similarity
workflow_graph.add_conditional_edges(
    "Check Name Exists",
    lambda state: "Execute Query" if state["nameExists"] else "Check Name Similarity",
    {
        "Check Name Similarity": "Check Name Similarity",
        "Execute Query": "Execute Query"
    }
)
workflow_graph.add_edge("Check Name Exists", "Check Name Similarity")
workflow_graph.add_edge("Check Name Similarity", "Adjust SQL Name")
workflow_graph.add_edge("Adjust SQL Name", "Execute Query")

#Check if query was successful, if not try to fix it
workflow_graph.add_conditional_edges(
    "Execute Query",
    lambda state: "Summarize Results" if state["sqlQueryResults"] else (
        "Stop Execution" if state["sqlRetryCount"] >= 3 else "Fix Query Error"
    ),
    {
        "Summarize Results": "Summarize Results",
        "Fix Query Error": "Fix Query Error",
        "Stop Execution": END
    }
)
workflow_graph.add_edge("Fix Query Error", "Execute Query")
workflow_graph.add_edge("Summarize Results", END)

graph = workflow_graph.compile(checkpointer=saver)
# print(graph.get_graph().draw_ascii())

def run_gpt_agent(query: str, thread_id: int) -> dict:
    config = {"configurable": {"thread_id": thread_id}}
    current_state = graph.get_state(config)
    print("Current State: ", current_state)
    if not current_state.values:
        initial_state = {
            "originalQuery": query,
            "pathDecision": None,
            "name": None,
            "bestNameMatch": None,
            "nameExists": None,
            "sqlQuery": None,
            "sqlQueryResults": None,
            "sqlError": None,
            "summary": None,
            "sqlRetryCount": 0
        }
    else:
        initial_state = {
            **current_state.values,  # Use previous state data
            "originalQuery": query  # Update with the new query
        }
    
    print("Thread ID: ", thread_id)
    print("Current State: ", current_state)
    result = graph.invoke(initial_state, config)
    # Stream back the summary in chunks
    # summary_generator = result["summary"]
    # if summary_generator is not None:
    #     for chunk in summary_generator:
    #         yield chunk

    summary = result.get("sqlQueryResults")

    print("Summary: ", summary)
    
    if summary:
        return {"summary": summary, "sqlQuery": result["sqlQuery"]}
    
    return {"error": "No summary generated"}

# Example query
if __name__ == "__main__":
    query = "How many kills does axile have?"
    initial_state = {
        "originalQuery": query,
        "pathDecision": None,
        "name": None,
        "bestNameMatch": None,
        "nameExists": None,
        "sqlQuery": None,
        "sqlQueryResults": None,
        "sqlError": None,
        "summary": None,
        "sqlRetryCount": 0
    }
    response = graph.invoke(initial_state)
    print(response)