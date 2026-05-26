from typing_extensions import TypedDict, Optional
from langgraph.graph import StateGraph, START, END
from langchain_core.runnables.config import RunnableConfig
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
from datetime import datetime

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

players = [
    {"name": "Ax1Le", "id": 16555},
    {"name": "interz", "id": 15071},
    {"name": "HeavyGod", "id": 20447},
    {"name": "ICY", "id": 21439},
    {"name": "Boombl4", "id": 11840}
]

# Format it into a prompt-friendly string
player_prompt = "Here are the player names and their IDs if needed for any tables:\n"
for player in players:
    player_prompt += f"Name: {player['name']}, ID: {player['id']}\n"

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
    SQL Query: `Select * from team_stats WHERE name = 'Cloud9' COLLATE NOCASE ORDER BY year DESC, month DESC limit 3;`\
6. User Query: What are the most common vetoes against Cloud9?\
    SQL Query: `SELECT json_extract(value, '$.map') AS vetoed_map, COUNT(*) AS veto_count FROM matches, json_each(matches.vetoes) WHERE json_extract(value, '$.team.name') != 'Cloud9' GROUP BY vetoed_map ORDER BY veto_count DESC LIMIT 5;`\
7. User Query: What is Cloud9's success rate on mirage in the past year? \
    SQL Query: `SELECT COUNT(*) as matches_played, SUM(CASE WHEN winnerTeam_name = 'Cloud9' THEN 1 ELSE 0 END) as matches_won FROM matches WHERE (team1_name = 'Cloud9' OR team2_name = 'Cloud9') AND maps LIKE '%mirage%' AND datetime(date / 1000, 'unixepoch') >= datetime('now', '-365 days'); \
8. User Query: Who had the best performance in Cloud9’s last match?\
    SQL Query: `WITH recent_match AS (SELECT maps FROM matches WHERE (team1_name = 'Cloud9' OR team2_name = 'Cloud9') ORDER BY date DESC LIMIT 1), map_stats AS (SELECT json_extract(value, '$.statsId') AS statsId FROM recent_match, json_each(recent_match.maps)), map_count AS (SELECT COUNT(*) AS map_count FROM map_stats) SELECT players.name AS player_name, SUM(player_matches.rating) / (SELECT map_count FROM map_count) AS avg_rating FROM player_matches JOIN players ON player_matches.id = players.id WHERE player_matches.mapStatsId IN (SELECT statsId FROM map_stats) GROUP BY players.name ORDER BY avg_rating DESC;`\
9. User Query: What is Ax1Le's Rating?\
    SQL Query: `SELECT rating2, roundsPlayed FROM player_stats WHERE ign = 'Ax1Le' COLLATE NOCASE ORDER BY year DESC, month DESC LIMIT 3;`\
10. User Query: How many 1-, 2-, 3-, 4-, or 5-kill rounds has Ax1Le had in the past 6 months?\
    SQL Query: `SELECT SUM(oneKillRounds), SUM(twoKillRounds), SUM(threeKillRounds), SUM(fourKillRounds), SUM(fiveKillRounds), SUM(roundsPlayed) FROM player_stats WHERE ign = 'Ax1Le' COLLATE NOCASE ORDER BY year DESC, month DESC LIMIT 6;`"


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
    print  ("in decide_request")
    print("Original Query: ", state["originalQuery"])
    print("Current Data: ", state["sqlQueryResults"])
    context = (f"You need to decide if this request needs an API call or if it is referring to data it already has and it just asking for a follow up."
               f"Read the user query and check the current data to see if the answer is found. If the explicit answer is not found, it needs an API call."
               f"If you are at all unsure, respond with 'API'. If you are completely sure that the summary contains the data, respond with 'Follow Up'."
                f"If current data is blank, it needs an API call."
                f"Respond with just 'API' or 'Follow Up'. \n\nUser Query: {state['originalQuery']}\n\nPrevious Summary: {state['summary']}")

    decision = query_local(context)
    print("State", state)
    print("Decision: ", decision)
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

    current_date = datetime.now().strftime("%Y-%m-%d")
    context = (f"INSTRUCTIONS:\n\nYou are an sql expert. Your task is to interpret the user's query and generate the appropriate SQLite query based on the table data you are given."
        f"Do not make up any fields, tables, or rows. Useonly the ones given for any request. Check for any relationships and use those if needed for queries. Double check them to make sure you are using a true field that exists in the table. "
        f"The tables can be linked via the main player and team IDs if names are not available. Return just the sql query and DO NOT format the query using triple backticks or code blocks."
         f"Here is the table schema:\n\n{schema}\n\n{player_prompt}\n\nHere are some examples: \n{examples}\n\nThe current date is {current_date}\n\nQuestion: {state['originalQuery']}.")
    

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
    f"\n\n{schema}\n\n{examples}\n\nSQL Error: {state['sqlError']}\n\nSQL Query: {state['sqlQuery']}\n\n Return only the adjusted query."
    f"Return it as a string and DO NOT format the query using triple backticks or code blocks.")
    
    sql_query = query_local(context)
    return {"sqlQuery": sql_query, "sqlError": None, "sqlQueryResults": None, "sqlRetryCount": sqlRetryCount}

@traceable
def summarize_results(state: AgentState) -> AgentState:
    context = (f"Summarize the results of the query for the user based on their question and the query result."
    f"\n\nOriginal Query: {state['originalQuery']}\n\nQuery Results: {state['sqlQueryResults']}")
    
    summary = query_local(context)
    return {"summary": summary}

@traceable
def summarize_follow_up(state: AgentState) -> AgentState:
    context = (f"Summarize the follow up question using the original query and the state from the query."
    f"\n\nOriginal Query: {state['originalQuery']}\n\nQuery Results: {state['sqlQueryResults']}")
    
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

def run_gpt_agent(config: RunnableConfig) -> dict:
    # Extract query and thread_id from config
    query = config.get("query")
    thread_id = config.get("thread_id")

    if not query:
        return {"error": "No query provided"}

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
    config = {"configurable": {"thread_id": 1}}
    response = graph.invoke(initial_state, config)
    print(response)