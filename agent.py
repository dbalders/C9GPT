from typing_extensions import TypedDict, Optional
from langgraph.graph import StateGraph, START, END
from langsmith import traceable
from langsmith.wrappers import wrap_openai
import sqlite3
import openai
from litellm import completion
from dotenv import load_dotenv
import os
from fuzzywuzzy import process
from langgraph.checkpoint.memory import MemorySaver

load_dotenv()

client = wrap_openai(openai.Client())

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

schema = "Database Schema:\
1. teams: id [PK], name, logo, facebook, twitter, instagram, country, rank, rankingDevelopment\
2. team_news: id, team_id (FK), name, link\
3. players: id [PK], name, timeOnTeam, mapsPlayed, type\
4. player_stats: id (FK), month, year, ign, image, age, country, team, kills, headshots, deaths, kdRatio, damagePerRound, grenadeDamagePerRound, mapsPlayed, roundsPlayed, killsPerRound, assistsPerRound, deathsPerRound, savedByTeammatePerRound, savedTeammatesPerRound, rating1, rating2, roundsWithKills, zeroKillRounds, oneKillRounds, twoKillRounds, threeKillRounds, fourKillRounds, fiveKillRounds, openingKills, openingDeaths, openingKillRatio, openingKillRating, teamWinPercentAfterFirstKill, firstKillInWonRounds, rifleKills, sniperKills, smgKills, pistolKills, grenadeKills, otherKills, last_updated (PK: id, month, year)\
5. player_matches: id (FK), date, team1, team2, map, kills, deaths, rating, mapStatsId (PK: id, mapStatsId)\
6. matches: id, statsId, title, date, significance, format_type, format_location, status, hasScorebot, team1_name, team1_id, team1_rank, team2_name, team2_id, team2_rank, winnerTeam_name, winnerTeam_id, winnerTeam_rank, vetoes, event, odds, maps, players, streams, demos, highlightedPlayers, headToHead, highlights, playerOfTheMatch\
7. team_stats: id, month, year, name, mapsPlayed, wins, draws, losses, totalKills, totalDeaths, roundsPlayed, kdRatio, currentLineup, historicPlayers, standins, substitutes, matches, mapStats, events, length (PK: id, month, year)"

player_names = "Official Player Names (Adjust to these): Ax1le, Interz, Boombl4, HeavyGod, Icy"

examples = "Examples:\
1. User Query: What are the results of past matches?\
   SQL Query: `SELECT title, date, team1, team2, winnerTeam, status FROM matches WHERE status = \"Over\" ORDER BY date DESC;`\
2. User Query: What are Ax1Le's most recent stats?\
   SQL Query: `SELECT * FROM player_stats WHERE ign = 'Ax1Le' COLLATE NOCASE LIMIT 1;`\
3. User Query: How many kills per round does Ax1Le get?\
   SQL Query: `SELECT killsPerRound FROM player_stats WHERE ign = 'Ax1Le' COLLATE NOCASE LIMIT 1;`\
4. User Query: What are the upcoming matches?\
   SQL Query: `SELECT title, date, team1, team2, status FROM matches WHERE status = \"Scheduled\" ORDER BY date ASC;`\
5. User Query: What are Cloud9's stats for the last 3 months?\
    SQL Query: `Select * from team_stats WHERE name = \"Cloud9\" COLLATE NOCASE and year = strftime('%Y', 'now') and \"month\" >= strftime('%m', 'now', '-3 months');`"

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
         f"\n\n{schema}\n\n{examples}\n\n{player_names}\n\n{state['originalQuery']}")
    

    sql_query = query_local(context)

    return {"sqlQuery": sql_query}

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
    state["retryCount"] += 1
    context = (f"An error has occured in this SQLite query. Adjust the query to fix the error so it can be run again. Use the schema and examples to help solve it."
    f"\n\n{schema}\n\n{examples}\n\nSQL Error: {state["sqlError"]}\n\nSQL Query: {state["sqlQuery"]}\n\n Return only the adjusted query.")
    
    sql_query = query_local(context)
    return {"sql": sql_query}

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
        "Stop Execution" if state["retryCount"] >= 3 else "Fix Query Error"
    ),
    {
        "Summarize Results": "Summarize Results",
        "Fix Query Error": "Fix Query Error",
        "Stop Execution": END
    }
)
workflow_graph.add_edge("Fix Query Error", "Execute Query")
workflow_graph.add_edge("Summarize Results", END)

# graph = workflow_graph.compile(checkpointer=memory)
graph = workflow_graph.compile()

def run_gpt_agent(query: str) -> dict:
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
    result = graph.invoke(initial_state)
    # Stream back the summary in chunks
    # summary_generator = result["summary"]
    # if summary_generator is not None:
    #     for chunk in summary_generator:
    #         yield chunk

    summary = result.get("summary")

    print("Summary: ", summary)
    
    if summary:
        return {"summary": summary}
    
    return {"error": "No summary generated"}

# Example query
if __name__ == "__main__":
    query = "How many kills does axile have this month?"
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