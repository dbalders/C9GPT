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

load_dotenv()

client = wrap_openai(openai.Client())

class AgentState(TypedDict):
    originalQuery: str
    originalDecision: str
    name: Optional[str]
    bestNameMatch: Optional[str]
    nameExists: Optional[bool]
    sqlQuery: Optional[str]
    sqlQueryResults: Optional[str]
    sqlError: Optional[str]
    summary: Optional[str]


workflow_graph = StateGraph(AgentState)

schema = f"Database Schema:
1. teams: id [PK], name, logo, facebook, twitter, instagram, country, rank, rankingDevelopment
2. team_news: id, team_id (FK), name, link
3. players: id [PK], name, timeOnTeam, mapsPlayed, type
4. player_stats: id (FK), month, year, ign, image, age, country, team, kills, headshots, deaths, kdRatio, damagePerRound, grenadeDamagePerRound, mapsPlayed, roundsPlayed, killsPerRound, assistsPerRound, deathsPerRound, savedByTeammatePerRound, savedTeammatesPerRound, rating1, rating2, roundsWithKills, zeroKillRounds, oneKillRounds, twoKillRounds, threeKillRounds, fourKillRounds, fiveKillRounds, openingKills, openingDeaths, openingKillRatio, openingKillRating, teamWinPercentAfterFirstKill, firstKillInWonRounds, rifleKills, sniperKills, smgKills, pistolKills, grenadeKills, otherKills, last_updated (PK: id, month, year)
5. player_matches: id (FK), date, team1, team2, map, kills, deaths, rating, mapStatsId (PK: id, mapStatsId)
6. matches: id, statsId, title, date, significance, format_type, format_location, status, hasScorebot, team1_name, team1_id, team1_rank, team2_name, team2_id, team2_rank, winnerTeam_name, winnerTeam_id, winnerTeam_rank, vetoes, event, odds, maps, players, streams, demos, highlightedPlayers, headToHead, highlights, playerOfTheMatch
7. team_stats: id, month, year, name, mapsPlayed, wins, draws, losses, totalKills, totalDeaths, roundsPlayed, kdRatio, currentLineup, historicPlayers, standins, substitutes, matches, mapStats, events, length (PK: id, month, year)"

player_names = f"Official Player Names (Adjust to these): Ax1le, Interz, Boombl4, HeavyGod, Icy"

examples = f"Examples:
1. User Query: What are the results of past matches?
   SQL Query: `SELECT title, date, team1, team2, winnerTeam, status FROM matches WHERE status = \"Over\" ORDER BY date DESC;`
2. User Query: What are Ax1Le's most recent stats?
   SQL Query: `SELECT * FROM player_stats WHERE ign = 'Ax1Le' COLLATE NOCASE LIMIT 1;`
3. User Query: How many kills per round does Ax1Le get?
   SQL Query: `SELECT killsPerRound FROM player_stats WHERE ign = 'Ax1Le' COLLATE NOCASE LIMIT 1;`
4. User Query: What are the upcoming matches?
   SQL Query: `SELECT title, date, team1, team2, status FROM matches WHERE status = \"Scheduled\" ORDER BY date ASC;`"

@traceable
def query_local(user_query):
    response = completion(
        model="ollama/llama3.1",  # Specify the model you've downloaded
        messages=[{"role": "user", "content": user_query}],  # User query
        api_base="http://localhost:11434"  # Point to your local Ollama server
    )
    return response

@traceable
def decide_request(state: AgentState) -> AgentState:
    context = f"You need to decide if this request needs an API call or if it is referring to data it already has and it just asking for a follow up. Respond with just the words API or Follow Up. \n\nUser Query: {state["user_query"]}"

    decision = query_local(context)
    return {"originalDecision": decision}

@traceable
def generate_sql_query(state: AgentState) -> AgentState:
    context = f"Your task is to interpret the user's query, generate the appropriate SQLite query that will be used for execution.
        Do not make up any fields sql, only use the ones given for any request. Double check them to make sure you are using a true field that exists in the table.
        Also if they ask about any specific name or team, extract that. Return the request as an object (name: [name], sql: [sql]) as one part and the SQLite query as the other. \n\n{schema}\n\n{examples}\n\n{player_names}\n\n{state["user_query"]}"
    
    sql_query = query_local(context)
    return {"name": sql_query.name, "sql": sql_query.sql}

@traceable
def check_name_exists(state: AgentState) -> AgentState:
    name = state["name"]
    conn = sqlite3.connect(os.getenv('DATABASE_NAME'))
    cursor = conn.cursor()
    query = f"SELECT name FROM players WHERE name = '{name}' COLLATE NOCASE"
    cursor.execute(query)
    result = cursor.fetchone()

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
    name = state["name"]
    best_name_match = state["bestNameMatch"]
    sql_query = state["sql"]
    
    adjusted_sql_query = sql_query.replace(name, best_name_match)
    return {"sqlQuery": adjusted_sql_query}

@traceable
def execute_query(state: AgentState) -> AgentState:
    try:
        conn = sqlite3.connect(os.getenv('DATABASE_NAME'))
        cursor = conn.cursor()

        cursor.execute(state["sql_query"])
        results = cursor.fetchall()
        conn.close()

        return {"sqlQueryResults": results, "error": None}
    
    except sqlite3.Error as e:
        # Catch any SQLite errors and return the error message
        return {"sqlQueryResults": None, "error": str(e)}

@traceable
def fix_query_error(state: AgentState) -> AgentState:
    context = f"An error has occured in this SQLite query. Adjust the query to fix the error so it can be run again. Use the schema and examples to help solve it.
    \n\n{schema}\n\n{examples}\n\nSQL Error: {state["sql_error"]}\n\nSQL Query: {state["sql_query"]}\n\n Return only the adjusted query."
    
    sql_query = query_local(context)
    return {"sql": sql_query}

@traceable
def summarize_results(state: AgentState) -> AgentState:
    context = f"Summarize the results of the query for the user based on their question and the query result.
    \n\nOriginal Query: {state["original_query"]}\n\nQuery Results: {state["query_results"]}"
    
    summary = query_local(context)
    return {"summary": summary}



workflow_graph.add_node("Decide Request Type", decide_request)
workflow_graph.add_node("Generate SQL Query", generate_sql_query)

workflow_graph.add_node("Check Name Exists", check_name_exists)
workflow_graph.add_node("Check Name Similarity", check_name_similarity)
workflow_graph.add_node("Adjust SQL Name", adjust_sql_name)

workflow_graph.add_node("Execute Query", execute_query)
workflow_graph.add_node("Fix Query Error", fix_query_error)

workflow_graph.add_node("Summarize Results", summarize_results)

# Example query
response = query_local("Tell me about the last 10 matches for Cloud9")
print(response)