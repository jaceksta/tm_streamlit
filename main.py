import streamlit as st
import duckdb
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from functions import extract_leagues_from_url, extract_tables_from_url, merge_tables_leagues


url = st.text_input("Enter Transfermarkt URL", "https://www.transfermarkt.pl/igor-sapala/profil/spieler/380597")
suffix_url = '/plus/0?saison=2024&verein=&liga=&wettbewerb=&pos=&trainer_id='



updated_url = re.sub(r"(://www\.transfermarkt)\.[a-z.]+", r"\1.com", url)
updated_url = updated_url.replace("profil", "leistungsdatendetails")
updated_url = updated_url + suffix_url


leagues = extract_leagues_from_url(updated_url)

leagues_dict = [{"name": league, "number": index} for index, league in enumerate(leagues)]

# Use `st.multiselect` with only the league names
selected_league_names = st.multiselect(
"Choose leagues:",
options=[league["name"] for league in leagues_dict]
)

# Map the selected names back to their indices
selected_league_numbers = [
league["number"] for league in leagues_dict if league["name"] in selected_league_names
]



tables = extract_tables_from_url(updated_url)   

# Assuming `df` is the DataFrame you've obtained

all_data = pd.DataFrame(columns = ['Date','Stage', 'Opponent_Name', 'Minutes_Played', 'Comp'])

for i in selected_league_numbers:
    all_data  = pd.concat([all_data,merge_tables_leagues(tables, leagues, i)])

all_data['Date'] = pd.to_datetime(all_data['Date'])
all_data = all_data.sort_values(by='Date')
df = pd.DataFrame(all_data)
df['Minutes_Played'] = pd.to_numeric(df['Minutes_Played'].str.replace("'", ""), errors='coerce').fillna(0)


# Connect to DuckDB and create an in-memory table
conn = duckdb.connect()
conn.execute("CREATE TABLE games AS SELECT * FROM df")

# Queries to extract statistics

# 1. Number of games missed
games_missed = conn.execute("""
SELECT COUNT(*) AS games_missed
FROM games
WHERE Minutes_Played = 0
""").fetchall()[0][0]


total_games = conn.execute("""
SELECT COUNT(*) AS total_games
FROM games
""").fetchall()[0][0]

# 2. Most number of games missed in a row
longest_miss_streak = conn.execute("""
SELECT MAX(streak) AS longest_miss_streak
FROM (
SELECT COUNT(*) AS streak
FROM (
SELECT *, 
CASE WHEN Minutes_Played = 0 THEN 1 ELSE 0 END AS missed_game,
ROW_NUMBER() OVER (ORDER BY Date) - 
SUM(CASE WHEN Minutes_Played = 0 THEN 1 ELSE 0 END) OVER (ORDER BY Date) AS grp
FROM games
) 
WHERE missed_game = 1
GROUP BY grp
)
""").fetchall()[0][0]

# 3. Number of times player missed 2+ games in a row
missed_2_plus_streaks = conn.execute("""
SELECT COUNT(*) AS missed_2_plus_streaks
FROM (
SELECT COUNT(*) AS streak
FROM (
SELECT *, 
CASE WHEN Minutes_Played = 0 THEN 1 ELSE 0 END AS missed_game,
ROW_NUMBER() OVER (ORDER BY Date) - 
SUM(CASE WHEN Minutes_Played = 0 THEN 1 ELSE 0 END) OVER (ORDER BY Date) AS grp
FROM games
)
WHERE missed_game = 1
GROUP BY grp
HAVING streak >= 2
)
""").fetchall()[0][0]

# 4. Number of times player missed 3+ games in a row
missed_3_plus_streaks = conn.execute("""
SELECT COUNT(*) AS missed_3_plus_streaks
FROM (
SELECT COUNT(*) AS streak
FROM (
SELECT *, 
CASE WHEN Minutes_Played = 0 THEN 1 ELSE 0 END AS missed_game,
ROW_NUMBER() OVER (ORDER BY Date) - 
SUM(CASE WHEN Minutes_Played = 0 THEN 1 ELSE 0 END) OVER (ORDER BY Date) AS grp
FROM games
)
WHERE missed_game = 1
GROUP BY grp
HAVING streak >= 3
)
""").fetchall()[0][0]

# 5. Longest streak of games with more than 75 minutes played
longest_play_streak_75_plus = conn.execute("""
SELECT MAX(streak) AS longest_play_streak_75_plus
FROM (
SELECT COUNT(*) AS streak
FROM (
SELECT *, 
CASE WHEN Minutes_Played > 75 THEN 1 ELSE 0 END AS played_75_plus,
ROW_NUMBER() OVER (ORDER BY Date) - 
SUM(CASE WHEN Minutes_Played > 75 THEN 1 ELSE 0 END) OVER (ORDER BY Date) AS grp
FROM games
)
WHERE played_75_plus = 1
GROUP BY grp
)
""").fetchall()[0][0]


games_75_minutes = conn.execute("""
SELECT COUNT(*) AS games_missed
FROM games
WHERE Minutes_Played >=75
""").fetchall()[0][0]

total_minutes_played = conn.execute("""
select sum(cast(Minutes_Played as integer)) from games
""").fetchall()[0][0]

# Print results

tab1, tab2 = st.tabs(['Stats', 'Games'])
with tab1:
    st.write("Total games missed:", games_missed)
    st.write("Total games possible:", total_games)
    st.write("Times missed 2+ games in a row:", missed_2_plus_streaks)
    st.write("Times missed 3+ games in a row:", missed_3_plus_streaks)
    st.write("Longest streak of games missed:", longest_miss_streak)
    st.write("Longest streak of games with 75+ minutes played:", longest_play_streak_75_plus)
    st.write("Games with more than 75 minutes:", games_75_minutes)
    st.write("Total Minutes Played:", total_minutes_played)
with tab2:
    st.table(df)







# Convert the 'Minutes_Played' to numeric for analysis, using 0 for 'Not in squad' and 'Yellow card suspension'



