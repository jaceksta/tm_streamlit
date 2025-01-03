import streamlit as st
import duckdb
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from functions import extract_leagues_from_url, extract_tables_from_url, merge_tables_leagues, parse_html

url = st.text_input("Enter Transfermarkt URL", "https://www.transfermarkt.pl/igor-sapala/profil/spieler/380597")
suffix_url_template = '/plus/0?saison={season}&verein=&liga=&wettbewerb=&pos=&trainer_id='

updated_url = re.sub(r"(://www\.transfermarkt)\.[a-z.]+", r"\1.com", url)
updated_url = updated_url.replace("profil", "leistungsdatendetails")

seasons_data = []

for season in range(2018, 2025):
    all_data = pd.DataFrame(columns=['Date', 'Stage', 'Opponent_Name', 'Minutes_Played', 'Comp'])
    season_url = updated_url + suffix_url_template.format(season=season)
    soup = parse_html(season_url)
    leagues = extract_leagues_from_url(soup)
    leagues_dict = [{"name": league, "number": index} for index, league in enumerate(leagues)]

    selected_league_names = st.multiselect(
        "Choose leagues for season {}:".format(season),
        options=[league["name"] for league in leagues_dict]
    )

    selected_league_numbers = [
        league["number"] for league in leagues_dict if league["name"] in selected_league_names
    ]

    tables = extract_tables_from_url(soup)

    for i in selected_league_numbers:
        all_data = pd.concat([all_data, merge_tables_leagues(tables, leagues, i)])

    all_data['Date'] = pd.to_datetime(all_data['Date'])
    all_data = all_data.sort_values(by='Date')
    all_data['Minutes_Played'] = pd.to_numeric(all_data['Minutes_Played'].str.replace("'", ""), errors='coerce').fillna(0)

    # Connect to DuckDB and create an in-memory table
    conn = duckdb.connect()
    conn.execute("CREATE TABLE games AS SELECT * FROM all_data")

    # Queries to extract statistics
    games_missed = conn.execute("""
    SELECT COUNT(*) AS games_missed
    FROM games
    WHERE Minutes_Played = 0
    """).fetchall()[0][0]

    total_games = conn.execute("""
    SELECT COUNT(*) AS total_games
    FROM games
    """).fetchall()[0][0]

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
    WHERE Minutes_Played >= 75
    """).fetchall()[0][0]

    total_minutes_played = conn.execute("""
    SELECT SUM(CAST(Minutes_Played AS INTEGER)) FROM games
    """).fetchall()[0][0]

    # Append season data
    seasons_data.append({
        'Season': season,
        'Games Missed': games_missed,
        'Total Games': total_games,
        'Longest Miss Streak': longest_miss_streak,
        'Missed 2+ Games Streaks': missed_2_plus_streaks,
        'Missed 3+ Games Streaks': missed_3_plus_streaks,
        'Longest 75+ Minutes Streak': longest_play_streak_75_plus,
        'Games with 75+ Minutes': games_75_minutes,
        'Total Minutes Played': total_minutes_played
    })

# Convert to DataFrame
seasons_df = pd.DataFrame(seasons_data)

# Display the results
st.table(seasons_df)
