import streamlit as st
import duckdb
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from functions import extract_leagues_from_url, extract_tables_from_url, merge_tables_leagues

def get_player_stats(url):
    suffix_url = '/plus/0?saison=2024&verein=&liga=&wettbewerb=&pos=&trainer_id='
    updated_url = re.sub(r"(://www\.transfermarkt)\.[a-z.]+", r"\1.com", url)
    updated_url = updated_url.replace("profil", "leistungsdatendetails")
    updated_url = updated_url + suffix_url

    leagues = extract_leagues_from_url(updated_url)
    leagues_dict = [{"name": league, "number": index} for index, league in enumerate(leagues)]

    selected_league_numbers = get_selected_leagues(leagues_dict)

    try:
        tables = extract_tables_from_url(updated_url)
        all_data = pd.DataFrame(columns=['Date', 'Stage', 'Opponent_Name', 'Minutes_Played', 'Comp'])

        for i in selected_league_numbers:
            all_data = pd.concat([all_data, merge_tables_leagues(tables, leagues, i)])

        all_data['Date'] = pd.to_datetime(all_data['Date'])
        all_data = all_data.sort_values(by='Date')

        return all_data

    except (requests.exceptions.HTTPError, Exception) as e:
        print(f"An error occurred: {e}")
        return None

def get_selected_leagues(leagues_dict):
    selected_league_names = st.multiselect(
        "Choose leagues:",
        options=[league["name"] for league in leagues_dict]
    )
    return [league["number"] for league in leagues_dict if league["name"] in selected_league_names]

def calculate_player_stats(df):
    df['Minutes_Played'] = pd.to_numeric(df['Minutes_Played'].str.replace("'", ""), errors='coerce').fillna(0)

    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE games AS SELECT * FROM df")

        games_missed = conn.execute("SELECT COUNT(*) AS games_missed FROM games WHERE Minutes_Played = 0").fetchall()[0][0]
        total_games = conn.execute("SELECT COUNT(*) AS total_games FROM games").fetchall()[0][0]
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

        games_75_minutes = conn.execute("SELECT COUNT(*) AS games_75_minutes FROM games WHERE Minutes_Played >= 75").fetchall()[0][0]
        total_minutes_played = conn.execute("SELECT SUM(CAST(Minutes_Played AS INTEGER)) AS total_minutes_played FROM games").fetchall()[0][0]

    return {
        "games_missed": games_missed,
        "total_games": total_games,
        "missed_2_plus_streaks": missed_2_plus_streaks,
        "missed_3_plus_streaks": missed_3_plus_streaks,
        "longest_miss_streak": longest_miss_streak,
        "longest_play_streak_75_plus": longest_play_streak_75_plus,
        "games_75_minutes": games_75_minutes,
        "total_minutes_played": total_minutes_played
    }

def main():
    url = st.text_input("Enter Transfermarkt URL", "https://www.transfermarkt.pl/igor-sapala/profil/spieler/380597")
    df = get_player_stats(url)

    if df is not None:
        stats = calculate_player_stats(df)

        tab1, tab2 = st.tabs(['Stats', 'Games'])
        with tab1:
            st.write("Total games missed:", stats["games_missed"])
            st.write("Total games possible:", stats["total_games"])
            st.write("Times missed 2+ games in a row:", stats["missed_2_plus_streaks"])
            st.write("Times missed 3+ games in a row:", stats["missed_3_plus_streaks"])
            st.write("Longest streak of games missed:", stats["longest_miss_streak"])
            st.write("Longest streak of games with 75+ minutes played:", stats["longest_play_streak_75_plus"])
            st.write("Games with more than 75 minutes:", stats["games_75_minutes"])
            st.write("Total Minutes Played:", stats["total_minutes_played"])
        with tab2:
            st.table(df)

if __name__ == "__main__":
    main()