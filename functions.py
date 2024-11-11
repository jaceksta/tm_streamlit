import streamlit as st
import requests
import io
from bs4 import BeautifulSoup
import pandas as pd


def parse_html(url):
        # Set headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'
    }
    
    # Send a GET request to fetch the content from the URL with headers
    response = requests.get(url, headers=headers)
    
    # Raise an exception if there's an HTTP error (e.g., 403, 404)
    response.raise_for_status()

    # Parse the content with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    
    return soup

def extract_tables_from_url(soup):


    # Find all divs with class 'responsive-table'
    divs = soup.find_all('div', class_='responsive-table')

    # List to store the dataframes for each table
    tables = []

    # Loop through each div and look for tables inside it
    for div in divs:
        # Extract all tables within the div
        table_elements = div.find_all('table')
        
        for table in table_elements:
            table_html = str(table)
            table_io = io.StringIO(table_html)

            # Use pandas to read HTML table into a dataframe
            table_df = pd.read_html(table_io, header=0)[0]  # [0] to get the first (and only) table in the list
            tables.append(table_df)

    return tables


def extract_leagues_from_url(soup):

    # Find all divs with class 'responsive-table'
    divs = soup.find_all('div', class_='content-box-headline--logo')

    # List to store the dataframes for each table
    elements = []

    # Loop through each div and look for tables inside it
    for div in divs:
        # Extract all tables within the div
        a_elements = div.find_all('a')
        
        for element in a_elements:
            # Use pandas to read HTML table into a dataframe

            elements.append(element.text.strip())

    return elements


def merge_tables_leagues(tables, leagues, i):
    df = tables[i+1]
        # Step 1: Remove rows where the 'Matchday' column contains irrelevant data
    df = df[~df['Matchday'].str.contains("Squad:", na=False)]

    # Step 2: Drop 'Unnamed' columns that are mostly empty or irrelevant, but keep 'Unnamed: 14'
    # Select all columns except those that are 'Unnamed' and not the minutes played column (Unnamed: 14)
    cols_to_keep = ['Matchday', 'Date', 'Venue', 'For', 'For.1', 'Opponent', 'Opponent.1', 'Result', 'Pos.', 'Unnamed: 14']
    df = df[cols_to_keep]

    # Step 3: Rename columns to meaningful names
    df = df.rename(columns={
        'Matchday': 'Stage',
        'Date': 'Date',
        'Venue': 'Venue',
        'For': 'Team_Score',
        'For.1': 'Opponent_Score',
        'Opponent': 'Opponent',
        'Opponent.1': 'Opponent_Name',
        'Result': 'Final_Score',
        'Pos.': 'Position',
        'Unnamed: 14': 'Minutes_Played'  # Renaming Unnamed: 14 to Minutes_Played
    })
    
    df['Comp'] = leagues[i]
    
    df = df[['Date','Stage', 'Opponent_Name', 'Minutes_Played', 'Comp']]
    
    return df