# Code for ETL operations on Country-GDP data

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            name_col = col[1]
            name_links = name_col.find_all('a')
            name_link = name_links[1]
            if name_link is not None:
                name = name_link.get('title', '')
                mc_usd_billion = col[2].get_text(strip=True)
                data_dict = {"Name": name, "MC_USD_Billion": mc_usd_billion}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    
    return df


def transform(df):
    # Convert "MC_USD_Billion" to numeric and handle commas
    df["MC_USD_Billion"] = pd.to_numeric(df["MC_USD_Billion"].str.replace(',', ''), errors='coerce')

    # Scale "MC_USD_Billion" by exchange rates and round to 2 decimal places
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * 0.8, 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * 0.93, 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * 82.95, 2)

    return df

query_statement = f"SELECT * from {table_name}"
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement1 = f"SELECT * from {table_name}"
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query_statement3 = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement1, sql_connection)
run_query(query_statement2, sql_connection)
run_query(query_statement3, sql_connection)

log_progress('Process Complete.')

sql_connection.close()