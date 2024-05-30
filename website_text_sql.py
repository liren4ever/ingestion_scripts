import pandas as pd
import psycopg2
from psycopg2 import sql
from tqdm import tqdm
from datetime import datetime

# Get today's date
today = datetime.today()

# Format the date as YYYY-MM-DD
formatted_date = today.strftime("%Y-%m-%d")

# Database connection parameters
db_params = {
    'dbname': 'rel8ed',
    'user': 'postgres',
    'password': 'rel8edpg',
    'host': '10.8.0.110',
    'port': '5432'
}

# Function to upsert a record
def upsert_record(cur, _url, _trafilatura, _lang, _p_lang, _first_time_check, _last_time_check):
    # SQL query to insert a new record
    query = sql.SQL("""
        INSERT INTO website_text (url, trafilatura, lang, p_lang, first_time_check, last_time_check)
        VALUES (
            %s,
            %s,
            %s,
            %s,
            COALESCE(
                (SELECT first_time_check FROM website_text WHERE url = %s), 
                %s
            ),
            %s
        )
    """)

    # Execute the query with provided parameters
    cur.execute(query, (_url, _trafilatura, _lang, _p_lang, _url, _first_time_check, _last_time_check))

# Example DataFrame
df = pd.read_parquet('/home/rli/scraped_websites.parquet')

try:
    # Establishing the connection
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Iterate over the DataFrame and upsert each record with progress bar
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Inserting records"):
        upsert_record(cur, str(row['url']), str(row['trafilatura']), str(row['lang']), str(row['p_lang']), formatted_date, formatted_date)

    # Commit the transaction
    conn.commit()

    print("All records inserted successfully")

except Exception as error:
    print("Error while inserting records:", error)

finally:
    if conn:
        cur.close()
        conn.close()
