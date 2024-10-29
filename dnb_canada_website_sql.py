import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import validators
from datetime import datetime
from uuid import uuid5, UUID

today = datetime.today().strftime('%Y-%m-%d')

# Function to generate UUID
def identifier_uuid(text):
    namespace = UUID("00000000-0000-0000-0000-000000000000")
    uuid = uuid5(namespace, text)
    return uuid

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

csv_path =  '/var/rel8ed.to/nfs/share/duns/can202410/CanadaURLS_20241024_090001.txt'
chunk_size = 10

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


### process website

# Specify the table and the primary key columns
table_name = "consolidated_website"
primary_key_columns = ["identifier", "url"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict


with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', sep='|', usecols=['DUNS', 'URL']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk.rename(columns={'DUNS': 'identifier','URL': 'url'}, inplace=True)
        chunk['identifier'] = chunk['identifier'].apply(lambda x: str(identifier_uuid(x+'DNB')))
        chunk['url'] = chunk['url'].apply(lambda x : x.lower().strip().replace('www.','') if validators.domain(x) else None)
        chunk = chunk[chunk['url'].notna()]
        chunk['first_time_check'] = today
        chunk['last_time_check'] = today
        chunk = chunk[['url', 'first_time_check', 'last_time_check', 'identifier']]

    # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join([f":{col}" for col in chunk.columns])  # Correct placeholders
        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient='records'))

        pbar.update()