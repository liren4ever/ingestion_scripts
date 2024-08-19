import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import validators

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)



csv_path = '/home/rli/CanadaURLS_20240408_100155.txt'
chunk_size = 1000000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


### process website

# Specify the table and the primary key columns
table_name = "duns_website"
primary_key_columns = ["identifier", "url"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict


with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', sep='|', usecols=['DUNS', 'URL']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk.rename(columns={'DUNS': 'identifier','URL': 'url'}, inplace=True)
        chunk['url'] = chunk['url'].apply(lambda x : x.lower().strip().replace('www.','') if validators.domain(x) else None)
        chunk = chunk[~chunk['url'].isna()]
        chunk['first_time_check'] = '2023-12-20'
        chunk['last_time_check'] = '2023-12-20'
        chunk = chunk[['identifier', 'url', 'first_time_check', 'last_time_check']]

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