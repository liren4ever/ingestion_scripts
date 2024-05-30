import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

# Specify the table and the primary key columns
table_name = 'duns_identifier'
primary_key_columns = ['identifier', 'identifier_hq', 'authority', 'hq_authority']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

csv_path = '/home/rli/dnb_data/dnb.csv'
chunk_size = 1000000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, skiprows=lambda x: 1 <= x <= 40697100, chunksize=chunk_size, dtype='str', usecols=['identifier', 'identifier_hq', 'first_time_check']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[~chunk['identifier_hq'].isna()]
        chunk['authority'] = 'DNB'
        chunk['hq_authority'] = 'DNB'
        chunk['last_time_check'] = '2023-12-20'
        chunk = chunk[['identifier', 'identifier_hq', 'authority', 'hq_authority', 'first_time_check', 'last_time_check']]

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
