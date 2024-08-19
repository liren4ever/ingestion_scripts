import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import os
from datetime import datetime
import sys
import numpy as np

today = datetime.today().strftime('%Y-%m-%d')

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)


raw_directory = '/home/rli/'

input_file_name = sys.argv[1]


csv_path = os.path.join(raw_directory, input_file_name)

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

### address processing

# Specify the table and the primary key columns
table_name = "consolidated_match"
primary_key_columns = ['identifier_x', 'identifier_y', 'version']  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                'identifier_x', 'identifier_y', 'name_score', 'address_score', 'city_score', 'postcode_score', 'meta_score'
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.where(pd.notnull(chunk), None)

        chunk['version'] = 'dl_v1'        
        
        chunk['first_time_check'] = '2024-08-14'
        chunk["last_time_check"] = '2024-08-14'
        chunk = chunk[
            [
                "identifier_x",
                "identifier_y",
                "name_score",
                "address_score",
                "city_score",
                "postcode_score",
                "meta_score",
                "version",
                "first_time_check",
                "last_time_check",
            ]
        ]
        chunk.drop_duplicates(inplace=True)

        # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ", ".join([f":{col}" for col in chunk.columns])

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient="records"))

        pbar.update()