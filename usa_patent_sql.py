import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import pandas as pd
import os
from dateutil.parser import parse
import numpy as np

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

raw_directory = '/home/rli/uspto/'

def convert_to_unified_format(date_string):
    try:
        # Try to parse the date string
        parsed_date = parse(date_string)
        return parsed_date.strftime("%Y-%m-%d")  # Convert to the desired unified format
    except ValueError:
        return None  # If the date string cannot be parsed, return None

### process assignor
csv_file = 'patent_assignor.csv'

csv_path = os.path.join(raw_directory, csv_file)

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

### processing

# Specify the table and the primary key columns
table_name = "patent_assignor"
primary_key_columns = [
    "reel_frame",
    "assignor",
    "recorded_date",
]  # Composite primary key
update_columns = ["update_date", "execution_date"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing assignors") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "reel_no",
                "frame_no",
                "update_date",
                "recorded_date",
                "assignor",
                "execution_date",
            ],
        ),
        desc="Processing assignors",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk['reel_frame'] = chunk.apply(lambda row: str(row['reel_no']) + '-' + str(row['frame_no']), axis=1)
        chunk['update_date'] = chunk['update_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk['recorded_date'] = chunk['recorded_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk['execution_date'] = chunk['execution_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk = chunk[~chunk['assignor'].isna()]
        chunk = chunk[
            [
                "reel_frame",
                "update_date",
                "recorded_date",
                "execution_date",
                "assignor",
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


###
### process assignee
csv_file = 'patent_assignee.csv'

csv_path = os.path.join(raw_directory, csv_file)

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


# Specify the table and the primary key columns
table_name = "patent_assignee"
primary_key_columns = [
    "reel_frame",
    "assignee",
    "recorded_date",
]  # Composite primary key
update_columns = ["update_date"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing assignees") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "reel_no",
                "frame_no",
                "update_date",
                "recorded_date",
                "assignee",
                "address",
                "city",
                "state",
                "postal",
                "country"
            ],
        ),
        desc="Processing assignees",
    ):
        chunk = chunk.copy()
        chunk = chunk[~chunk['assignee'].isna()]
        chunk = chunk[~chunk['recorded_date'].isna()]
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk = chunk.apply(lambda x: ' '.join(str(x).split()) if isinstance(x, str) else x)
        chunk['reel_frame'] = chunk.apply(lambda row: str(row['reel_no']) + '-' + str(row['frame_no']), axis=1)
        chunk['update_date'] = chunk['update_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk['recorded_date'] = chunk['recorded_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk.fillna('', inplace=True)
        chunk = chunk[
            [
                "reel_frame",
                "update_date",
                "recorded_date",
                "assignee",
                "address",
                "city",
                "state",
                "postal",
                "country"
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