import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
from datetime import datetime
import re


today = datetime.today().strftime('%Y-%m-%d')

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

csv_path = '/home/rli/dnb_data/dnb.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

###loading name

# Specify the table and the primary key columns
table_name = "duns_name"
primary_key_columns = ["identifier", "business_name"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'name', 'first_time_check']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[~chunk['name'].isna()]
        chunk['name_type'] = 'main'
        chunk['last_time_check'] = today
        chunk.rename(columns={'name': 'business_name', 'uuid':'identifier'}, inplace=True)
        chunk = chunk[['identifier', 'business_name', 'name_type', 'first_time_check', 'last_time_check']]

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

### loading alt_name

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'alt_name', 'first_time_check']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[~chunk['alt_name'].isna()]
        chunk['name_type'] = 'dba'
        chunk.rename(columns={'alt_name': 'business_name', 'uuid':'identifier'}, inplace=True)
        chunk['last_time_check'] = today
        chunk = chunk[['identifier', 'business_name', 'name_type', 'first_time_check', 'last_time_check']]

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


### loading location
# Specify the table and the primary key columns
table_name = "duns_location"
primary_key_columns = [
    "identifier",
    "address",
    "city",
    "state",
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

# Define the regex patterns
usa_pattern = r"^\d{5}(-\d{4})?$"
canada_pattern = r"^[A-Za-z]\d[A-Za-z]\d[A-Za-z]\d$"

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "uuid",
                "address",
                "city",
                "state",
                "postal",
                "country",
                "first_time_check",
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk.fillna("", inplace=True)
        chunk["postal"] = chunk["postal"].apply(lambda x: str(x).replace(" ", ""))
        chunk["postal"] = chunk.apply(
            lambda row: row["postal"].replace(row["state"], ""), axis=1
        )
        chunk["postal"] = chunk["postal"].apply(lambda x: x[0:6])
        chunk.loc[chunk["country"] == "USA", "postal"] = chunk.loc[
            chunk["country"] == "USA", "postal"
        ].apply(lambda x: x if bool(re.match(usa_pattern, x)) else "")
        chunk.loc[chunk["country"] == "CAN", "postal"] = chunk.loc[
            chunk["country"] == "CAN", "postal"
        ].apply(lambda x: x if bool(re.match(canada_pattern, x)) else "")
        chunk["state"] = chunk["state"].apply(lambda x: x.upper()[0:2])
        chunk["last_time_check"] = today
        chunk.rename(columns={'uuid':'identifier'}, inplace=True)
        chunk = chunk[
            [
                "identifier",
                "address",
                "city",
                "state",
                "postal",
                "country",
                "first_time_check",
                "last_time_check",
            ]
        ]

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


### loading phone

# Specify the table and the primary key columns
table_name = "duns_phone"
primary_key_columns = ["identifier", "phone"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'phone', 'first_time_check']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[chunk['phone'].str.contains(r'\d', na=False)]
        chunk['phone'] = chunk['phone'].str.replace(r'\D', '', regex=True)
        chunk['last_time_check'] = today
        chunk.rename(columns={'uuid':'identifier'}, inplace=True)
        chunk = chunk[['identifier', 'phone', 'first_time_check', 'last_time_check']]

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


### loading identifier

# Specify the table and the primary key columns
table_name = 'duns_identifier'
primary_key_columns = ['identifier', 'identifier_hq']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'uuid_hq', 'legal_type', 'first_time_check']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[~chunk['identifier_hq'].isna()]
        chunk['last_time_check'] = today
        chunk.rename(columns={'uuid':'identifier', 'uuid_hq':'identifier_hq'}, inplace=True)
        chunk['status'] = 'Active'
        chunk = chunk[['first_time_check', 'last_time_check', 'identifier', 'identifier_hq', 'legal_type', 'status']]

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


### loading identifier mapping

# Specify the table and the primary key columns
table_name = 'consolidated_identifier_mapping'
primary_key_columns = ['identifier', 'raw_id', 'raw_authority']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['identifier', 'uuid']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[~chunk['identifier'].isna()]
        chunk.rename(columns={'identifier':'raw_id'}, inplace=True)
        chunk.rename(columns={'uuid':'identifier'}, inplace=True)
        chunk['raw_authority'] = 'DNB'

    # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join([f":{col}" for col in chunk.columns])  # Correct placeholders

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO NOTHING
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient='records'))

        pbar.update()


with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['identifier', 'identifier_hq', 'uuid_hq']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[~chunk['identifier_hq'].isna()]
        chunk = chunk[chunk['identifier'] != chunk['identifier_hq']]
        chunk = chunk[['identifier_hq', 'uuid_hq']]
        chunk.rename(columns={'identifier_hq':'raw_id'}, inplace=True)
        chunk.rename(columns={'uuid_hq':'identifier'}, inplace=True)
        chunk['raw_authority'] = 'DNB'

    # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join([f":{col}" for col in chunk.columns])  # Correct placeholders

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO NOTHING
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient='records'))

        pbar.update()