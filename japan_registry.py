import zipfile
import os
import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
from uuid import uuid5, UUID
from datetime import datetime

today = datetime.today().strftime('%Y-%m-%d')

def identifier_uuid(text):
    namespace = UUID("00000000-0000-0000-0000-000000000000")
    uuid = uuid5(namespace, text)
    return uuid

directory = "/var/rel8ed.to/nfs/share/japan"
zip_file = os.listdir(directory)
zip_file = [f for f in zip_file if f.endswith('.zip')][0]

def unzip_file(directory):
    with zipfile.ZipFile(os.path.join(directory, zip_file), "r") as zip_ref:
        zip_ref.extractall(directory)

unzip_file(directory)

file = os.listdir(directory)
file = [f for f in file if f.endswith('.csv')][0]

file_path = os.path.join(directory, file)

df = pd.read_csv(file_path, dtype=str, encoding='utf-8', header=None)

cols = [1, 5, 6, 9, 10, 11, 28]
df = df[cols]
df.columns = ["identifier", "date", "business_name", "state", "city", "address", "alt_business_name"]

csv_path = os.path.join(directory, "japan_registry.csv")

df.to_csv(csv_path, index=False)

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

# Specify the table and the primary key columns
table_name = "registry_name"
primary_key_columns = [
    "identifier",
    "business_name",
]  # Composite primary key
update_columns = ["last_time_check"] 

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str"
        ),
        desc="Processing name chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk['identifier'] = chunk['identifier'].apply(lambda x : str(identifier_uuid(x+'_JAPAN')))
        chunk['first_time_check'] = today
        chunk['last_time_check'] = today
        chunk['start_date'] = None
        chunk['end_date'] = None
        chunk['name_type'] = 'main'

        
        chunk = chunk[
            [
                "business_name",
                "name_type",
                "start_date",
                "end_date",
                "first_time_check",
                "last_time_check",
                "identifier"
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


with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=["alt_business_name", "identifier"]
        ),
        desc="Processing name chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk = chunk[chunk['alt_business_name'].notna()]
        chunk.rename(columns={"alt_business_name":"business_name"}, inplace=True)
        chunk['identifier'] = chunk['identifier'].apply(lambda x : str(identifier_uuid(x+'_JAPAN')))
        chunk['first_time_check'] = today
        chunk['last_time_check'] = today
        chunk['start_date'] = None
        chunk['end_date'] = None
        chunk['name_type'] = 'dba'

        
        chunk = chunk[
            [
                "business_name",
                "name_type",
                "start_date",
                "end_date",
                "first_time_check",
                "last_time_check",
                "identifier"
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



# Specify the table and the primary key columns
table_name = "registry_location"
primary_key_columns = [
    "identifier",
    "address",
    "city",
    "state",
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict


with tqdm(total=total_chunks, desc="Processing location chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
        ),
        desc="Processing location chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk['identifier'] = chunk['identifier'].apply(lambda x : str(identifier_uuid(x+'_JAPAN')))
        chunk['country'] = 'JPN'
        chunk['first_time_check'] = today
        chunk['last_time_check'] = today
        chunk['latitude'] = None
        chunk['longitude'] = None
        chunk['postal'] = None
        chunk['location_type'] = None
        chunk = chunk[
            [
                "identifier",
                "address",
                "city",
                "state",
                "postal",
                "country",
                "longitude",
                "latitude",
                "location_type",
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

### activity processing

# Specify the table and the primary key columns
table_name = "registry_activity"
primary_key_columns = [
    "identifier",
    "activity_name",
    "activity_date",
]  # Composite primary key
update_columns = ["last_time_check"]

with tqdm(total=total_chunks, desc="Processing activity chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
        ),
        desc="Processing activity chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'date':'activity_date'}, inplace=True)
        chunk['registry_url'] = chunk['identifier'].apply(lambda x : 'https://www.houjin-bangou.nta.go.jp/henkorireki-johoto.html?selHouzinNo='+x)
        chunk['identifier'] = chunk['identifier'].apply(lambda x : str(identifier_uuid(x+'_JAPAN')))
        chunk['authority'] = 'National Tax Agency Japan'
        chunk['activity_name'] = 'Incorporation'
        chunk['first_time_check'] = today
        chunk['last_time_check'] = today

        chunk = chunk[
            [
                "authority",
                "activity_name",
                "activity_date",
                "first_time_check",
                "last_time_check",
                "identifier",
                "registry_url"
            ]
        ]
        chunk.drop_duplicates(inplace=True)

        # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ", ".join([f":{col}" for col in chunk.columns])

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
        {', '.join([f"{col} = excluded.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient="records"))

        pbar.update()


## process identifier mapping

##Specify the table and the primary key columns
table_name = "consolidated_identifier_mapping"
primary_key_columns = ['identifier', 'raw_id', 'raw_authority']  # Composite primary key


with tqdm(total=total_chunks, desc="Processing identifier mapping chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing identifier mapping chunks"):
        chunk = chunk.copy()
        chunk = chunk[chunk['identifier'].notna()]
        chunk['uuid'] = chunk['identifier'].apply(lambda x : str(identifier_uuid(x+'_JAPAN')))
        chunk.rename(columns={'identifier':'raw_id', 'uuid':'identifier'}, inplace=True)
        chunk['raw_authority'] = 'National Tax Agency Japan'
        chunk = chunk[['identifier', 'raw_id', 'raw_authority']]
        chunk.drop_duplicates(inplace=True)

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