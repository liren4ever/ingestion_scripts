import os
import pandas as pd
from datetime import datetime
from uuid import uuid5, UUID
from cleanco import typesources, matches
import tarfile
from sqlalchemy import create_engine, text
import re
from tqdm import tqdm
import psycopg2

classification_sources = typesources()

today = datetime.today().strftime('%Y-%m-%d')

# Function to untar files
def untar_file(tar_path, extract_path):
    with tarfile.open(tar_path, 'r') as tar:
        tar.extractall(path=extract_path)
        print(f"Extracted {tar_path} to {extract_path}")

# Function to generate UUID
def identifier_uuid(text):
    namespace = UUID("00000000-0000-0000-0000-000000000000")
    uuid = uuid5(namespace, text)
    return uuid

# Function to process files
def process_file(file_path):
    # Read the file
    with open(file_path, 'r') as file:
        content = file.read()

    # Modify the content
    modified_content = content.replace('\\,', '').replace('\\"', '').replace('\\', '').replace('"', ' ').replace(',',' ')

    # Write the modified content back to the file
    with open(file_path, 'w') as file:
        file.write(modified_content)

# Directory paths
data_dir = '/var/rel8ed.to/nfs/share/duns/extracted/'
extract_dir = '/var/rel8ed.to/nfs/share/duns/extracted/'

# Ensure the extract directory exists
os.makedirs(extract_dir, exist_ok=True)

# List and untar files
files = os.listdir(data_dir)
tar_files = [f for f in files if f.endswith('.gz')]

for tar_file in tar_files:
    tar_path = os.path.join(data_dir, tar_file)
    untar_file(tar_path, extract_dir)

files = os.listdir(extract_dir)
length = len(files)

fl_files = [fl for fl in files if fl.endswith('FL')]
for fl in fl_files:
    file_path = os.path.join(extract_dir, fl)
    process_file(file_path)
    print(file_path)

header = True
# Read the CSV file
for file in fl_files:
    file_path = os.path.join(extract_dir, file)
    print(file)
    data = pd.read_csv(file_path, header=None, sep='|', dtype='str', encoding='utf-8')
    data.columns = ['identifier', 'name', 'address', 'city', 'state', 'postal', 'alt_name', 'country', 'phone', 'location_status', 'identifier_hq']
    data['uuid'] = data['identifier'].apply(lambda x: identifier_uuid(x+'DNB'))
    data['uuid_hq'] = data['identifier_hq'].apply(lambda x: identifier_uuid(x+'DNB'))
    data['legal_type'] = data['name'].apply(lambda x : matches(str(x), classification_sources)[0] if matches(str(x), classification_sources) != [] else '')
    data['first_time_check'] = today
    data.to_csv('/var/rel8ed.to/nfs/share/duns/extracted/dnb.csv', index=False, mode='a', header=header)
    header = False
    length -= 1
    print(length, 'files remaining')
    os.remove(file_path)

print("CSV file created")

####

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

csv_path = '/var/rel8ed.to/nfs/share/duns/extracted/dnb.csv'
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
        chunk['last_time_check'] = chunk['first_time_check']
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
        chunk['last_time_check'] = chunk['first_time_check']
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


### loading name to consolidated_name

table_name = "consolidated_name"
primary_key_columns = ["identifier", "business_name"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'name', 'first_time_check']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[~chunk['name'].isna()]
        chunk['name_type'] = 'main'
        chunk['last_time_check'] = chunk['first_time_check']
        chunk['start_date'] = None
        chunk['end_date'] = None
        chunk.rename(columns={'name': 'business_name', 'uuid':'identifier'}, inplace=True)
        chunk = chunk[['business_name', 'name_type', 'start_date', 'end_date', 'first_time_check', 'last_time_check', 'identifier']]

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

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'alt_name', 'first_time_check']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[~chunk['alt_name'].isna()]
        chunk['name_type'] = 'dba'
        chunk.rename(columns={'alt_name': 'business_name', 'uuid':'identifier'}, inplace=True)
        chunk['last_time_check'] = chunk['first_time_check']
        chunk['start_date'] = None
        chunk['end_date'] = None
        chunk = chunk[['business_name', 'name_type', 'start_date', 'end_date', 'first_time_check', 'last_time_check', 'identifier']]

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
        chunk['last_time_check'] = chunk['first_time_check']
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

### loading consolidated location
# Specify the table and the primary key columns
table_name = "consolidated_location"
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
        chunk['last_time_check'] = chunk['first_time_check']
        chunk.rename(columns={'uuid':'identifier'}, inplace=True)
        chunk['latitude'] = None
        chunk['longitude'] = None
        chunk['location_type'] = 'main'
        chunk['location_status'] = None
        chunk = chunk[
            [
                "address",
                "city",
                "state",
                "postal",
                "country",
                "latitude",
                "longitude",
                "location_type",
                "location_status",
                "first_time_check",
                "last_time_check",
                "identifier",
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
table_name = "consolidated_phone"
primary_key_columns = ["identifier", "phone"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'phone', 'first_time_check']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[chunk['phone'].str.contains(r'\d', na=False)]
        chunk['phone'] = chunk['phone'].str.replace(r'\D', '', regex=True)
        chunk['phone_type'] = 'work'
        chunk['last_time_check'] = chunk['first_time_check']
        chunk.rename(columns={'uuid':'identifier'}, inplace=True)
        chunk = chunk[['identifier', 'phone', 'phone_type', 'first_time_check', 'last_time_check']]

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
        chunk = chunk[~chunk['uuid_hq'].isna()]
        chunk['last_time_check'] = chunk['first_time_check']
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


### update cosolidated tables with new dnb data

# Establish a connection to your PostgreSQL database
conn = psycopg2.connect(
    host="10.8.0.110",
    dbname="rel8ed",
    user="postgres",
    password="rel8edpg"
)

# Create a cursor to perform database operations
cur = conn.cursor()

# Define the SQL queries
sql_queries = [
    """
    -- First query: get the latest last_time_check
    WITH check_date AS (
        SELECT last_time_check
        FROM duns_identifier
        ORDER BY last_time_check DESC
        LIMIT 1
    )
    UPDATE duns_identifier
    SET status = 'Inactive'
    WHERE last_time_check != (SELECT last_time_check FROM check_date);
    """,
    """
    -- Second query: update location
    insert into consolidated_location
    SELECT address, city, state, postal, country, null as latitude, null as longitude, '' as location_type, '' as location_status, first_time_check, last_time_check, identifier 
    FROM duns_location
    on conflict do nothing;
    """,
    """
    -- Third query: update name
    insert into consolidated_name
    select business_name, name_type, null as start_date, null as end_date, first_time_check, last_time_check, identifier 
    from duns_name
    on conflict do nothing; 
    """,
    """
    -- Fourth query: hierarchy
    insert into consolidated_identifier_hierarchy
    select identifier, identifier_hq, first_time_check, last_time_check 
    from duns_identifier
    where identifier != identifier_hq
    on conflict do nothing;
    """,
    """
    -- Fourth query: status
    INSERT INTO consolidated_status (identifier, status, first_time_check, last_time_check)
    SELECT identifier, status, first_time_check, last_time_check 
    FROM duns_identifier
    ON CONFLICT (identifier) 
    DO UPDATE SET 
        status = EXCLUDED.status, 
        last_time_check = EXCLUDED.last_time_check;

    """,
    """
    -- Fourth query: legal type
    insert into consolidated_legal_type
    select identifier, legal_type
    from duns_identifier 
    where legal_type != null or legal_type != '' or legal_type = 'NaN'
    on conflict do nothing;
    """,
]

try:
    # Execute each SQL query in the list
    for query in sql_queries:
        cur.execute(query)
    
    # Commit the transaction
    conn.commit()
    print("All queries executed successfully.")
    
except Exception as e:
    # Rollback in case of error
    conn.rollback()
    print(f"An error occurred: {e}")
    
finally:
    # Close the cursor and the connection
    cur.close()
    conn.close()
