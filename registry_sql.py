#!/home/rli/venv/bin/python3
import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import os
import re
import sys
import string
import numpy as np

translator = str.maketrans('', '', string.punctuation)

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)


raw_directory = '/var/rel8ed.to/nfs/share/project_originals/files/files_cooked/'

input_file_name = sys.argv[1]

def column_exists(column_name, csv_path):
    try:
        with open(csv_path, 'r') as f:
            first_line = f.readline()
            column_names = first_line.strip().split(',')
            return column_name in column_names
    except FileNotFoundError:
        print(f"File not found: {csv_path}")
        return False

csv_path = os.path.join(raw_directory, input_file_name+'.csv')
file_date = input_file_name.split('_')[-1].split('.')[0]

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


### checking columns in csv file

df = pd.read_csv(csv_path, nrows=0)
available_columns = df.columns

desired_columns = ['first_time_check', 'last_time_check']  # Replace with your column names
valid_columns = [col for col in desired_columns if col in available_columns]

### address processing

# Specify the table and the primary key columns
table_name = "consolidated_location"
primary_key_columns = [
    "identifier",
    "address",
    "city",
    "state",
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict


used_columns = ["uuid", "address_en", "city", "region_code", "postal_code", "country_code", "lat", "lon", "address_type"]

used_columns += valid_columns
# Define the regex patterns
usa_pattern = r"^\d{5}(-\d{4})?$"
canada_pattern = r"^[A-Za-z]\d[A-Za-z]\d[A-Za-z]\d$"

with tqdm(total=total_chunks, desc="Processing location chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns,
        ),
        desc="Processing location chunks",
    ):
        # Add missing columns and fill with a default value if they don't exist
        for col in desired_columns:
            if col not in chunk.columns:
                chunk[col] = ""  # Initialize with empty strings or NaN
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.fillna('', inplace=True)
        chunk.rename(columns={'uuid':'identifier', 'address_en': 'address', 'region_code': 'state', 'postal_code': 'postal', 'country_code': 'country', "lon":"longitude", "lat":"latitude", "address_type":"location_type"}, inplace=True)
        
        chunk['city'] = chunk['city'].apply(lambda x: str(x).lower())
        chunk["postal"] = chunk["postal"].apply(lambda x: str(x).replace(" ", ""))
        chunk["postal"] = chunk.apply(
            lambda row: row["postal"].replace(row["state"], ""), axis=1
        )
        chunk.loc[chunk["country"] == "USA", "postal"] = chunk.loc[
            chunk["country"] == "USA", "postal"
        ].apply(lambda x: x if bool(re.match(usa_pattern, x)) else "")
        chunk.loc[chunk["country"] == "CAN", "postal"] = chunk.loc[
            chunk["country"] == "CAN", "postal"
        ].apply(lambda x: x[0:6])
        chunk.loc[chunk["country"] == "CAN", "postal"] = chunk.loc[
            chunk["country"] == "CAN", "postal"
        ].apply(lambda x: x if bool(re.match(canada_pattern, x)) else "")
        chunk["state"] = chunk["state"].apply(lambda x: x.upper()[0:2])
        chunk.loc[chunk["latitude"] == "", "latitude"] = None
        chunk.loc[chunk["longitude"] == "", "longitude"] = None
        chunk.loc[chunk['first_time_check'] == "", 'first_time_check'] = file_date
        chunk.loc[chunk['last_time_check'] == "", 'last_time_check'] = file_date
        chunk['location_status'] = None
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
                "location_status",
                "first_time_check",
                "last_time_check",
            ]
        ]
        chunk = chunk[chunk['location_type']!='officer']
        chunk.loc[:, ['postal', 'country', 'longitude', 'latitude', 'location_type', 'location_status']] = chunk[['postal', 'country', 'longitude', 'latitude', 'location_type', 'location_status']].replace({pd.NA: None, np.nan: None, '': None})
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


### alt_address processing

used_columns = ["uuid", "alt_address_en", "alt_city", "alt_region_code", "alt_postal_code", "alt_country_code", "alt_address_type"]

used_columns += valid_columns

with tqdm(total=total_chunks, desc="Processing location chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns,
        ),
        desc="Processing location chunks",
    ):
        # Add missing columns and fill with a default value if they don't exist
        for col in desired_columns:
            if col not in chunk.columns:
                chunk[col] = ""  # Initialize with empty strings or NaN
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.fillna("", inplace=True)
        chunk.rename(columns={'uuid':'identifier', 'alt_address_en': 'address', 'alt_city': 'city', 'alt_region_code': 'state', 'alt_postal_code': 'postal', 'alt_country_code': 'country', "alt_address_type":"location_type"}, inplace=True)
        chunk = chunk[chunk['address'] != ""]
        chunk = chunk[chunk['country'] != ""]
        chunk['city'] = chunk['city'].apply(lambda x: str(x).lower())
        chunk["postal"] = chunk["postal"].apply(lambda x: str(x).replace(" ", ""))
        chunk.loc[chunk["country"] == "USA", "postal"] = chunk.loc[
            chunk["country"] == "USA", "postal"
        ].apply(lambda x: x if bool(re.match(usa_pattern, x)) else "")
        chunk.loc[chunk["country"] == "CAN", "postal"] = chunk.loc[
            chunk["country"] == "CAN", "postal"
        ].apply(lambda x: x[0:6])
        chunk.loc[chunk["country"] == "CAN", "postal"] = chunk.loc[
            chunk["country"] == "CAN", "postal"
        ].apply(lambda x: x if bool(re.match(canada_pattern, x)) else "")
        chunk["state"] = chunk["state"].apply(lambda x: x.upper()[0:2])
        chunk["latitude"] = None
        chunk["longitude"] = None
        chunk['location_type'] = chunk['location_type'].apply(lambda x: x.split(' ')[0].lower())
        chunk.loc[chunk['first_time_check'] == "", 'first_time_check'] = file_date
        chunk.loc[chunk['last_time_check'] == "", 'last_time_check'] = file_date
        chunk['location_status'] = None
        chunk = chunk[
            [
                "identifier",
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
            ]
        ]
        chunk = chunk[chunk['location_type']!='officer']
        chunk.loc[:, ['postal', 'country', 'longitude', 'latitude', 'location_type', 'location_status']] = chunk[['postal', 'country', 'longitude', 'latitude', 'location_type', 'location_status']].replace({pd.NA: None, np.nan: None, '': None})
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

### name processing

# Specify the table and the primary key columns
table_name = "consolidated_name"
primary_key_columns = [
    "identifier",
    "business_name",
]  # Composite primary key
update_columns = ["last_time_check","business_name_en"] 

used_columns = ["uuid", "business_name", "business_name_type", "business_name_en", "name_start_date", "name_end_date"]

used_columns += valid_columns # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns,
        ),
        desc="Processing name chunks",
    ):
        # Add missing columns and fill with a default value if they don't exist
        for col in desired_columns:
            if col not in chunk.columns:
                chunk[col] = ""  # Initialize with empty strings or NaN
        chunk = chunk.copy()
        chunk.fillna('', inplace=True)
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

        chunk = chunk[chunk['business_name_en'] != ""]

        chunk.rename(columns={'uuid':'identifier', 'business_name_type': 'name_type', 'name_start_date':'start_date', 'name_end_date':'end_date'}, inplace=True)
        chunk.loc[chunk['first_time_check'] == "", 'first_time_check'] = file_date
        chunk.loc[chunk['last_time_check'] == "", 'last_time_check'] = file_date
        # Additional processing here
        chunk.replace('', None, inplace=True)  # Convert empty strings back to NaN
        chunk = chunk[
            [
                "identifier",
                "business_name",
                "business_name_en",
                "name_type",
                "start_date",
                "end_date",
                "first_time_check",
                "last_time_check",
            ]
        ]
        chunk = chunk[chunk['business_name'].str.len() <= 500]
        chunk.loc[:, ['name_type', 'start_date', 'end_date']] = chunk[['name_type', 'start_date', 'end_date']].replace({pd.NA: None, np.nan: None, '': None})
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

### alt_name processing

used_columns = ["uuid", "alt_business_names", "alt_business_names_type", "alt_business_names_en"]

used_columns += valid_columns

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns
        ),
        desc="Processing name chunks",
    ):
        # Add missing columns and fill with a default value if they don't exist
        for col in desired_columns:
            if col not in chunk.columns:
                chunk[col] = ""  # Initialize with empty strings or NaN
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.fillna("", inplace=True)
        chunk = chunk[chunk['alt_business_names_en'] != ""]
        chunk.rename(columns={'uuid':'identifier','alt_business_names':'business_name', 'alt_business_names_en':'business_name_en', 'alt_business_names_type': 'name_type'}, inplace=True)
        chunk['start_date'] = None
        chunk['end_date'] = None
        chunk.loc[chunk['first_time_check'] == "", 'first_time_check'] = file_date
        chunk.loc[chunk['last_time_check'] == "", 'last_time_check'] = file_date
        chunk = chunk[
            [
                "identifier",
                "business_name",
                "business_name_en",
                "name_type",
                "start_date",
                "end_date",
                "first_time_check",
                "last_time_check",
            ]
        ]
        chunk = chunk[chunk['business_name'].str.len() <= 500]
        chunk.loc[:, ['name_type', 'start_date', 'end_date']] = chunk[['name_type', 'start_date', 'end_date']].replace({pd.NA: None, np.nan: None, '': None})
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

### identifier processing

# Specify the table and the primary key columns
table_name = "consolidated_legal_type"
primary_key_columns = [
    "identifier",
]  # Composite primary key
update_columns = ["legal_type"]  # Columns to update in case of conflict

used_columns = ["uuid", "legal_type"]

with tqdm(total=total_chunks, desc="Processing legal type chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns,
        ),
        desc="Processing legal type chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk = chunk[chunk['legal_type'].notna()]
        chunk.rename(columns={'uuid':'identifier'}, inplace=True)
        chunk = chunk[
            [
                "identifier",
                "legal_type",
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


# Specify the table and the primary key columns
table_name = "consolidated_status"
primary_key_columns = [
    "identifier",
    "status"
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

used_columns = ["uuid", "registry_status"]

used_columns += valid_columns  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing status chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns,
        ),
        desc="Processing status chunks",
    ):
        # Add missing columns and fill with a default value if they don't exist
        for col in desired_columns:
            if col not in chunk.columns:
                chunk[col] = ""  # Initialize with empty strings or NaN
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk = chunk[chunk['registry_status'].notna()]
        chunk.rename(columns={'uuid':'identifier', 'registry_status':'status'}, inplace=True)
        chunk.loc[chunk['first_time_check'] == "", 'first_time_check'] = file_date
        chunk.loc[chunk['last_time_check'] == "", 'last_time_check'] = file_date
        chunk = chunk[
            [
                "identifier",
                "status",
                "first_time_check",
                "last_time_check"
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
        
### activity processing

# Specify the table and the primary key columns
table_name = "registry_activity"
primary_key_columns = [
    "identifier",
    "activity_name",
    "activity_date",
]  # Composite primary key
update_columns = ["last_time_check"]

used_columns = ["uuid", "authority", "creation_year", "Category", "registry_url"]

used_columns += valid_columns  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing activity chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns,
        ),
        desc="Processing activity chunks",
    ):
        # Add missing columns and fill with a default value if they don't exist
        for col in desired_columns:
            if col not in chunk.columns:
                chunk[col] = ""  # Initialize with empty strings or NaN
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'uuid':'identifier', 'Category':'activity_name', 'creation_year':'activity_date'}, inplace=True)
        chunk = chunk[chunk['activity_date'].notna()]
        chunk.loc[chunk['first_time_check'] == "", 'first_time_check'] = file_date
        chunk.loc[chunk['last_time_check'] == "", 'last_time_check'] = file_date

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
        chunk.loc[:, ['registry_url']] = chunk[['registry_url']].replace({pd.NA: None, np.nan: None, '': None})
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


### activity person

# Specify the table and the primary key columns
table_name = "consolidated_person"
primary_key_columns = [
    "identifier",
    "person_name"
]  # Composite primary key

update_columns = ["last_time_check"] 

used_columns = ["uuid", "person", "title"]

used_columns += valid_columns

with tqdm(total=total_chunks, desc="Processing person chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns,
        ),
        desc="Processing person chunks",
    ):
        # Add missing columns and fill with a default value if they don't exist
        for col in desired_columns:
            if col not in chunk.columns:
                chunk[col] = ""  # Initialize with empty strings or NaN
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'uuid':'identifier', 'person':'person_name'}, inplace=True)
        chunk = chunk[chunk['person_name'].notna()]
        chunk = chunk[chunk['person_name'] != "No Agent"]
        chunk = chunk[chunk['person_name'] != "agent resign"]
        chunk = chunk[chunk['person_name'] != "."]
        chunk = chunk[chunk['person_name'] != "0"]
        chunk = chunk[chunk['person_name'] != "00"]
        chunk.loc[chunk['first_time_check'] == "", 'first_time_check'] = file_date
        chunk.loc[chunk['last_time_check'] == "", 'last_time_check'] = file_date
        chunk = chunk[
            [
                "identifier",
                "person_name",
                "title",
                "first_time_check",
                "last_time_check"
            ]
        ]
        chunk.loc[:, ['title']] = chunk[['title']].replace({pd.NA: None, np.nan: None, '': None})
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


### process industry

# Specify the table and the primary key columns
table_name = "consolidated_category"
primary_key_columns = ["identifier", "category_code", "category_type"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

used_columns = ['uuid', 'category_code', 'category_name', 'category_type']

used_columns += valid_columns

with tqdm(total=total_chunks, desc="Processing category chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=used_columns), desc="Processing category chunks"):
        # Add missing columns and fill with a default value if they don't exist
        for col in desired_columns:
            if col not in chunk.columns:
                chunk[col] = ""  # Initialize with empty strings or NaN
        chunk = chunk.copy()
        chunk.rename(columns={'uuid':'identifier'}, inplace=True)
        chunk = chunk[chunk['category_code'].notna()]
        chunk = chunk[chunk['category_name'].notna()]
        chunk.loc[chunk['first_time_check'] == "", 'first_time_check'] = file_date
        chunk.loc[chunk['last_time_check'] == "", 'last_time_check'] = file_date
        chunk = chunk[chunk['category_code'].str.isdigit()]
        chunk = chunk[['identifier', 'category_code', 'category_name', 'category_type', 'first_time_check', 'last_time_check']]
        chunk.loc[:, ['category_name']] = chunk[['category_name']].replace({pd.NA: None, np.nan: None, '': None})        

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


### process phone

phone_column_exists = column_exists('phone', csv_path)

# Specify the table and the primary key columns
table_name = "consolidated_phone"
primary_key_columns = ["identifier", "phone"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

if phone_column_exists:
    with tqdm(total=total_chunks, desc="Processing phone chunks") as pbar:
        for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'phone', 'phone_type']), desc="Processing phone chunks"):
            chunk = chunk.copy()
            chunk = chunk[chunk['phone'].notna()]
            chunk.rename(columns={'uuid':'identifier'}, inplace=True)
            chunk['first_time_check'] = file_date
            chunk["last_time_check"] = file_date
            chunk = chunk[['identifier', 'phone', 'phone_type', 'first_time_check', 'last_time_check']]
            chunk.loc[:, ['phone_type']] = chunk[['phone_type']].replace({pd.NA: None, np.nan: None, '': None})     

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

else:
    print("Phone column does not exist in the CSV file. Skipping phone processing.")


### process email


email_column_exists = column_exists('email', csv_path)
# Specify the table and the primary key columns
table_name = "consolidated_email"
primary_key_columns = ["identifier", "email"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

if email_column_exists:
    with tqdm(total=total_chunks, desc="Processing email chunks") as pbar:
        for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'email', 'email_type']), desc="Processing email chunks"):
            chunk = chunk.copy()
            chunk = chunk[chunk['email'].notna()]
            chunk.rename(columns={'uuid':'identifier'}, inplace=True)
            chunk['first_time_check'] = file_date
            chunk["last_time_check"] = file_date
            chunk = chunk[['identifier', 'email', 'email_type', 'first_time_check', 'last_time_check']]
            chunk.loc[:, ['email_type']] = chunk[['email_type']].replace({pd.NA: None, np.nan: None, '': None})     
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

else:
    print("Email column does not exist in the CSV file. Skipping email processing.")


### process website
website_column_exists = column_exists('website', csv_path)
# Specify the table and the primary key columns
table_name = "consolidated_website"
primary_key_columns = ["identifier", "url"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

if website_column_exists:
    with tqdm(total=total_chunks, desc="Processing website chunks") as pbar:
        for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['uuid', 'website']), desc="Processing website chunks"):
            chunk = chunk.copy()
            chunk = chunk[chunk['website'].notna()]
            chunk.rename(columns={'uuid':'identifier'}, inplace=True)
            chunk['first_time_check'] = file_date
            chunk["last_time_check"] = file_date
            chunk.rename(columns={'website':'url'}, inplace=True)
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

else:
    print("Website column does not exist in the CSV file. Skipping website processing.")

## process identifier mapping

##Specify the table and the primary key columns
table_name = "consolidated_identifier_mapping"
primary_key_columns = ['identifier', 'raw_id', 'raw_authority']  # Composite primary key


with tqdm(total=total_chunks, desc="Processing identifier mapping chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['identifier', 'uuid', 'authority']), desc="Processing identifier mapping chunks"):
        chunk = chunk.copy()
        chunk = chunk[chunk['identifier'].notna()]
        chunk.rename(columns={'identifier':'raw_id', 'authority':'raw_authority'}, inplace=True)
        chunk.rename(columns={'uuid':'identifier'}, inplace=True)
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

## process alternative identifier
alternative_column_exists = column_exists('alternative_identifier', csv_path)
##Specify the table and the primary key columns
table_name = "consolidated_identifier_mapping"
primary_key_columns = ['identifier', 'raw_id', 'raw_authority']  # Composite primary key

used_columns = ['uuid', 'alternative_identifier', 'alternative_authority']

if alternative_column_exists:

    with tqdm(total=total_chunks, desc="Processing identifier mapping chunks") as pbar:
        for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=used_columns), desc="Processing identifier mapping chunks"):
            chunk = chunk.copy()
            chunk = chunk[chunk['alternative_identifier'].notna()]
            chunk.rename(columns={'uuid':'identifier', 'alternative_identifier':'raw_id', 'alternative_authority':'raw_authority'}, inplace=True)
            chunk.drop_duplicates(inplace=True)
            chunk = chunk[['identifier', 'raw_id', 'raw_authority']]

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

else:
    print("Alternative column does not exist in the CSV file. Skipping Alternative processing.")


# ### process gui search
# # Specify the table and the primary key columns
# table_name = "consolidated_search_gui"
# primary_key_columns = [
#     "identifier",
#     "business_name",
# ]  # Composite primary key
# update_columns = ["search"]  # Columns to update in case of conflict

# with tqdm(total=total_chunks, desc="Processing search gui") as pbar:
#     for chunk in tqdm(
#         pd.read_csv(
#             csv_path,
#             chunksize=chunk_size,
#             dtype="str",
#             usecols=[
#                 "uuid",
#                 "business_name",
#                 "business_name_en",
#             ],
#         ),
#         desc="Processing search gui",
#     ):
#         chunk = chunk.copy()
#         chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
#         chunk.rename(columns={'business_name_en': 'search', 'uuid':'identifier'}, inplace=True)
#         chunk = chunk[~chunk['search'].isna()]
#         chunk['search'] = chunk['search'].str.translate(translator)
#         chunk.drop_duplicates(inplace=True)

#         # Construct the insert statement with ON CONFLICT DO UPDATE
#         placeholders = ", ".join([f":{col}" for col in chunk.columns])

#         insert_sql = f"""
#         INSERT INTO {table_name} ({', '.join(chunk.columns)})
#         VALUES ({placeholders})
#         ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
#         {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
#         """

#         if chunk is not None and not chunk.empty:
#             with engine.begin() as connection:
#                 connection.execute(text(insert_sql), chunk.to_dict(orient="records"))

#         pbar.update()