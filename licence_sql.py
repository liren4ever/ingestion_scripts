import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import os
import re
from datetime import datetime
import sys

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)


raw_directory = '/var/rel8ed.to/nfs/share/project_originals/files/files_cooked/'

input_file_name = sys.argv[1]

files = os.listdir(raw_directory)
files = [f for f in files if f.endswith('.csv')]
files = [f for f in files if f.startswith(input_file_name)]

def extract_date(filename):
    # Assuming the date is always in the same position in the filename
    date_str = filename.split('_')[-1].split('.')[0]  # Splits the string and takes the second last element
    return datetime.strptime(date_str, '%Y-%m-%d')

latest_file = max(files, key=extract_date)

csv_path = os.path.join(raw_directory, latest_file)
file_date = latest_file.split('_')[-1].split('.')[0]

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

### address processing

# Specify the table and the primary key columns
table_name = "licence_location"
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
                "identifier",
                "address_en",
                "city",
                "region_code",
                "postal_code",
                "country_code",
                "lat",
                "lon",
                "address_type"
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.fillna("", inplace=True)

        chunk.rename(columns={'address_en': 'address', 'region_code': 'state', 'postal_code': 'postal', 'country_code': 'country', "lon":"longitude", "lat":"latitude", "address_type":"location_type"}, inplace=True)
        
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
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
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


### alt_address processing

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                "alt_address_en",
                "alt_city",
                "alt_region_code",
                "alt_postal_code",
                "alt_country_code",
                "alt_address_type",
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.fillna("", inplace=True)

        chunk.rename(columns={'alt_address_en': 'address', 'alt_city': 'city', 'alt_region_code': 'state', 'alt_postal_code': 'postal', 'alt_country_code': 'country', "alt_address_type":"location_type"}, inplace=True)
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
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
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



### name processing

# Specify the table and the primary key columns
table_name = "licence_name"
primary_key_columns = [
    "identifier",
    "business_name",
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                "business_name",
                "business_name_type",
                "business_name_en",
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.fillna("", inplace=True)

        chunk = chunk[chunk['business_name_en'] != ""]

        chunk.rename(columns={'business_name_type': 'name_type'}, inplace=True)
        chunk["start_date"] = None
        chunk["end_date"] = None
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk = chunk[
            [
                "identifier",
                "business_name",
                "name_type",
                "start_date",
                "end_date",
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



### alt_name processing

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                "alt_business_names",
                "alt_business_names_en",
                "alt_business_names_type",
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.fillna("", inplace=True)

        chunk = chunk[chunk['alt_business_names_en'] != ""]

        chunk.rename(columns={'alt_business_names':'business_name', 'alt_business_names_type': 'name_type'}, inplace=True)
        chunk["start_date"] = None
        chunk["end_date"] = None
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk = chunk[
            [
                "identifier",
                "business_name",
                "name_type",
                "start_date",
                "end_date",
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



### identifier processing


# Specify the table and the primary key columns
table_name = "licence_identifier"
primary_key_columns = [
    "identifier",
    "legal_type",
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                'legal_type',
                "url",
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'url':'identifier_url'}, inplace=True)
        chunk.fillna("", inplace=True)
        chunk = chunk[chunk['legal_type'] != ""]
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk = chunk[
            [
                "identifier",
                "legal_type",
                "first_time_check",
                "last_time_check",
                "identifier_url",
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
table_name = "licence_activity"
primary_key_columns = [
    "identifier",
    "licence_name",
    "authority",
    "issued_date"
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                "licence_number",
                'Category',
                "authority",
                "issued_date",
                "renewed_date",
                "expired_date",
                "licence_status",
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'Category':'licence_name'}, inplace=True)
        chunk.fillna("", inplace=True)

        chunk = chunk[chunk['licence_name'] != ""]

        chunk['issued_date'] = chunk['issued_date'].replace('', pd.NA)
        chunk['renewed_date'] = chunk['renewed_date'].replace('', pd.NA)
        chunk['expired_date'] = chunk['expired_date'].replace('', pd.NA)
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date

        chunk = chunk[
            [
                "identifier",
                "licence_number",
                "licence_name",
                "authority",
                "issued_date",
                "renewed_date",
                "expired_date",
                "licence_status",
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


### activity person

# Specify the table and the primary key columns
table_name = "licence_person"
primary_key_columns = [
    "identifier",
    "person_name",
    "title"
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                "person",
                "title"
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'person':'person_name'}, inplace=True)
        chunk.fillna("", inplace=True)
        chunk = chunk[chunk['person_name'] != ""]
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk = chunk[
            [
                "identifier",
                "person_name",
                "title",
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



### process industry

# Specify the table and the primary key columns
table_name = "licence_category"
primary_key_columns = ["identifier", "category_code", "category_type"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict


with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['identifier', 'category_code', 'category_name', 'category_type']), desc="Processing chunks"):
        chunk = chunk.copy()
        chunk = chunk[chunk['category_code'].notna()]
        chunk = chunk[chunk['category_name'].notna()]
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk = chunk[['identifier', 'category_code', 'category_name', 'category_type', 'first_time_check', 'last_time_check']]

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