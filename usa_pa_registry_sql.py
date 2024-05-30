import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import pandas as pd
import os
import re
from datetime import datetime

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)


raw_directory = '/var/rel8ed.to/nfs/share/project_originals/files/files_cooked/'
files = os.listdir(raw_directory)
files = [f for f in files if f.endswith('.csv')]
files = [f for f in files if f.startswith('platinum_usa_pa_pa_registry')]

def extract_date(filename):
    # Assuming the date is always in the same position in the filename
    date_str = filename.split('_')[-1].split('.')[0]  # Splits the string and takes the second last element
    return datetime.strptime(date_str, '%Y-%m-%d')

latest_file = max(files, key=extract_date)

csv_path = os.path.join(raw_directory, latest_file)
file_date = latest_file.split('_')[-1].split('.')[0]

chunk_size = 10000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

### address processing

# Specify the table and the primary key columns
table_name = "registry_location"
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
                "address_en",
                "city_en",
                "region_code",
                "postal_code",
                "country_code",
                "lat",
                "lon"
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'address_en': 'address', 'city_en': 'city', 'region_code': 'state', 'postal_code': 'postal', 'country_code': 'country', "lon":"latitude", "lat":"longitude"}, inplace=True)
        chunk.fillna("", inplace=True)
        chunk["state"] = chunk.apply(
            lambda row: row["state"].replace(row["country"][0:2]+'-', ""), axis=1
        )
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
        # chunk['identifier'] = chunk['identifier'].apply(lambda x: x+'_OR')
        chunk.loc[chunk["latitude"] == "", "latitude"] = None
        chunk.loc[chunk["longitude"] == "", "longitude"] = None
        chunk["location_type"] = "main"
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
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

# with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
#     for chunk in tqdm(
#         pd.read_csv(
#             csv_path,
#             chunksize=chunk_size,
#             dtype="str",
#             usecols=[
#                 "registry_number",
#                 "alt_address_en",
#                 "alt_city_en",
#                 "alt_region_code",
#                 "alt_postal_code",
#                 "alt_country_code",
#                 "alt_address_type",
#             ],
#         ),
#         desc="Processing chunks",
#     ):
#         chunk = chunk.copy()
#         chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
#         chunk.rename(columns={'registry_number': 'identifier', 'alt_address_en': 'address', 'alt_city_en': 'city', 'alt_region_code': 'state', 'alt_postal_code': 'postal', 'alt_country_code': 'country', "alt_address_type":"location_type"}, inplace=True)
#         chunk.fillna("", inplace=True)
#         chunk = chunk[chunk['address'] != ""]
#         chunk = chunk[chunk['country'] != ""]
#         chunk["state"] = chunk.apply(
#             lambda row: row["state"].replace(row["country"][0:2]+'-', ""), axis=1
#         )
#         chunk["postal"] = chunk["postal"].apply(lambda x: str(x).replace(" ", ""))
#         chunk["postal"] = chunk.apply(
#             lambda row: row["postal"].replace(row["state"], ""), axis=1
#         )
#         chunk.loc[chunk["country"] == "USA", "postal"] = chunk.loc[
#             chunk["country"] == "USA", "postal"
#         ].apply(lambda x: x if bool(re.match(usa_pattern, x)) else "")
#         chunk.loc[chunk["country"] == "CAN", "postal"] = chunk.loc[
#             chunk["country"] == "CAN", "postal"
#         ].apply(lambda x: x[0:6])
#         chunk.loc[chunk["country"] == "CAN", "postal"] = chunk.loc[
#             chunk["country"] == "CAN", "postal"
#         ].apply(lambda x: x if bool(re.match(canada_pattern, x)) else "")
#         chunk["state"] = chunk["state"].apply(lambda x: x.upper()[0:2])
#         chunk['identifier'] = chunk['identifier'].apply(lambda x: x+'_NY')
#         chunk["latitude"] = None
#         chunk["longitude"] = None
#         chunk['first_time_check'] = file_date
#         chunk["last_time_check"] = file_date
#         chunk = chunk[
#             [
#                 "identifier",
#                 "address",
#                 "city",
#                 "state",
#                 "postal",
#                 "country",
#                 "latitude",
#                 "longitude",
#                 "location_type",
#                 "first_time_check",
#                 "last_time_check",
#             ]
#         ]
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



### name processing

# Specify the table and the primary key columns
table_name = "registry_name"
primary_key_columns = [
    "identifier",
    "name",
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
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'business_name':'name'}, inplace=True)
        chunk.fillna("", inplace=True)
        chunk = chunk[chunk['name'] != ""]
        chunk["name_type"] = 'main'
        chunk["start_date"] = None
        chunk["end_date"] = None
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk['identifier'] = chunk['identifier'].apply(lambda x: x+'_OR')
        chunk = chunk[
            [
                "identifier",
                "name",
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

# with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
#     for chunk in tqdm(
#         pd.read_csv(
#             csv_path,
#             chunksize=chunk_size,
#             dtype="str",
#             usecols=[
#                 "registry_number",
#                 "alt_business_names",
#             ],
#         ),
#         desc="Processing chunks",
#     ):
#         chunk = chunk.copy()
#         chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
#         chunk.rename(columns={'registry_number': 'identifier', 'alt_business_names':'name'}, inplace=True)
#         chunk.fillna("", inplace=True)
#         chunk = chunk[chunk['name'] != ""]
#         chunk["name_type"] = None
#         chunk["start_date"] = None
#         chunk["end_date"] = None
#         chunk['first_time_check'] = file_date
#         chunk["last_time_check"] = file_date
#         chunk['identifier'] = chunk['identifier'].apply(lambda x: x+'_NY')
#         chunk = chunk[
#             [
#                 "identifier",
#                 "name",
#                 "name_type",
#                 "start_date",
#                 "end_date",
#                 "first_time_check",
#                 "last_time_check",
#             ]
#         ]
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



### identifier processing


# Specify the table and the primary key columns
table_name = "registry_identifier"
primary_key_columns = [
    "identifier",
    "identifier_hq",
]  # Composite primary key
update_columns = ["status", "last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                'registry_status',
                "legal_type",
                "authority",
                "url"
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'registry_number': 'identifier', 'registry_status':'status', 'url':'identifier_url'}, inplace=True)
        chunk.fillna("", inplace=True)
        chunk = chunk[chunk['identifier'] != ""]
        chunk["identifier_hq"] = None
        chunk["hq_authority"] = None
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk['identifier'] = chunk['identifier'].apply(lambda x: x+'_OR')
        chunk = chunk[
            [
                "identifier",
                "identifier_hq",
                "authority",
                "hq_authority",
                "legal_type",
                "status",
                "first_time_check",
                "last_time_check",
                "identifier_url"
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
    "name",
    "activity_date"
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
                "authority",
                'Category',
                "creation_year",
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'Category':'name', 'creation_year':'activity_date'}, inplace=True)
        chunk.fillna("", inplace=True)
        chunk = chunk[chunk['activity_date'] != ""]
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk['identifier'] = chunk['identifier'].apply(lambda x: x+'_OR')
        chunk = chunk[
            [
                "identifier",
                "authority",
                "name",
                "activity_date",
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
table_name = "registry_person"
primary_key_columns = [
    "identifier",
    "name"
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
                "person"
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'person':'name'}, inplace=True)
        chunk.fillna("", inplace=True)
        chunk = chunk[chunk['name'] != ""]
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk['identifier'] = chunk['identifier'].apply(lambda x: x+'_OR')
        chunk['title'] = None
        chunk = chunk[
            [
                "identifier",
                "name",
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