import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import os
import re
import sys

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)


raw_directory = '/var/rel8ed.to/nfs/share/project_originals/files/files_cooked/'

input_file_name = sys.argv[1]


csv_path = os.path.join(raw_directory, input_file_name+'.csv')
file_date = input_file_name.split('_')[-1].split('.')[0]

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


def column_exists(column_name, csv_path):
    try:
        with open(csv_path, 'r') as f:
            first_line = f.readline()
            column_names = first_line.strip().split(',')
            return column_name in column_names
    except FileNotFoundError:
        print(f"File not found: {csv_path}")
        return False

### address processing
### process name and location to consolidated tables

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

with tqdm(total=total_chunks, desc="Processing location chunks") as pbar:
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
        desc="Processing location chunks",
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
        chunk['location_status'] = None
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
                "location_status",
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

with tqdm(total=total_chunks, desc="Processing location chunks") as pbar:
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
        desc="Processing location chunks",
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
        chunk['location_status'] = None
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
                "location_status",
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



# Specify the table and the primary key columns
table_name = "consolidated_name"
primary_key_columns = [
    "identifier",
    "business_name",
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
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
        desc="Processing name chunks",
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

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
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
        desc="Processing name chunks",
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
        chunk = chunk[chunk['business_name'].str.len() <= 500]
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

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
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
        desc="Processing name chunks",
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

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
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
        desc="Processing name chunks",
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
        chunk = chunk[chunk['business_name'].str.len() <= 500]
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

with tqdm(total=total_chunks, desc="Processing legal type chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                'legal_type',
            ],
        ),
        desc="Processing legal type chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk = chunk[chunk['legal_type'].notna()]
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date
        chunk = chunk[
            [
                "identifier",
                "legal_type",
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


with tqdm(total=total_chunks, desc="Processing licence chunks") as pbar:
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
                "licence_url",
            ],
        ),
        desc="Processing licence chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'Category':'licence_name'}, inplace=True)
        chunk.fillna("", inplace=True)

        chunk = chunk[chunk['licence_name'] != ""]
        chunk.loc[chunk["issued_date"] == "", "issued_date"] = None
        chunk.loc[chunk["renewed_date"] == "", "renewed_date"] = None
        chunk.loc[chunk["expired_date"] == "", "expired_date"] = None
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
                "last_time_check",
                "licence_url"
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
table_name = "consolidated_person"
primary_key_columns = [
    "identifier",
    "person_name"
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

person_column_exists = column_exists('person', csv_path)

if person_column_exists:

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
else:
    print("Phone column does not exist in the CSV file. Skipping person processing.")

### process industry

# Specify the table and the primary key columns
table_name = "consolidated_category"
primary_key_columns = ["identifier", "category_code", "category_type"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict


category_column_exists = column_exists('category_code', csv_path)

if category_column_exists:

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
else:
    print("Phone column does not exist in the CSV file. Skipping category processing.")

### process email

# Specify the table and the primary key columns
table_name = "consolidated_email"
primary_key_columns = ["identifier", "email"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict
    
email_column_exists = column_exists('email', csv_path)

if email_column_exists:
    with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
        for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['identifier', 'email', 'email_type']), desc="Processing chunks"):
            chunk = chunk.copy()
            chunk = chunk[chunk['email'].notna()]
            chunk['email'] = chunk['email'].apply(lambda x: x.lower())
            chunk['first_time_check'] = file_date
            chunk["last_time_check"] = file_date
            chunk = chunk[['identifier', 'email', 'email_type', 'first_time_check', 'last_time_check']]

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
# Specify the table and the primary key columns
table_name = "consolidated_website"
primary_key_columns = ["identifier", "url"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict
    
website_column_exists = column_exists('website', csv_path)

if website_column_exists:

    with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
        for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['identifier', 'website']), desc="Processing chunks"):
            chunk = chunk.copy()
            chunk = chunk[chunk['website'].notna()]
            chunk['website'] = chunk['website'].apply(lambda x: x.lower().replace('www.',''))
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

### process phone

# Specify the table and the primary key columns
table_name = "consolidated_phone"
primary_key_columns = ["identifier", "phone"]  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict
    
phone_column_exists = column_exists('phone', csv_path)

if phone_column_exists:
    with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
        for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['identifier', 'phone', 'phone_type']), desc="Processing chunks"):
            chunk = chunk.copy()
            chunk.fillna('', inplace=True)
            chunk = chunk[chunk['phone'].notna()]
            chunk = chunk[chunk['phone'].str.len() == 10]
            chunk['first_time_check'] = file_date
            chunk["last_time_check"] = file_date
            chunk['phone_type'] = chunk['phone_type'].apply(lambda x: x.split(' ')[0].lower())
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
else:
    print("Phone column does not exist in the CSV file. Skipping phone processing.")


### process legal type consolidated
# Specify the table and the primary key columns
table_name = "consolidated_legal_type"
primary_key_columns = [
    "identifier",
]  # Composite primary key
update_columns = ["legal_type"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing legal type chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                'legal_type',
            ],
        ),
        desc="Processing legal type chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk = chunk[chunk['legal_type'].notna()]
        chunk = chunk[
            [
                "identifier",
                "legal_type"
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
    "status",
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing status chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier",
                "licence_status",
            ],
        ),
        desc="Processing status chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.rename(columns={'licence_status':'status'}, inplace=True)
        chunk.fillna("", inplace=True)

        chunk = chunk[chunk['status'] != ""]
        chunk['first_time_check'] = file_date
        chunk["last_time_check"] = file_date

        chunk = chunk[
            [
                "identifier",
                "status",
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
        {', '.join([f"{col} = excluded.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient="records"))

        pbar.update()



# Specify the table and the primary key columns
table_name = "consolidated_description"
primary_key_columns = [
    "identifier",
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict


    
desc_column_exists = column_exists('business_description', csv_path)


if desc_column_exists:
    with tqdm(total=total_chunks, desc="Processing status chunks") as pbar:
        for chunk in tqdm(
            pd.read_csv(
                csv_path,
                chunksize=chunk_size,
                dtype="str",
                usecols=[
                    "identifier",
                    "business_description",
                ],
            ),
            desc="Processing status chunks",
        ):
            chunk = chunk.copy()
            chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
            chunk.rename(columns={'business_description':'description'}, inplace=True)
            chunk = chunk[chunk['description'].notna()]
            chunk['first_time_check'] = file_date
            chunk["last_time_check"] = file_date
            chunk = chunk[
                [
                    "identifier",
                    "description",
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
            {', '.join([f"{col} = excluded.{col}" for col in update_columns])}
            """

            if chunk is not None and not chunk.empty:
                with engine.begin() as connection:
                    connection.execute(text(insert_sql), chunk.to_dict(orient="records"))

            pbar.update()

else:
    print("Desc column does not exist in the CSV file. Skipping phone processing.")