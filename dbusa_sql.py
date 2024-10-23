import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)


### process name

csv_path = '/home/rli/dbusa_data/dbusa_name.csv'
chunk_size = 1000000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'dbusa_name'
primary_key_columns = ['identifier', 'business_name']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing name chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)
        chunk.fillna('', inplace=True)

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

# Specify the table and the primary key columns
table_name = 'consolidated_name'
primary_key_columns = ['identifier', 'business_name']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing name chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)
        chunk['start_date'] = None
        chunk['end_date'] = None

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


### process address

csv_path = '/home/rli/dbusa_data/dbusa_address.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'dbusa_location'
primary_key_columns = ['identifier', 'address', 'city', 'state']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)

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

# Specify the table and the primary key columns
table_name = 'consolidated_location'
primary_key_columns = ['identifier', 'address', 'city', 'state']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)

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


### process social

csv_path = '/home/rli/dbusa_data/dbusa_social.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_social'
primary_key_columns = ['identifier', 'url']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)
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


### process designation

csv_path = '/home/rli/dbusa_data/dbusa_designation.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_designation'
primary_key_columns = ['identifier', 'designation', 'designation_type']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)
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


### process category

csv_path = '/home/rli/dbusa_data/dbusa_category.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_category'
primary_key_columns = ['identifier', 'category_code', 'category_type']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)
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


### process emp

csv_path = '/home/rli/dbusa_data/dbusa_emp.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_emp'
primary_key_columns = ['identifier', 'count']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)
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


### process person

csv_path = '/home/rli/dbusa_data/dbusa_person.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_person'
primary_key_columns = ['identifier', 'person_name']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop(columns=['gender'], inplace=True)
        chunk.drop_duplicates(inplace=True)
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

csv_path = '/home/rli/dbusa_data/dbusa_phone.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_phone'
primary_key_columns = ['identifier', 'phone']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)
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


### process sales

csv_path = '/home/rli/dbusa_data/dbusa_sales.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_sales'
primary_key_columns = ['identifier', 'count']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk = chunk[chunk['count'].str.isdigit()]
        chunk.drop_duplicates(inplace=True)
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


### process website

csv_path = '/home/rli/dbusa_data/dbusa_url.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_website'
primary_key_columns = ['identifier', 'url']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.replace('www.','').strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)
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


### process identifier

csv_path = '/home/rli/dbusa_data/dbusa_identifier.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'dbusa_identifier'
primary_key_columns = ['identifier', 'identifier_hq']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.replace('www.','').strip() if isinstance(x, str) else x)
        chunk.drop_duplicates(inplace=True)
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


## process alternative identifier

csv_path = '/home/rli/dbusa_data/dbusa_alternative_identifier.csv'
chunk_size = 100000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

##Specify the table and the primary key columns
table_name = "consolidated_identifier_mapping"
primary_key_columns = ['identifier', 'raw_id', 'raw_authority']  # Composite primary key


with tqdm(total=total_chunks, desc="Processing identifier mapping chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str', usecols=['identifier', 'alternative_identifier', 'alternative_authority']), desc="Processing identifier mapping chunks"):
        chunk = chunk.copy()
        chunk = chunk[chunk['alternative_identifier'].notna()]
        chunk.rename(columns={'alternative_identifier':'raw_id','alternative_authority':'raw_authority'}, inplace=True)
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