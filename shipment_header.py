import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

### process shipment header

csv_path = '/home/rli/kumi_match/ams__header_2020__202009291500.csv'
chunk_size = 1000
# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'shipment_header'
primary_key_columns = ['identifier']  # Composite primary key

with tqdm(total=total_chunks, desc="Processing shipment header chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing shipment header chunks"):

        for col in chunk.select_dtypes(include=['object']).columns:
            chunk[col] = chunk[col].str.strip()
        chunk.drop_duplicates(inplace=True)
        chunk.fillna('', inplace=True)

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


### process shipment cargo

csv_path = '/home/rli/kumi_match/ams__cargodesc_2020__202009291500.csv'
chunk_size = 1000
# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'shipment_cargo'
primary_key_columns = ['identifier', 'container_number', 'description_sequence_number']  # Composite primary key

with tqdm(total=total_chunks, desc="Processing shipment cargo chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing shipment cargo chunks"):

        for col in chunk.select_dtypes(include=['object']).columns:
            chunk[col] = chunk[col].str.strip()
        chunk.drop_duplicates(inplace=True)
        chunk.fillna('', inplace=True)

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


### process shipment shipper

csv_path = '/home/rli/kumi_match/shipper_kumi.csv'
chunk_size = 1000
# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'shipment_shipper'
primary_key_columns = ['shipment_identifier']  # Composite primary key
update_columns = ['identifier','meta_score']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing shipment shipper chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing shipment shipper chunks"):

        for col in chunk.select_dtypes(include=['object']).columns:
            chunk[col] = chunk[col].str.strip()
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

### process shipment consignee

csv_path = '/home/rli/kumi_match/consignee_kumi.csv'
chunk_size = 1000
# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'shipment_consignee'
primary_key_columns = ['shipment_identifier']  # Composite primary key
update_columns = ['identifier','meta_score']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing shipment consignee chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing shipment consignee chunks"):

        for col in chunk.select_dtypes(include=['object']).columns:
            chunk[col] = chunk[col].str.strip()
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