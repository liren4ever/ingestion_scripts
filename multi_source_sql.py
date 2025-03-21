import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

csv_path = '/home/rli/kumi_match/kumi_meta_20250317.csv'
chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


# Specify the table and the primary key columns
table_name = "consolidated_match"
primary_key_columns = [
    "identifier_x",
    "identifier_y",
    "version"
]  # Composite primary key

with tqdm(total=total_chunks, desc="Processing") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "identifier_x",
                "identifier_y",
                "meta_score",
            ],
        ),
        desc="Processing",
    ):
        chunk = chunk.copy()
        chunk['version'] = 'V0'
        chunk['first_time_check'] = '2025-03-17'
        chunk['last_time_check'] = '2025-03-17'
        
        chunk.drop_duplicates(inplace=True)

        # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ", ".join([f":{col}" for col in chunk.columns])

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO NOTHING
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient="records"))

        pbar.update()