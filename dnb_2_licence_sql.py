import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

csv_path = '/home/rli/duns_lic_matched.csv'

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

### address processing

# Specify the table and the primary key columns
table_name = "duns_licence"
primary_key_columns = [
    "duns",
    "licence",
]  # Composite primary key
# update_columns = ["last_time_check"]  # Columns to update in case of conflict

# Define the regex patterns

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "DUNS_identifier",
                "identifier",
                "meta_score",
                "name_score",
                "address_score",
                "city_score",
                "postal_score",
            ],
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

        chunk.rename(columns={'DUNS_identifier': 'duns', 'identifier': 'licence'}, inplace=True)
        chunk = chunk[
            [
                "duns",
                "licence",
                "meta_score",
                "name_score",
                "address_score",
                "city_score",
                "postal_score",
            ]
        ]
        chunk.drop_duplicates(inplace=True)

        # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ", ".join([f":{col}" for col in chunk.columns])

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient="records"))

        pbar.update()