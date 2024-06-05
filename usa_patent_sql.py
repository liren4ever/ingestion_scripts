import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import pandas as pd
import os
from dateutil.parser import parse

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

raw_directory = '/home/rli/uspto/'

def convert_to_unified_format(date_string):
    try:
        # Try to parse the date string
        parsed_date = parse(date_string)
        return parsed_date.strftime("%Y-%m-%d")  # Convert to the desired unified format
    except ValueError:
        return None  # If the date string cannot be parsed, return None

### process assignor
csv_file = 'patent_assignor.csv'

csv_path = os.path.join(raw_directory, csv_file)

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

### processing

# Specify the table and the primary key columns
table_name = "patent_assignor"
primary_key_columns = [
    "reel_frame",
    "assignor",
    "recorded_date",
]  # Composite primary key
update_columns = ["update_date", "execution_date"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing assignors") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "reel_no",
                "frame_no",
                "update_date",
                "recorded_date",
                "assignor",
                "execution_date",
            ],
        ),
        desc="Processing assignors",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk['reel_frame'] = chunk.apply(lambda row: str(row['reel_no']) + '-' + str(row['frame_no']), axis=1)
        chunk['update_date'] = chunk['update_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk['recorded_date'] = chunk['recorded_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk['execution_date'] = chunk['execution_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk = chunk[~chunk['assignor'].isna()]
        chunk = chunk[
            [
                "reel_frame",
                "update_date",
                "recorded_date",
                "execution_date",
                "assignor",
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


###
### process assignee
csv_file = 'patent_assignee.csv'

csv_path = os.path.join(raw_directory, csv_file)

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


# Specify the table and the primary key columns
table_name = "patent_assignee"
primary_key_columns = [
    "reel_frame",
    "assignee",
    "recorded_date",
]  # Composite primary key
update_columns = ["update_date"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing assignees") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "reel_no",
                "frame_no",
                "update_date",
                "recorded_date",
                "assignee",
                "address",
                "city",
                "state",
                "postal",
                "country"
            ],
        ),
        desc="Processing assignees",
    ):
        chunk = chunk.copy()
        chunk = chunk[~chunk['assignee'].isna()]
        chunk = chunk[~chunk['recorded_date'].isna()]
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk = chunk.apply(lambda x: ' '.join(str(x).split()) if isinstance(x, str) else x)
        chunk['reel_frame'] = chunk.apply(lambda row: str(row['reel_no']) + '-' + str(row['frame_no']), axis=1)
        chunk['update_date'] = chunk['update_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk['recorded_date'] = chunk['recorded_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk.fillna('', inplace=True)
        chunk = chunk[
            [
                "reel_frame",
                "update_date",
                "recorded_date",
                "assignee",
                "address",
                "city",
                "state",
                "postal",
                "country"
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



###
### process assignment
csv_file = 'patent_assignment.csv'

csv_path = os.path.join(raw_directory, csv_file)

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


# Specify the table and the primary key columns
table_name = "patent_assignment"
primary_key_columns = [
    "reel_frame",
    "application_number"
]  # Composite primary key

with tqdm(total=total_chunks, desc="Processing assignment") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=[
                "reel_no",
                "frame_no",
                "patent_doc_number",
                "patent_kind",
            ],
        ),
        desc="Processing assignment",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['patent_kind']=='X0']
        chunk['reel_frame'] = chunk.apply(lambda row: str(row['reel_no']) + '-' + str(row['frame_no']), axis=1)
        chunk.rename(columns={'patent_doc_number':'application_number'}, inplace=True)
        chunk = chunk[
            [
                "reel_frame",
                "application_number"
            ]
        ]
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



### process patent

cols = ['reel_no', 'frame_no', 'patent_title', 'patent_doc_number', 'patent_kind', 'patent_date']

df = pd.read_csv('/home/rli/uspto/patent_assignment.csv', usecols=cols, dtype=str)
df['reel_frame'] = df.apply(lambda row: str(row['reel_no']) + '-' + str(row['frame_no']), axis=1)

df1 = df[df['patent_kind']=='X0']
df1 = df1[['reel_frame', 'patent_title', 'patent_doc_number', 'patent_date']]
df1['patent_url'] = df1['patent_doc_number'].apply(lambda x: f'https://assignment.uspto.gov/patent/index.html#/patent/search/resultAbstract?id={x}&type=applNum')
df1.rename(columns={'patent_doc_number':'application_number', 'patent_date':'application_date'}, inplace=True)

df2 = df[df['patent_kind'].isin(['B2','B1'])]
df2 = df2[['reel_frame', 'patent_doc_number', 'patent_date']]
df2.rename(columns={'patent_doc_number':'patent_number'}, inplace=True)

df3 = df[df['patent_kind']=='A1']
df3 = df3[['reel_frame', 'patent_doc_number', 'patent_date']]
df3.rename(columns={'patent_doc_number':'publication_number', 'patent_date':'publication_date'}, inplace=True)


df = df1.merge(df2, on='reel_frame', how='left').merge(df3, on='reel_frame', how='left')

df.drop_duplicates(subset=['application_number'], inplace=True)
df = df[['patent_title', 'application_number', 'application_date', 'patent_url', 'patent_number', 'patent_date', 'publication_number', 'publication_date']]
df.to_csv('/home/rli/uspto/patent.csv', index=False)


### process patent
csv_file = 'patent.csv'

csv_path = os.path.join(raw_directory, csv_file)

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


# Specify the table and the primary key columns
table_name = "patent"
primary_key_columns = [
    "application_number"
]  # Composite primary key
update_columns = ["application_date", "publication_number", "publication_date", "patent_number", "patent_date", "patent_title", "patent_url"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing patent") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
        ),
        desc="Processing patent",
    ):
        chunk = chunk.copy()
        chunk['application_date'] = chunk['application_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk['publication_date'] = chunk['publication_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk['patent_date'] = chunk['patent_date'].apply(lambda x : convert_to_unified_format(str(x)))
        chunk.fillna('', inplace=True)
        chunk.loc[chunk['application_date']=='' , 'application_date'] = None
        chunk.loc[chunk['publication_date']=='' , 'publication_date'] = None
        chunk.loc[chunk['patent_date']=='' , 'patent_date'] = None

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