import pandas as pd
import os
from tqdm import tqdm
import subprocess
from datetime import datetime
import sys
from sqlalchemy import create_engine, text
from uuid import uuid5, UUID

def identifier_uuid(text):
    namespace = UUID("00000000-0000-0000-0000-000000000000")
    uuid = uuid5(namespace, text)
    return uuid


connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)


file_name = sys.argv[1]

file_directory = '/home/rli/dbusa_data/'

file_path = os.path.join(file_directory, file_name)

file_date = datetime.today().strftime('%Y-%m-%d')


chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', file_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Process name

cols = ['DBUSA_Business_ID','DBUSA_Executive_ID',"Full_Name","Gender","Source_Title","Standardized_Title"]
##"Executive_Department","Primary_Exec_Indicator","Executive_Level","Exec_Type","Executive_LinkedIN","Executive_Phone","Executive_Email_Verification_Date"]

genders_map = {'0':'M', 'M':'M', '2':'F', 'F':'F', '3':'U', 'U':'U'}

header = True

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['DBUSA_Executive_ID'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','DBUSA_Executive_ID':'person_identifier', 'Full_Name':'person_name', 'Gender':'gender', 'Source_Title':'title', "Standardized_Title":"standardized_title"},inplace=True)
        chunk['gender'] = chunk['gender'].map(genders_map)
        chunk = chunk[[ 'identifier', 'person_identifier', 'person_name', 'gender', 'title', 'standardized_title', 'first_time_check', 'last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_name.csv', index=False, header=header, mode='a')
        header = False
        pbar.update()


### load database

csv_path = '/home/rli/dbusa_data/dbusa_executive_name.csv'
chunk_size = 10000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'dbusa_executive'
primary_key_columns = ['identifier', 'person_identifier']  # Composite primary key
update_columns = ['gender', 'title', 'standardized_title', 'last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.copy()
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


# Process phone

cols = ['DBUSA_Executive_ID',"Executive_Phone"]
##"Executive_Department","Primary_Exec_Indicator","Executive_Level","Exec_Type","Executive_LinkedIN","Executive_Phone","Executive_Email_Verification_Date"]

header = True

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Executive_Phone'].notnull()]
        chunk['phone_type'] = 'work'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Executive_Phone':'phone'},inplace=True)
        chunk = chunk[['person_identifier', 'phone', 'phone_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_phone.csv', index=False, header=header, mode='a')
        header = False
        pbar.update()


cols = ['DBUSA_Executive_ID',"Cons_Phone"]

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Cons_Phone'].notnull()]
        chunk['phone_type'] = 'home'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Cons_Phone':'phone'},inplace=True)
        chunk = chunk[['person_identifier', 'phone', 'phone_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_phone.csv', index=False, header=header, mode='a')

        pbar.update()


cols = ['DBUSA_Executive_ID',"Cons_Cell_Phone"]

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Cons_Cell_Phone'].notnull()]
        chunk['phone_type'] = 'cell phone'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Cons_Cell_Phone':'phone'},inplace=True)
        chunk = chunk[['person_identifier', 'phone', 'phone_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_phone.csv', index=False, header=header, mode='a')

        pbar.update()


csv_path = '/home/rli/dbusa_data/dbusa_executive_phone.csv'
chunk_size = 10000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'dbusa_executive_phone'
primary_key_columns = ['person_identifier', 'phone']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
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


# Process email


cols = ['DBUSA_Executive_ID',"Cons_Email"]

header = True

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Cons_Email'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk['email_type'] = ''
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Cons_Email':'email'},inplace=True)
        chunk = chunk[['person_identifier', 'email', 'email_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_email.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()


cols = ['DBUSA_Executive_ID',"Cons_Email_02"]

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Cons_Email_02'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk['email_type'] = ''
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Cons_Email_02':'email'},inplace=True)
        chunk = chunk[['person_identifier', 'email', 'email_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_email.csv', index=False, header=header, mode='a')

        pbar.update()


cols = ['DBUSA_Executive_ID',"Cons_Email_03"]

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Cons_Email_03'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk['email_type'] = ''
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Cons_Email_03':'email'},inplace=True)
        chunk = chunk[['person_identifier', 'email', 'email_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_email.csv', index=False, header=header, mode='a')

        pbar.update()


cols = ['DBUSA_Executive_ID',"Cons_Email_04"]

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Cons_Email_04'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk['email_type'] = ''
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Cons_Email_04':'email'},inplace=True)
        chunk = chunk[['person_identifier', 'email', 'email_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_email.csv', index=False, header=header, mode='a')

        pbar.update()


cols = ['DBUSA_Executive_ID',"Cons_Email_05"]

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Cons_Email_05'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk['email_type'] = ''
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Cons_Email_05':'email'},inplace=True)
        chunk = chunk[['person_identifier', 'email', 'email_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_email.csv', index=False, header=header, mode='a')

        pbar.update()

cols = ['DBUSA_Executive_ID',"Email"]

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Email'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk['email_type'] = 'work'
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Email':'email'},inplace=True)
        chunk = chunk[['person_identifier', 'email', 'email_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_email.csv', index=False, header=header, mode='a')

        pbar.update()

### load database

csv_path = '/home/rli/dbusa_data/dbusa_executive_email.csv'
chunk_size = 10000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'dbusa_executive_email'
primary_key_columns = ['person_identifier', 'email']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
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


# Process social
cols = ['DBUSA_Executive_ID',"Executive_LinkedIN"]

header = True

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            file_path,
            chunksize=chunk_size,
            dtype="str",
            sep="|",
            usecols=cols,
            encoding="latin1",
        ),
        desc="Processing chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk[chunk['Executive_LinkedIN'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk['url_type'] = 'linkedin'
        chunk.rename(columns={'DBUSA_Executive_ID':'person_identifier', 'Executive_LinkedIN':'url'},inplace=True)
        chunk = chunk[['person_identifier', 'url', 'url_type', 'first_time_check', 'last_time_check']]
        chunk['person_identifier'] = chunk['person_identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_executive_social.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()


### load database

csv_path = '/home/rli/dbusa_data/dbusa_executive_social.csv'
chunk_size = 10000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'dbusa_executive_social'
primary_key_columns = ['person_identifier', 'url']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
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
