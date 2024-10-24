import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from tqdm import tqdm
import subprocess
from uuid import uuid5, UUID
import numpy as np


def identifier_uuid(text):
    namespace = UUID("00000000-0000-0000-0000-000000000000")
    uuid = uuid5(namespace, text)
    return uuid

####
connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

file_date = datetime.today().strftime('%Y-%m-%d')


files = os.listdir('/var/rel8ed.to/nfs/share/usspending_data/')
files = [f for f in files if f.endswith('.csv')]
files = [f for f in files if 'Contracts' in f]

### processing name

cols = ['recipient_uei', 'recipient_duns', 'recipient_name']

header = True

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_name'].notnull()]
    df.rename(columns={'recipient_name': 'business_name'}, inplace=True)
    df['name_type'] = 'main'
    df['uei_uuid'] = df['recipient_uei'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['duns_uuid'] = df['recipient_duns'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['identifier'] = df['uei_uuid'].fillna(df['duns_uuid'])
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'business_name', 'name_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_name.csv', index=False, mode='a', header=header)

    header = False



cols = ['recipient_uei', 'recipient_duns', 'recipient_doing_business_as_name']
for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_doing_business_as_name'].notnull()]
    df.rename(columns={'recipient_doing_business_as_name': 'business_name'}, inplace=True)
    df['name_type'] = 'dba'
    df['uei_uuid'] = df['recipient_uei'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['duns_uuid'] = df['recipient_duns'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['identifier'] = df['uei_uuid'].fillna(df['duns_uuid'])
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'business_name', 'name_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_name.csv', index=False, mode='a', header=header)



### loading data to database

csv_path = '/var/rel8ed.to/nfs/share/usspending_data/usspending_name.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'federal_awards_name'
primary_key_columns = ['identifier', 'business_name']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk = chunk[chunk['identifier']!='identifier']
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
table_name = 'consolidated_name'
primary_key_columns = ['identifier', 'business_name']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        chunk = chunk[chunk['identifier']!='identifier']
        chunk['start_date'] = None
        chunk['end_date'] = None
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

### processing address

cols = ['recipient_uei', 'recipient_duns', 'recipient_country_code', 'recipient_address_line_1', 'recipient_address_line_2', 'recipient_city_name', 'recipient_state_code', 'recipient_zip_4_code']

header = True

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_uei'].notnull()]
    df.fillna('', inplace=True)
    df['address'] = df[['recipient_address_line_1', 'recipient_address_line_2']].apply(lambda x: (' '.join(x.astype(str))).strip(), axis=1)
    df['postal'] = df.apply(lambda x: ('-'.join([x['recipient_zip_4_code'][:5], x['recipient_zip_4_code'][5:9]])).strip('-') if pd.notnull(x['recipient_zip_4_code']) and x['recipient_country_code'] == 'USA' else x['recipient_zip_4_code'], axis=1)
    df.rename(columns={'recipient_city_name': 'city', 'recipient_state_code':'state', 'recipient_country_code':'country'}, inplace=True)
    df['uei_uuid'] = df['recipient_uei'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['duns_uuid'] = df['recipient_duns'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['identifier'] = df['uei_uuid'].fillna(df['duns_uuid'])
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'address', 'city', 'state', 'postal', 'country', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_address.csv', index=False, mode='a', header=header)

    header = False

### loading data to database

csv_path = '/var/rel8ed.to/nfs/share/usspending_data/usspending_address.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'federal_awards_location'
primary_key_columns = ['identifier', 'address', 'city', 'state']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
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
table_name = 'consolidated_location'
primary_key_columns = ['identifier', 'address', 'city', 'state']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
        chunk.fillna('', inplace=True)
        chunk['latitude'] = None
        chunk['longitude'] = None
        chunk['location_type'] = None
        chunk['location_status'] = None
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


### processing identifier

cols = ['recipient_uei', 'recipient_parent_uei']

header = True

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_parent_uei'].notnull()]
    df = df[df['recipient_uei'].notnull()]
    df.rename(columns={'recipient_uei': 'identifier', 'recipient_parent_uei': 'identifier_hq'}, inplace=True)
    df['identifier'] = df['identifier'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['identifier_hq'] = df['identifier_hq'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['authority'] = 'Unique Entity Identifier'
    df['hq_authority'] = 'Unique Entity Identifier'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'identifier_hq', 'authority', 'hq_authority', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_identifier.csv', index=False, mode='a', header=header)

    header = False


cols = ['recipient_parent_duns', 'recipient_duns']

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_parent_duns'].notnull()]
    df = df[df['recipient_duns'].notnull()]
    df.rename(columns={'recipient_duns': 'identifier', 'recipient_parent_duns': 'identifier_hq'}, inplace=True)
    df['identifier'] = df['identifier'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['identifier_hq'] = df['identifier_hq'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['authority'] = 'DNB'
    df['hq_authority'] = 'DNB'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'identifier_hq', 'authority', 'hq_authority', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_identifier.csv', index=False, mode='a', header=header)

### loading data to database

csv_path = '/var/rel8ed.to/nfs/share/usspending_data/usspending_identifier.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_identifier_hierarchy'
primary_key_columns = ['identifier', 'identifier_hq']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
        chunk['flag'] = np.where(chunk['identifier'] == chunk['identifier_hq'], 't', 'f')
        chunk.drop(columns=['authority', 'hq_authority'], inplace=True)
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

### processing raw identifier

cols = ['recipient_uei']

header = True

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_uei'].notnull()]
    df.rename(columns={'recipient_uei': 'raw_id'}, inplace=True)
    df['identifier'] = df['raw_id'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['raw_authority'] = 'Unique Entity Identifier'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'raw_id', 'raw_authority']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_raw_identifier.csv', index=False, mode='a', header=header)

    header = False

cols = ['recipient_parent_uei']

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_parent_uei'].notnull()]
    df.rename(columns={'recipient_parent_uei': 'raw_id'}, inplace=True)
    df['identifier'] = df['raw_id'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['raw_authority'] = 'Unique Entity Identifier'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'raw_id', 'raw_authority']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_raw_identifier.csv', index=False, mode='a', header=header)

cols = ['recipient_duns']

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_duns'].notnull()]
    df.rename(columns={'recipient_duns': 'raw_id'}, inplace=True)
    df['identifier'] = df['raw_id'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['raw_authority'] = 'DNB'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'raw_id', 'raw_authority']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_identifier.csv', index=False, mode='a', header=header)

cols = ['recipient_parent_duns']

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_parent_duns'].notnull()]
    df.rename(columns={'recipient_parent_duns': 'raw_id'}, inplace=True)
    df['identifier'] = df['raw_id'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['raw_authority'] = 'DNB'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'raw_id', 'raw_authority']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_identifier.csv', index=False, mode='a', header=header)


cols = ['recipient_uei', 'cage_code']

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_uei'].notnull()]
    df = df[df['cage_code'].notnull()]
    df.rename(columns={'cage_code': 'raw_id'}, inplace=True)
    df['identifier'] = df['recipient_uei'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['raw_authority'] = 'Commercial and Government Entity'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'raw_id', 'raw_authority']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_raw_identifier.csv', index=False, mode='a', header=header)

cols = ['recipient_duns', 'cage_code']

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_duns'].notnull()]
    df = df[df['cage_code'].notnull()]
    df.rename(columns={'cage_code': 'raw_id'}, inplace=True)
    df['identifier'] = df['recipient_duns'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['raw_authority'] = 'Commercial and Government Entity'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'raw_id', 'raw_authority']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_raw_identifier.csv', index=False, mode='a', header=header)

### loading data to database

csv_path = '/var/rel8ed.to/nfs/share/usspending_data/usspending_raw_identifier.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_identifier_mapping'
primary_key_columns = ['identifier', 'raw_id', 'raw_authority']

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
        chunk.drop_duplicates(inplace=True)
        chunk = chunk[chunk['raw_id'].notnull()]
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


### processing category

cols = ['recipient_uei', 'recipient_duns', 'naics_code', 'naics_description']

header = True

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['naics_code'].notnull()]
    df['uei_uuid'] = df['recipient_uei'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['duns_uuid'] = df['recipient_duns'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['identifier'] = df['uei_uuid'].fillna(df['duns_uuid'])
    df.rename(columns={'naics_code': 'category_code', 'naics_description':'category_name'}, inplace=True)
    df['category_type'] = 'NAICS'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'category_code', 'category_name', 'category_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_category.csv', index=False, mode='a', header=header)

    header = False

### loading data to database

csv_path = '/var/rel8ed.to/nfs/share/usspending_data/usspending_category.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

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


### processing phone

cols = ['recipient_uei', 'recipient_duns', 'recipient_phone_number']

header = True

for f in files:
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_phone_number'].notnull()]
    df['uei_uuid'] = df['recipient_uei'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['duns_uuid'] = df['recipient_duns'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['identifier'] = df['uei_uuid'].fillna(df['duns_uuid'])
    df.rename(columns={'recipient_phone_number': 'phone'}, inplace=True)
    df['phone_type'] = 'work'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'phone', 'phone_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_phone.csv', index=False, mode='a', header=header)

    header = False

cols = ['recipient_uei', 'recipient_duns', 'recipient_fax_number']

for f in files:
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_fax_number'].notnull()]
    df['uei_uuid'] = df['recipient_uei'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['duns_uuid'] = df['recipient_duns'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['identifier'] = df['uei_uuid'].fillna(df['duns_uuid'])
    df.rename(columns={'recipient_fax_number': 'phone'}, inplace=True)
    df['phone_type'] = 'fax'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'phone', 'phone_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_phone.csv', index=False, mode='a', header=header)


### loading data to database

csv_path = '/var/rel8ed.to/nfs/share/usspending_data/usspending_phone.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

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

### processing activity

cols = ["recipient_uei","recipient_duns","contract_award_unique_key","award_id_piid","parent_award_agency_id","parent_award_agency_name","parent_award_id_piid","federal_action_obligation","total_dollars_obligated","total_outlayed_amount_for_overall_award","base_and_exercised_options_value","current_total_value_of_award","base_and_all_options_value","potential_total_value_of_award", "action_date","period_of_performance_start_date", "period_of_performance_current_end_date", "period_of_performance_potential_end_date", "ordering_period_end_date", "solicitation_date", "awarding_agency_code", "awarding_agency_name", "awarding_sub_agency_code", "awarding_sub_agency_name", "awarding_office_code", "awarding_office_name","funding_agency_code", "funding_agency_name", "funding_sub_agency_code", "funding_sub_agency_name", "funding_office_code", "funding_office_name", "usaspending_permalink", "initial_report_date", "last_modified_date"]

header = True

for f in files:
    print(f)
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df['uei_uuid'] = df['recipient_uei'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['duns_uuid'] = df['recipient_duns'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['identifier'] = df['uei_uuid'].fillna(df['duns_uuid'])
    df.rename(columns={'initial_report_date':'first_time_check', 'last_modified_date':'last_time_check'}, inplace=True)
    df['period_of_performance_potential_end_date'] = df['period_of_performance_potential_end_date'].apply(lambda x : x[0:10] if pd.notnull(x) else x)
    df.drop_duplicates(inplace=True)
    df = df[["identifier","contract_award_unique_key","award_id_piid","parent_award_agency_id","parent_award_agency_name","parent_award_id_piid","federal_action_obligation","total_dollars_obligated","total_outlayed_amount_for_overall_award","base_and_exercised_options_value","current_total_value_of_award","base_and_all_options_value","potential_total_value_of_award", "action_date","period_of_performance_start_date", "period_of_performance_current_end_date", "period_of_performance_potential_end_date", "ordering_period_end_date", "solicitation_date", "awarding_agency_code", "awarding_agency_name", "awarding_sub_agency_code", "awarding_sub_agency_name", "awarding_office_code", "awarding_office_name","funding_agency_code", "funding_agency_name", "funding_sub_agency_code", "funding_sub_agency_name", "funding_office_code", "funding_office_name","usaspending_permalink", "first_time_check", "last_time_check"]]
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_activity.csv', index=False, mode='a', header=header)

    header = False

### loading data to database

csv_path = '/var/rel8ed.to/nfs/share/usspending_data/usspending_activity.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'federal_awards_activity'
primary_key_columns = ['contract_award_unique_key']  # Composite primary key
update_columns = ["federal_action_obligation","total_dollars_obligated","total_outlayed_amount_for_overall_award","base_and_exercised_options_value","current_total_value_of_award","base_and_all_options_value","potential_total_value_of_award", "action_date","period_of_performance_start_date", "period_of_performance_current_end_date", "period_of_performance_potential_end_date", "ordering_period_end_date", "solicitation_date", "last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
        
        chunk.fillna('', inplace=True)
        chunk.loc[chunk['action_date']=='' , 'action_date'] = None
        chunk.loc[chunk['period_of_performance_start_date']=='' , 'period_of_performance_start_date'] = None
        chunk.loc[chunk['period_of_performance_current_end_date']=='' , 'period_of_performance_current_end_date'] = None
        chunk.loc[chunk['period_of_performance_potential_end_date']=='' , 'period_of_performance_potential_end_date'] = None
        chunk.loc[chunk['ordering_period_end_date']=='' , 'ordering_period_end_date'] = None
        chunk.loc[chunk['solicitation_date']=='' , 'solicitation_date'] = None
        chunk.loc[chunk['first_time_check']=='' , 'first_time_check'] = file_date
        chunk.loc[chunk['last_time_check']=='' , 'last_time_check'] = file_date
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

### processing matching
cols = ['recipient_uei', 'recipient_duns']

header = True

for f in files:
    df = pd.read_csv('/var/rel8ed.to/nfs/share/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_uei'].notnull()]
    df = df[df['recipient_duns'].notnull()]
    df['identifier_y'] = df['recipient_uei'].apply(lambda x: identifier_uuid(str(x)+'_UEI'))
    df['identifier_x'] = df['recipient_duns'].apply(lambda x: identifier_uuid(str(x)+'DNB'))
    df['name_score'] = 1
    df['address_score'] = 1
    df['city_score'] = 1
    df['postcode_score'] = 1
    df['meta_score'] = 1
    df['version'] = 'source_v1'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier_x', 'identifier_y', 'name_score', 'address_score', 'city_score', 'postcode_score', 'meta_score', 'version', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/var/rel8ed.to/nfs/share/usspending_data/usspending_match.csv', index=False, mode='a', header=header)

    header = False

### loading data to database

csv_path = '/var/rel8ed.to/nfs/share/usspending_data/usspending_match.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consolidated_match'
primary_key_columns = ['identifier_x', 'identifier_y', 'version']  # Composite primary key
update_columns = ['name_score','address_score','city_score','postcode_score','meta_score','last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
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