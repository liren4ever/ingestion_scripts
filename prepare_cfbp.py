import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import subprocess
from datetime import datetime

file_date = datetime.today().strftime('%Y-%m-%d')

cols = ['Date received', 'Product', 'Sub-product', 'Issue', 'Sub-issue', 'Company public response', 'Company', 'State', 'ZIP code', 'Date sent to company', 'Company response to consumer', 'Timely response?', 'Consumer disputed?', 'Complaint ID']

df = pd.read_csv('/home/rli/cfbp_data/complaints.csv', dtype='str', usecols=cols)

cols_changes = {
    'Date received': 'date_received',
    'Product': 'product',
    'Sub-product': 'sub_product',
    'Issue': 'issue',
    'Sub-issue': 'sub_issue',
    'Company public response': 'company_public_response',
    'Company': 'business_name',
    'State': 'state',
    'ZIP code': 'postal',
    'Date sent to company': 'date_sent_to_company',
    'Company response to consumer': 'company_response',
    'Timely response?': 'timely_response',
    'Consumer disputed?': 'consumer_disputed',
    'Complaint ID': 'complaint_id'
}


df.rename(columns=cols_changes, inplace=True)
df['url'] = df['complaint_id'].apply(lambda x: 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/detail/' + x)
df['identifier'] = ''

df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
df = df.astype(str)
df.fillna('', inplace=True)
df['first_time_check'] = file_date
df['last_time_check'] = file_date


df.to_csv('/home/rli/cfbp_data/cfbp.csv', index=False)


####
connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)


### process name

csv_path = '/home/rli/cfbp_data/cfbp.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'consumer_complaint'
primary_key_columns = ['complaint_id']  # Composite primary key
update_columns = ['date_received', 'product', 'sub_product', 'issue', 'sub_issue', 'company_public_response', 'company_response', 'timely_response', 'consumer_disputed', 'last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.map(lambda x: x.strip() if isinstance(x, str) else x)
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