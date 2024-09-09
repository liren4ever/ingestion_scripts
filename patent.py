from datetime import datetime, timedelta
import os
import zipfile
from bs4 import BeautifulSoup
import pandas as pd
import urllib.request
from postal.parser import parse_address
from dateutil.parser import parse
import pycountry
import us
from sqlalchemy import create_engine, text
from tqdm import tqdm
from time import sleep
import re
import string
import unicodedata
from cleanco import basename
from uuid import uuid5, UUID

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

def country_name_to_alpha3(country_name):
    try:
        country = pycountry.countries.get(name=country_name)
        return country.alpha_3
    except AttributeError:
        return None
    
def state_name_to_abbrev(state_name):
    state = us.states.lookup(state_name)
    if state:
        return state.abbr
    return state_name

def convert_to_unified_format(date_string):
    try:
        # Try to parse the date string
        parsed_date = parse(date_string)
        return parsed_date.strftime("%Y-%m-%d")  # Convert to the desired unified format
    except ValueError:
        return None

def get_text_or_empty(element, tag_name):
    try:
        return element.find_all(tag_name)[0].text
    except (IndexError, AttributeError):
        return ""

def clean_string(input_string):
    # Remove punctuation
    no_punct = input_string.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation))).strip()
    # Remove accent marks
    no_accents = ''.join(c for c in unicodedata.normalize('NFD', no_punct)
                        if unicodedata.category(c) != 'Mn')
    # Remove consecutive spaces
    no_consecutive_spaces = ' '.join(no_accents.split())
    # Convert to lowercase
    lowercase_string = no_consecutive_spaces.lower()
    result = re.sub(r'\s+',' ',lowercase_string)

    return result

def identifier_uuid(name, address, city, state):
    text = ';'.join([name.lower(), address.lower(), city.lower(), state.lower()])
    namespace = UUID("00000000-0000-0000-0000-000000000000")
    uuid = uuid5(namespace, text)
    return uuid
# Directory where the patent data will be stored

raw_data_dir = "/var/rel8ed.to/nfs/share/uspto/patent_folder"

# Initialize an empty list to store the last 7 days' dates
last_seven_days_list = []

# Get today's date
today = datetime.today()

# Generate the last 7 days' dates 249
last_seven_days_list = [(today - timedelta(days=i)).strftime('%Y%m%d') for i in range(2,3)]
# Download the patent data for the last 7 days and extract them

for file_date in last_seven_days_list:
    url = "https://bulkdata.uspto.gov/data/patent/assignment/ad" + file_date + ".zip"
    file_name = "ad" + file_date + ".zip"
    file_path = os.path.join(raw_data_dir, file_name)
    if not os.path.exists(file_path):
        print("downloading file: " + file_name)
        urllib.request.urlretrieve(url, file_path)
        sleep(0.5)
    else:
        print("file already exists: " + file_name)


for file_date in last_seven_days_list:
    file_name = "ad" + file_date + ".zip"
    file_path = os.path.join(raw_data_dir, file_name)    
    if not os.path.exists(file_path.replace('.zip', '.xml')):
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            print("unzipping file: " + file_name)
            zip_ref.extractall(raw_data_dir)
    else:
        print("file already unzipped: " + file_name)


# Delete the previously processed file
if os.path.exists("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignee.csv"):
    os.remove("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignee.csv")

if os.path.exists("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignment.csv"):
    os.remove("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignment.csv")

if os.path.exists("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignor.csv"):
    os.remove("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignor.csv")

# Process the extracted XML files for assignee
#['19800101-20231231-01','19800101-20231231-02', '19800101-20231231-03','19800101-20231231-04','19800101-20231231-05','19800101-20231231-06','19800101-20231231-07','19800101-20231231-08','19800101-20231231-09', '19800101-20231231-10','19800101-20231231-11','19800101-20231231-12','19800101-20231231-13','19800101-20231231-14','19800101-20231231-15','19800101-20231231-16','19800101-20231231-17','19800101-20231231-18','19800101-20231231-19','19800101-20231231-20']
header = True
# for file_date in last_seven_days_list:
for file_date in last_seven_days_list:
    file_name = "ad" + file_date + ".xml"
    file_path = os.path.join(raw_data_dir, file_name)
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            print("processing file: " + file_name)
            data = f.read()
            bs_data = BeautifulSoup(data, "xml")
            corp = bs_data.find_all("patent-assignment")
            if len(corp) == 0:
                print("skipping file: " + file_name)
                continue
            else:
                assignee_rows = []
                assignor_rows = []
                assignment_rows = []
                

                for i in corp:

                    assignment = i.find_all("assignment-record")[0]
                    reel_no = get_text_or_empty(assignment, "reel-no")
                    frame_no = get_text_or_empty(assignment, "frame-no")
                    update_date = get_text_or_empty(assignment, "last-update-date").strip()
                    recorded_date = get_text_or_empty(assignment, "recorded-date").strip()
                    patent_properties = i.find("patent-properties")
                    if patent_properties:
                        patent_property = patent_properties.find("patent-property")
                        if patent_property:
                            invention_title = patent_property.find("invention-title")
                            patent_title = invention_title.text if invention_title else ''
                        else:
                            patent_title = ''
                    else:
                        patent_title = ''

                    for ii in i.find_all("patent-assignees")[0].find_all("patent-assignee"):
                        assignee = get_text_or_empty(ii, "name")
                        street = get_text_or_empty(ii, "address-1")
                        street1 = get_text_or_empty(ii, "address-2")
                        city = get_text_or_empty(ii, "city")
                        province = get_text_or_empty(ii, "state")
                        postalCode = get_text_or_empty(ii, "postcode")

                    for ii in i.find("patent-properties").find("patent-property").find_all("document-id"):
                        location = ii.country.text
                        doc_number = ii.find('doc-number').text
                        kind = ii.kind.text
                        patent_date = ii.date.text if ii.date else ''

                    for ii in i.find_all("patent-assignors")[0].find_all("patent-assignor"):
                        assignor = get_text_or_empty(ii, "name")
                        execution_date = get_text_or_empty(ii, "execution-date").strip()             
                        
                        assignee_row = [reel_no, frame_no, update_date, recorded_date, assignee, street, street1, city, province, postalCode]
                        assignee_rows.append(assignee_row)

                        assignment_row = [reel_no, frame_no, patent_title, doc_number, kind, patent_date]
                        assignment_rows.append(assignment_row)

                        assignor_row = [reel_no, frame_no, update_date, recorded_date, assignor, execution_date]
                        assignor_rows.append(assignor_row)

                assignee_df  = pd.DataFrame(assignee_rows, columns=["reel_no","frame_no","update_date","recorded_date","name","street","street1","city","province","postalCode"])

                assignee_df.to_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignee.csv", index=False, mode="a", header=header)

                assignment_df  = pd.DataFrame(assignment_rows, columns=["reel_no","frame_no","patent_title","patent_doc_number","patent_kind","patent_date"])

                assignment_df.to_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignment.csv", index=False, mode="a", header=header)

                assignor_df  = pd.DataFrame(assignor_rows, columns=["reel_no","frame_no","update_date","recorded_date","assignor","execution_date"])

                assignor_df.to_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignor.csv", index=False, mode="a", header=header)

                header = False     
    else:
        continue

# Create File for SQl

df = pd.read_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignee.csv", dtype='str')

df['address'] = df.apply(lambda x : ', '.join([str(i) for i in [x['street'], x['street1'], x['city'],x['province'], x['postalCode']] if pd.notna(i)]), axis=1)

df.drop(columns=['street', 'street1', 'city', 'province', 'postalCode'], inplace=True)

df['address_en'] = df['address'].apply(lambda x : parse_address(x))

df['city'] = df['address_en'].apply(lambda x : {addr_type: addr_value for addr_value, addr_type in x})

df['state'] = df['city'].apply(lambda x : x['state'] if 'state' in x else '')

df['postal'] = df['city'].apply(lambda x : x['postcode'] if 'postcode' in x else '')

df['country'] = df['city'].apply(lambda x : x['country'] if 'country' in x else '')

df['city'] = df['city'].apply(lambda x : x['city'] if 'city' in x else '')

df['address_en'] = df['address_en'].apply(lambda x : [(value, label) for value, label in x if label in ['house_number', 'road', 'unit',  'po_box']])

df['address_en'] = df['address_en'].apply(lambda x : {addr_type: addr_value for addr_value, addr_type in x})
df['address'] = df['address_en'].apply(lambda x : ' '.join(x.values()))

df.drop(columns=['address_en'], inplace=True)

df['state'] = df['state'].apply(lambda x : state_name_to_abbrev(x))

df['country'] = df['country'].apply(lambda x : country_name_to_alpha3(x))

df['update_date'] = df['update_date'].apply(lambda x : convert_to_unified_format(str(x)))
df['recorded_date'] = df['recorded_date'].apply(lambda x : convert_to_unified_format(str(x)))

df['business_name'] = df['name'].apply(lambda x : re.sub(r'\s+',' ',str(x)).strip())
df['business_name_en'] = df['business_name'].apply(lambda x : basename(x))
df['business_name_en'] = df['business_name_en'].apply(lambda x : clean_string(x))

df.rename(columns={'name':'assignee'}, inplace=True)

df['identifier'] = df.apply(lambda row: identifier_uuid(row['business_name_en'], row['address'], row['city'], row['state']), axis=1)

df.to_csv('/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignee_sql.csv', index=False)


# Load into SQL

csv_path = '/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignee_sql.csv'

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
                "country",
                "identifier"
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
                "country",
                "identifier"
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


# Load into SQL

csv_path = '/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignment.csv'

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

### process assignor

csv_path = '/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignor.csv'

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
        chunk = chunk[chunk['assignor'].notna()]
        chunk = chunk[chunk['recorded_date'].notna()]
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


### process patent

cols = ['reel_no', 'frame_no', 'patent_title', 'patent_doc_number', 'patent_kind', 'patent_date']

df = pd.read_csv('/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent_assignment.csv', usecols=cols, dtype=str)
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
df.to_csv('/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent.csv', index=False)


### process patent

csv_path = '/var/rel8ed.to/nfs/share/uspto/kumiai_data/patent.csv'

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