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

today = datetime.today()

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

def country_alpha2_to_alpha3(alpha2_code):
    try:
        country = pycountry.countries.get(alpha_2=alpha2_code)
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

raw_data_dir = "/var/rel8ed.to/nfs/share/uspto/trademark_application"

# Initialize an empty list to store the last 7 days' dates
last_seven_days_list = []

# Get today's date
today = datetime.today()

# Generate the last 7 days' dates 249
last_seven_days_list = [(today - timedelta(days=i)).strftime('%y%m%d') for i in range(2,249)]



# Download the patent data for the last 7 days and extract them
for file_date in last_seven_days_list:
    url = "https://bulkdata.uspto.gov/data/trademark/dailyxml/applications/apc" + file_date + ".zip"
    file_name = "apc" + file_date + ".zip"
    file_path = os.path.join(raw_data_dir, file_name)
    if not os.path.exists(file_path):
        print("downloading file: " + file_name)
        urllib.request.urlretrieve(url, file_path)
        sleep(0.5)
    else:
        print("file already exists: " + file_name)


for file_date in last_seven_days_list:
    file_name = "apc" + file_date + ".zip"
    file_path = os.path.join(raw_data_dir, file_name)
    if not os.path.exists(file_path.replace('.zip', '.xml')): 
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            print("unzipping file: " + file_name)
            zip_ref.extractall(raw_data_dir)
    else:
        print("file already unzipped: " + file_name)

if os.path.exists("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_application.csv"):
    os.remove("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_application.csv")


header = True
# for file_date in last_seven_days_list:
for file_date in last_seven_days_list:
    file_name = "apc" + file_date + ".xml"
    file_path = os.path.join(raw_data_dir, file_name)
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            print("processing file: " + file_name)
            data = f.read()
            bs_data = BeautifulSoup(data, "xml")
            corp = bs_data.find_all("case-file")
            print("the length of file is: ", len(corp))

            if len(corp) == 0:
                continue
            else:
                rows = []

                for i in corp:

                    serial_number = get_text_or_empty(i, "serial-number").strip()
                    registration_number = get_text_or_empty(i, "registration-number").strip()
                    transaction_date = get_text_or_empty(i, "transaction-date").strip()
                    case_file = i.find_all("case-file-header")[0]
                    filing_date = get_text_or_empty(case_file, 'filing-date')
                    status_code = get_text_or_empty(case_file, 'status-code')
                    status_date = get_text_or_empty(case_file, 'status-date')
                    mark_identification = get_text_or_empty(case_file, 'mark-identification')

                    for ii in i.find_all("case-file-statement"):
                        type_code = get_text_or_empty(ii, "type-code").strip()
                        if type_code == "GS0181":
                            type_text = get_text_or_empty(ii, "text").strip()


                    for ii in i.find_all("classification"):
                        international_codes = ';'.join([tag.text for tag in ii.find_all("international-code")])
                        us_codes = ';'.join([tag.text for tag in ii.find_all("us-code")])
                    
                    for ii in i.find_all("case-file-owner"):
                        legal_entity_type_code = get_text_or_empty(ii, "legal-entity-type-code").strip()
                        party_name = get_text_or_empty(ii, "party-name").strip()
                        address_1 = get_text_or_empty(ii, "address-1").strip()
                        address_2 = get_text_or_empty(ii, "address-2").strip()
                        city = get_text_or_empty(ii, "city").strip()
                        country = get_text_or_empty(ii, "country").strip()
                        postcode = get_text_or_empty(ii, "postcode").strip()

                        row = [serial_number, registration_number, transaction_date, filing_date, status_code, status_date, mark_identification, type_text, international_codes, us_codes, legal_entity_type_code, party_name, address_1, address_2, city, country, postcode]
                        rows.append(row)
                
                df = pd.DataFrame(rows, columns=['serial_number', 'registration_number', 'transaction_date', 'filing_date', 'status_code', 'status_date', 'mark_identification', 'type_text', 'international_codes', 'us_codes', 'legal_entity_type_code', 'party_name', 'address_1', 'address_2', 'city', 'country', 'postcode'])
                df.to_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_application.csv", index=False, mode="a", header=header)

                header = False

### process
status_map = {'000':'Indifferent',
'400':'Dead/Cancelled',
'401':'Dead/Cancelled',
'402':'Dead/Abandoned',
'403':'Dead/Cancelled',
'404':'Dead/Cancelled',
'405':'Dead/Cancelled',
'406':'Dead/Cancelled',
'410':'Live/Pending',
'411':'Dead/Abandoned',
'412':'Dead/Abandoned',
'413':'Live/Pending',
'414':'Dead/Cancelled',
'415':'Dead/Cancelled',
'416':'Dead/Abandoned',
'417':'Dead/Cancelled',
'600':'Dead/Abandoned',
'601':'Dead/Abandoned',
'602':'Dead/Abandoned',
'603':'Dead/Abandoned',
'604':'Dead/Abandoned',
'605':'Dead/Abandoned',
'606':'Dead/Abandoned',
'607':'Dead/Abandoned',
'608':'Dead/Abandoned',
'609':'Dead/Abandoned',
'610':'Dead',
'612':'Dead/Abandoned',
'614':'Dead/Abandoned',
'616':'Live/Pending',
'618':'Dead/Abandoned',
'620':'Live/Pending',
'622':'Indifferent',
'624':'Live/Registered',
'625':'Live/Registered',
'626':'Dead/Cancelled',
'630':'Live/Pending',
'631':'Live/Pending',
'632':'Dead',
'638':'Live/Pending',
'640':'Live/Pending',
'641':'Live/Pending',
'642':'Live/Pending',
'643':'Live/Pending',
'644':'Live/Pending',
'645':'Live/Pending',
'646':'Live/Pending',
'647':'Live/Pending',
'648':'Live/Pending',
'649':'Live/Pending',
'650':'Live/Pending',
'651':'Live/Pending',
'652':'Live/Pending',
'653':'Live/Pending',
'654':'Live/Pending',
'655':'Live/Pending',
'656':'Live/Pending',
'657':'Live/Pending',
'658':'Live/Pending',
'659':'Live/Pending',
'660':'Live/Pending',
'661':'Live/Pending',
'663':'Live/Pending',
'664':'Live/Pending',
'665':'Live/Pending',
'666':'Live/Pending',
'667':'Live/Pending',
'668':'Live/Pending',
'672':'Live/Pending',
'680':'Live/Pending',
'681':'Live/Pending',
'682':'Live/Pending',
'686':'Live/Pending',
'688':'Live/Pending',
'689':'Live/Pending',
'690':'Live/Pending',
'692':'Live/Pending',
'693':'Live/Pending',
'694':'Live/Pending',
'700':'Live/Registered',
'701':'Live/Registered',
'702':'Live/Registered',
'703':'Live/Registered',
'704':'Live/Registered',
'705':'Live/Registered',
'706':'Live/Registered',
'707':'Live/Registered',
'708':'Live/Registered',
'709':'Dead/Cancelled',
'710':'Dead/Cancelled',
'711':'Dead/Cancelled',
'712':'Dead/Cancelled',
'713':'Dead/Cancelled',
'714':'Dead/Cancelled',
'715':'Indifferent/Cancelled',
'717':'Live',
'718':'Live/Pending',
'719':'Live/Pending',
'720':'Live/Pending',
'721':'Live/Pending',
'722':'Live/Pending',
'724':'Live/Pending',
'725':'Live/Pending',
'730':'Live/Pending',
'731':'Live/Pending',
'732':'Live/Pending',
'733':'Live/Pending',
'734':'Live/Pending',
'739':'Live/Registered',
'740':'Live/Pending',
'744':'Live/Pending',
'745':'Live/Pending',
'746':'Live/Pending',
'748':'Live/Pending',
'752':'Live/Pending',
'753':'Live/Pending',
'756':'Live/Pending',
'757':'Live/Pending',
'760':'Live/Pending',
'762':'Live/Pending',
'763':'Live/Pending',
'764':'Live/Pending',
'765':'Live/Pending',
'766':'Live/Pending',
'771':'Live/Pending',
'772':'Live/Pending',
'773':'Live/Pending',
'774':'Live/Pending',
'775':'Live/Pending',
'777':'Live/Pending',
'778':'Live/Registered',
'779':'Live/Pending',
'780':'Live/Registered',
'781':'Dead/Cancelled',
'782':'Dead/Cancelled',
'790':'Live/Registered',
'794':'Live/Pending',
'800':'Live/Registered',
'801':'Live/Pending',
'802':'Live/Pending',
'803':'Live/Pending',
'804':'Live/Pending',
'806':'Live/Pending',
'807':'Live/Pending',
'808':'Live/Pending',
'809':'Live/Pending',
'810':'Live/Pending',
'811':'Live/Pending',
'812':'Live/Pending',
'813':'Live/Pending',
'814':'Live/Pending',
'815':'Live/Pending',
'816':'Live/Pending',
'817':'Live/Pending',
'818':'Live/Pending',
'819':'Live/Pending',
'820':'Live/Pending',
'821':'Live/Pending',
'822':'Live/Pending',
'823':'Live/Pending',
'824':'Live/Pending',
'825':'Live/Pending',
'900':'Dead/Expired',
'901':'Dead',
'968':'Dead',
'969':'Live/Pending',
'970':'Indifferent',
'973':'Live/Pending'}


df = pd.read_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_application.csv", dtype='str')
df.fillna('', inplace=True)
df['status_code'] = df['status_code'].map(status_map)
df['country'] = df['country'].apply(lambda x : country_alpha2_to_alpha3(x))
df['address'] = df[['address_1', 'address_2']].apply(lambda x : ' '.join(x).strip(), axis=1)
df['address'] = df['address'].apply(lambda x : re.sub('\s+',' ',x).strip())
df['address_en'] = df['address'].apply(lambda x : parse_address(x))
df['address_en'] = df['address_en'].apply(lambda x : [(value, label) for value, label in x if label in ['house_number', 'road', 'unit',  'po_box']] if x != '' else '')
df['address_en'] = df['address_en'].apply(lambda x : {addr_type: addr_value for addr_value, addr_type in x})
df['address_en'] = df['address_en'].apply(lambda x : ' '.join(x.values()))
df['address_en'] = df['address_en'].apply(lambda x : clean_string(x))

df['city_en'] = df['city'].apply(lambda x : parse_address(x))
df['region_code'] = df['city_en'].apply(lambda x : [(value, label) for value, label in x if label in ['state']] if x != '' else '')
df['region_code'] = df['region_code'].apply(lambda x : {addr_type: addr_value for addr_value, addr_type in x})
df['region_code'] = df['region_code'].apply(lambda x : ' '.join(x.values()))
df['region_code'] = df['region_code'].apply(lambda x : clean_string(x))
df['city_en'] = df['city_en'].apply(lambda x : [(value, label) for value, label in x if label in ['city']] if x != '' else '')
df['city_en'] = df['city_en'].apply(lambda x : {addr_type: addr_value for addr_value, addr_type in x})
df['city_en'] = df['city_en'].apply(lambda x : ' '.join(x.values()))
df['city_en'] = df['city_en'].apply(lambda x : clean_string(x))

df['business_name'] = df['party_name'].apply(lambda x : re.sub(r'\s+',' ',x).strip())
df['business_name_en'] = df['business_name'].apply(lambda x : basename(x))
df['business_name_en'] = df['business_name_en'].apply(lambda x : clean_string(x))

df['identifier'] = df.apply(lambda row: identifier_uuid(row['business_name_en'], row['address_en'], row['city_en'], row['region_code']), axis=1)

df['transaction_date'] = df['transaction_date'].apply(lambda x : convert_to_unified_format(str(x)))
df['filing_date'] = df['filing_date'].apply(lambda x : convert_to_unified_format(str(x)))
df['status_date'] = df['status_date'].apply(lambda x : convert_to_unified_format(str(x)))

df.rename(columns={'postcode':'postal', 'mark_identification':'trademark', 'type_text':'description'}, inplace=True)

df.to_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_application_sql.csv", index=False)

# load data to database

csv_path = '/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_application_sql.csv'

chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1


# Specify the table and the primary key columns
table_name = "trademark"
primary_key_columns = [
    "serial_number",
    "identifier",
]  # Composite primary key
update_columns = ['registration_number', 'transaction_date', 'filing_date', 'status_code', 'status_date', 'trademark', 'description', 'international_codes', 'us_codes', 'trademark_url']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing trademark") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=['serial_number', 'registration_number', 'transaction_date', 'filing_date', 'status_code', 'status_date', 'trademark', 'description', 'international_codes', 'us_codes', 'identifier', 'business_name', 'business_name_en', 'address', 'city_en', 'region_code', 'postal', 'country'],
        ),
        desc="Processing trademark",
    ):
        chunk = chunk.copy()
        chunk = chunk[~chunk['serial_number'].isna()]
        chunk = chunk[~chunk['identifier'].isna()]
        chunk = chunk[~chunk['business_name_en'].isna()]
        chunk.rename(columns={'city_en':'city', 'region_code':'state'}, inplace=True)
        chunk.fillna('', inplace=True)
        chunk['trademark_url'] = 'https://tsdr.uspto.gov/#caseNumber='+chunk['serial_number']+'&caseSearchType=US_APPLICATION&caseType=DEFAULT&searchType=statusSearch'
        chunk = chunk[
            [
                "serial_number",
                "registration_number",
                "transaction_date",
                "filing_date",
                "status_code",
                "status_date",
                "trademark",
                "description",
                "international_codes",
                "us_codes",
                "identifier",
                "business_name",
                "address",
                "city",
                "state",
                "postal",
                "country",
                "trademark_url"
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
table_name = "consolidated_location"
primary_key_columns = [
    "identifier",
    "address",
    "city",
    "state",
]  # Composite primary key
update_columns = ["last_time_check"]  # Columns to update in case of conflict


used_columns = ["identifier", "address", "city_en", "region_code", "postal", "country", 'filing_date', 'transaction_date']


with tqdm(total=total_chunks, desc="Processing location chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns,
        ),
        desc="Processing location chunks",
    ):
        chunk = chunk.copy()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk.fillna('', inplace=True)
        chunk.rename(columns={'region_code': 'state', 'city_en':'city', 'filing_date':'first_time_check', 'transaction_date':'last_time_check'}, inplace=True)
        chunk["latitude"] = None
        chunk["longitude"] = None
        chunk['location_type'] = 'principal'
        chunk['location_status'] = None
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



# Specify the table and the primary key columns
table_name = "consolidated_name"
primary_key_columns = [
    "identifier",
    "business_name",
]  # Composite primary key
update_columns = ["last_time_check"] 

used_columns = ["identifier", "business_name", "business_name_en", 'filing_date', 'transaction_date']

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
            usecols=used_columns,
        ),
        desc="Processing name chunks",
    ):
        chunk = chunk.copy()
        chunk.fillna('', inplace=True)
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        chunk = chunk[chunk['business_name_en'] != ""]
        chunk['start_date'] = None
        chunk['end_date'] = None
        chunk['name_type'] = 'main'
        chunk.rename(columns={'filing_date':'first_time_check', 'transaction_date':'last_time_check'}, inplace=True)
        # Additional processing here
        chunk.replace('', None, inplace=True)  # Convert empty strings back to NaN
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