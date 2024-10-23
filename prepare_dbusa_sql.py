import pandas as pd
import os
from tqdm import tqdm
import subprocess
from datetime import datetime
import sys
from uuid import uuid5, UUID

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


def identifier_uuid(text):
    namespace = UUID("00000000-0000-0000-0000-000000000000")
    uuid = uuid5(namespace, text)
    return uuid

# Process name

cols = ['DBUSA_Business_ID','Company_Name']

header = True

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
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
        chunk['name_type'] = 'main'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Company_Name':'business_name'},inplace=True)
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_name.csv', index=False, header=header, mode='a')
        header = False
        pbar.update()


# Process secondary name

cols = ['DBUSA_Business_ID','Company_Name_Secondary']

with tqdm(total=total_chunks, desc="Processing name chunks") as pbar:
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
        chunk = chunk[chunk['Company_Name_Secondary'].notnull()]
        chunk['name_type'] = 'secondary'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Company_Name_Secondary':'business_name'},inplace=True)
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_name.csv', index=False, header=header, mode='a')

        pbar.update()


# Process address name

cols = ['DBUSA_Business_ID','Physical_Address','Physical_Address_City','Physical_Address_State','Physical_Address_Zip','Physical_Address_Zip4','Latitude','Longitude','Business_Status_Description']

header = True

with tqdm(total=total_chunks, desc="Processing address chunks") as pbar:
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
        chunk = chunk[chunk['Physical_Address'].notnull()]
        chunk['postal'] = chunk.apply(lambda x: '-'.join([str(i) for i in [x['Physical_Address_Zip'], x['Physical_Address_Zip4']] if pd.notna(i)]), axis=1)
        chunk['location_type'] = 'primary'
        chunk['country'] = 'USA'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Physical_Address':'address', 'Physical_Address_City':'city', 'Physical_Address_State':'state', 'Latitude':'latitude', 'Longitude':'longitude', 'Business_Status_Description':'location_status'},inplace=True)
        chunk = chunk[['identifier','address','city','state','postal','country','latitude','longitude','location_type','location_status','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_address.csv', index=False, header=header, mode='a')
        header = False
        pbar.update()


# Process mailing address

state_abbrev = {'AK':'USA', 'AL':'USA', 'AR':'USA', 'AS':'USA', 'AZ':'USA', 'CA':'USA', 'CO':'USA', 'CT':'USA', 'DC':'USA', 'DE':'USA', 'FL':'USA', 'GA':'USA', 'GU':'USA', 'HI':'USA', 'IA':'USA', 'ID':'USA', 'IL':'USA', 'IN':'USA', 'KS':'USA', 'KY':'USA', 'LA':'USA', 'MA':'USA', 'MD':'USA', 'ME':'USA', 'MI':'USA', 'MN':'USA', 'MO':'USA', 'MP':'USA', 'MS':'USA', 'MT':'USA', 'NC':'USA', 'ND':'USA', 'NE':'USA', 'NH':'USA', 'NJ':'USA', 'NM':'USA', 'NV':'USA', 'NY':'USA', 'OH':'USA', 'OK':'USA', 'OR':'USA', 'PA':'USA', 'PR':'USA', 'RI':'USA', 'SC':'USA', 'SD':'USA', 'TN':'USA', 'TX':'USA', 'UM':'USA', 'UT':'USA', 'VA':'USA', 'VI':'USA', 'VT':'USA', 'WA':'USA', 'WI':'USA', 'WV':'USA', 'WY':'USA', 'NL':'CAN', 'PE':'CAN', 'NS':'CAN', 'NB':'CAN', 'QC':'CAN', 'ON':'CAN', 'MB':'CAN', 'SK':'CAN', 'AB':'CAN', 'BC':'CAN', 'YT':'CAN', 'NT':'CAN', 'NU':'CAN'}

cols = ['DBUSA_Business_ID','Mailing_Address','Mailing_Address_City','Mailing_Address_State','Mailing_Address_Zip','Mailing_Address_Zip4']


with tqdm(total=total_chunks, desc="Processing address chunks") as pbar:
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
        chunk = chunk[chunk['Mailing_Address'].notnull()]
        chunk['postal'] = chunk.apply(lambda x: '-'.join([str(i) for i in [x['Mailing_Address_Zip'], x['Mailing_Address_Zip4']] if pd.notna(i)]), axis=1)
        chunk['location_type'] = 'mailing'
        chunk['country'] = chunk['Mailing_Address_State'].map(state_abbrev)
        chunk['latitude'] = ''
        chunk['longitude'] = ''
        chunk['location_status'] = ''
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Mailing_Address':'address', 'Mailing_Address_City':'city', 'Mailing_Address_State':'state'},inplace=True)
        chunk = chunk[['identifier','address','city','state','postal','country','latitude','longitude','location_type','location_status','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_address.csv', index=False, header=header, mode='a')

        pbar.update()


# Process phone

cols = ['DBUSA_Business_ID','Phone']

header = True

with tqdm(total=total_chunks, desc="Processing phone chunks") as pbar:
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
        chunk = chunk[chunk['Phone'].notnull()]
        chunk['phone_type'] = 'primary'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Phone':'phone'},inplace=True)
        chunk = chunk[['identifier','phone','phone_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_phone.csv', index=False, header=header, mode='a')
        header = False
        pbar.update()


# Process secondary phone

cols = ['DBUSA_Business_ID','Phone_Secondary']

with tqdm(total=total_chunks, desc="Processing phone chunks") as pbar:
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
        chunk = chunk[chunk['Phone_Secondary'].notnull()]
        chunk['phone_type'] = 'secondary'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Phone_Secondary':'phone'},inplace=True)
        chunk = chunk[['identifier','phone','phone_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_phone.csv', index=False, header=header, mode='a')

        pbar.update()

# Process toll phone

cols = ['DBUSA_Business_ID','Toll_Free']

with tqdm(total=total_chunks, desc="Processing phone chunks") as pbar:
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
        chunk = chunk[chunk['Toll_Free'].notnull()]
        chunk['phone_type'] = 'toll free'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Toll_Free':'phone'},inplace=True)
        chunk = chunk[['identifier','phone','phone_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_phone.csv', index=False, header=header, mode='a')

        pbar.update()


# Process fax phone

cols = ['DBUSA_Business_ID','Fax']

with tqdm(total=total_chunks, desc="Processing fax chunks") as pbar:
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
        chunk = chunk[chunk['Fax'].notnull()]
        chunk['phone_type'] = 'fax'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Fax':'phone'},inplace=True)
        chunk = chunk[['identifier','phone','phone_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_phone.csv', index=False, header=header, mode='a')

        pbar.update()


# Process email

cols = ['DBUSA_Business_ID','Email']

header = True

with tqdm(total=total_chunks, desc="Processing email chunks") as pbar:
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
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Email':'email'},inplace=True)
        chunk = chunk[['identifier','email','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_email.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()



# Process website

cols = ['DBUSA_Business_ID','URL']

header = True

with tqdm(total=total_chunks, desc="Processing website chunks") as pbar:
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
        chunk = chunk[chunk['URL'].notnull()]
        hunk = chunk.map(lambda x: x.replace('www.','').strip() if isinstance(x, str) else x)
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','URL':'url'},inplace=True)
        chunk = chunk[['identifier','url','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_url.csv', index=False, header=header, mode='a')

        header = False

        pbar.update()


# Process social

cols = ['DBUSA_Business_ID','URL_Facebook']

header = True

with tqdm(total=total_chunks, desc="Processing facebook chunks") as pbar:
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
        chunk = chunk[chunk['URL_Facebook'].notnull()]
        chunk['url_type'] = 'facebook'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','URL_Facebook':'url'},inplace=True)
        chunk = chunk[['identifier','url','url_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_social.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()


# Process social

cols = ['DBUSA_Business_ID','URL_Yelp']

with tqdm(total=total_chunks, desc="Processing yelp chunks") as pbar:
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
        chunk = chunk[chunk['URL_Yelp'].notnull()]
        chunk['url_type'] = 'yelp'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','URL_Yelp':'url'},inplace=True)
        chunk = chunk[['identifier','url','url_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_social.csv', index=False, header=header, mode='a')

        pbar.update()


# Process social

cols = ['DBUSA_Business_ID','URL_Instagram']

with tqdm(total=total_chunks, desc="Processing instagram chunks") as pbar:
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
        chunk = chunk[chunk['URL_Instagram'].notnull()]
        chunk['url_type'] = 'instagram'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','URL_Instagram':'url'},inplace=True)
        chunk = chunk[['identifier','url','url_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_social.csv', index=False, header=header, mode='a')

        pbar.update()


# Process social

cols = ['DBUSA_Business_ID','URL_LinkedIN']

with tqdm(total=total_chunks, desc="Processing linkedin chunks") as pbar:
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
        chunk = chunk[chunk['URL_LinkedIN'].notnull()]
        chunk['url_type'] = 'linkedin'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','URL_LinkedIN':'url'},inplace=True)
        chunk = chunk[['identifier','url','url_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_social.csv', index=False, header=header, mode='a')

        pbar.update()

# Process social

cols = ['DBUSA_Business_ID','URL_Twitter']

with tqdm(total=total_chunks, desc="Processing twitter chunks") as pbar:
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
        chunk = chunk[chunk['URL_Twitter'].notnull()]
        chunk['url_type'] = 'twitter'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','URL_Twitter':'url'},inplace=True)
        chunk = chunk[['identifier','url','url_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_social.csv', index=False, header=header, mode='a')

        pbar.update()


# Process social

cols = ['DBUSA_Business_ID','URL_YouTube']

with tqdm(total=total_chunks, desc="Processing youtube chunks") as pbar:
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
        chunk = chunk[chunk['URL_YouTube'].notnull()]
        chunk['url_type'] = 'youtube'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','URL_YouTube':'url'},inplace=True)
        chunk = chunk[['identifier','url','url_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_social.csv', index=False, header=header, mode='a')

        pbar.update()


# Process identifier

cols = ['SubHQ_ID','HQ_ID']

header = True

with tqdm(total=total_chunks, desc="Processing hq chunks") as pbar:
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
        chunk = chunk[chunk['SubHQ_ID'].notnull()]
        chunk = chunk[chunk['HQ_ID'].notnull()]
        chunk['authority'] = 'DBUSA'
        chunk['hq_authority'] = 'DBUSA'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'SubHQ_ID':'identifier','HQ_ID':'identifier_hq'},inplace=True)
        chunk = chunk[['identifier','identifier_hq','authority','hq_authority','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk['identifier_hq'] = chunk['identifier_hq'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_identifier.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()


cols = ['DBUSA_Business_ID','HQ_ID']

with tqdm(total=total_chunks, desc="Processing hq chunks") as pbar:
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
        chunk = chunk[chunk['HQ_ID'].notnull()]
        chunk['authority'] = 'DBUSA'
        chunk['hq_authority'] = 'DBUSA'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','HQ_ID':'identifier_hq'},inplace=True)
        chunk = chunk[['identifier','identifier_hq','authority','hq_authority','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk['identifier_hq'] = chunk['identifier_hq'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_identifier.csv', index=False, header=header, mode='a')

        pbar.update()




### process alternate identifier

cols = ['DBUSA_Business_ID','EIN']

header = True

with tqdm(total=total_chunks, desc="Processing ein chunks") as pbar:
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
        chunk = chunk[chunk['EIN'].notnull()]
        chunk['alternative_authority'] = 'Employer Identification Number'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','EIN':'alternative_identifier'},inplace=True)
        chunk = chunk[['identifier','alternative_identifier','alternative_authority','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_alternative_identifier.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()


cols = ['DBUSA_Business_ID','Ticker_Symbol', 'Stock_Exchange']

with tqdm(total=total_chunks, desc="Processing stock chunks") as pbar:
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
        chunk = chunk[chunk['Stock_Exchange'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Ticker_Symbol':'alternative_identifier','Stock_Exchange':'alternative_authority'},inplace=True)
        chunk = chunk[['identifier','alternative_identifier','alternative_authority','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_alternative_identifier.csv', index=False, header=header, mode='a')

        pbar.update()


# Process person

cols = ['DBUSA_Business_ID','Full_Name','Gender','Source_Title']
genders_map = {'0':'M', 'M':'M', '2':'F', 'F':'F', '3':'U', 'U':'U'}

header = True

with tqdm(total=total_chunks, desc="Processing person chunks") as pbar:
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
        chunk = chunk[chunk['Full_Name'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Full_Name':'person_name','Gender':'gender','Source_Title':'title'},inplace=True)
        chunk['gender'] = chunk['gender'].apply(lambda x: genders_map.get(x, 'U'))
        chunk = chunk[['identifier','person_name','gender','title','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_person.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()

# Process category

cols = ['DBUSA_Business_ID','Primary_SIC_4Digit_Code','Primary_SIC_4Digit_Description']

header = True

with tqdm(total=total_chunks, desc="Processing sic chunks") as pbar:
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
        chunk = chunk[chunk['Primary_SIC_4Digit_Code'].notnull()]
        chunk['category_type'] = 'SIC'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Primary_SIC_4Digit_Code':'category_code','Primary_SIC_4Digit_Description':'category_name'},inplace=True)
        chunk = chunk[['identifier','category_code','category_name','category_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_category.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()


cols = ['DBUSA_Business_ID','NAICS01_Code','NAICS01_Description']

with tqdm(total=total_chunks, desc="Processing naics chunks") as pbar:
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
        chunk = chunk[chunk['NAICS01_Code'].notnull()]
        chunk['category_type'] = 'NAICS'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','NAICS01_Code':'category_code','NAICS01_Description':'category_name'},inplace=True)
        chunk = chunk[['identifier','category_code','category_name','category_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_category.csv', index=False, header=header, mode='a')

        pbar.update()


# Process sales

cols = ['DBUSA_Business_ID','Location_Sales_Total','Location_Sales_Description']

header = True

with tqdm(total=total_chunks, desc="Processing sales chunks") as pbar:
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
        chunk = chunk[chunk['Location_Sales_Total'].notnull()]
        chunk['currency'] = 'USD'
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Location_Sales_Total':'count','Location_Sales_Description':'description'},inplace=True)
        chunk = chunk[['identifier','count','description','currency','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_sales.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()


# Process employee

cols = ['DBUSA_Business_ID','Location_Employee_Count','Location_Employee_Description']

header = True

with tqdm(total=total_chunks, desc="Processing employee chunks") as pbar:
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
        chunk = chunk[chunk['Location_Employee_Count'].notnull()]
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Location_Employee_Count':'count','Location_Employee_Description':'description'},inplace=True)
        chunk = chunk[['identifier','count','description','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_emp.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()


# Process designation

cols = ['DBUSA_Business_ID','Female_Owned_Indicator']

header = True

with tqdm(total=total_chunks, desc="Processing female owned chunks") as pbar:
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
        chunk = chunk[chunk['Female_Owned_Indicator'].notnull()]
        chunk['designation'] = chunk['Female_Owned_Indicator'].apply(lambda x : 'female owned' if x == '1' else '')
        chunk = chunk[chunk['designation']!='']
        chunk['designation_type'] = None
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier'},inplace=True)
        chunk = chunk[['identifier','designation','designation_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_designation.csv', index=False, header=header, mode='a')
        header = False

        pbar.update()



cols = ['DBUSA_Business_ID','Home_Based_Business_Indicator']

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
        chunk = chunk[chunk['Home_Based_Business_Indicator'].notnull()]
        chunk['designation'] = chunk['Home_Based_Business_Indicator'].apply(lambda x : 'home based business' if x == '1' else '')
        chunk = chunk[chunk['designation']!='']
        chunk['designation_type'] = None
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier'},inplace=True)
        chunk = chunk[['identifier','designation','designation_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_designation.csv', index=False, header=header, mode='a')

        pbar.update()



cols = ['DBUSA_Business_ID','Small_Business_Indicator']

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
        chunk = chunk[chunk['Small_Business_Indicator'].notnull()]
        chunk['designation'] = chunk['Small_Business_Indicator'].apply(lambda x : 'small business' if x == '1' else '')
        chunk = chunk[chunk['designation']!='']
        chunk['designation_type'] = None
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier'},inplace=True)
        chunk = chunk[['identifier','designation','designation_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_designation.csv', index=False, header=header, mode='a')

        pbar.update()



cols = ['DBUSA_Business_ID','Manufacturing_Indicator']

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
        chunk = chunk[chunk['Manufacturing_Indicator'].notnull()]
        chunk['designation'] = chunk['Manufacturing_Indicator'].apply(lambda x : 'manufacturing' if x == '1' else '')
        chunk = chunk[chunk['designation']!='']
        chunk['designation_type'] = None
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier'},inplace=True)
        chunk = chunk[['identifier','designation','designation_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_designation.csv', index=False, header=header, mode='a')

        pbar.update()


cols = ['DBUSA_Business_ID','Public_Indicator']

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
        chunk = chunk[chunk['Public_Indicator'].notnull()]
        chunk['designation'] = chunk['Public_Indicator'].apply(lambda x : 'public family' if x == '1' else '')
        chunk = chunk[chunk['designation']!='']
        chunk['designation_type'] = None
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier'},inplace=True)
        chunk = chunk[['identifier','designation','designation_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_designation.csv', index=False, header=header, mode='a')

        pbar.update()


cols = ['DBUSA_Business_ID','Non_Profit_Indicator']

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
        chunk = chunk[chunk['Non_Profit_Indicator'].notnull()]
        chunk['designation'] = chunk['Non_Profit_Indicator'].apply(lambda x : 'non profit' if x == '1' else '')
        chunk = chunk[chunk['designation']!='']
        chunk['designation_type'] = None
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier'},inplace=True)
        chunk = chunk[['identifier','designation','designation_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_designation.csv', index=False, header=header, mode='a')

        pbar.update()


cols = ['DBUSA_Business_ID','Domestic_Foreign_Owner_Indicator']

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
        chunk = chunk[chunk['Domestic_Foreign_Owner_Indicator'].notnull()]
        chunk['designation'] = chunk['Domestic_Foreign_Owner_Indicator'].apply(lambda x : 'domestic foreign owner' if x == '1' else '')
        chunk = chunk[chunk['designation']!='']
        chunk['designation_type'] = None
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier'},inplace=True)
        chunk = chunk[['identifier','designation','designation_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_designation.csv', index=False, header=header, mode='a')

        pbar.update()


cols = ['DBUSA_Business_ID','Minority_Owned_Indicator', 'Minority_Type']

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
        chunk = chunk[chunk['Minority_Owned_Indicator'].notnull()]
        chunk['designation'] = chunk['Minority_Owned_Indicator'].apply(lambda x : 'minority owned' if x == '1' else '')
        chunk = chunk[chunk['designation']!='']
        chunk['first_time_check'] = file_date
        chunk['last_time_check'] = file_date
        chunk.rename(columns={'DBUSA_Business_ID':'identifier','Minority_Type':'designation_type'},inplace=True)
        chunk = chunk[['identifier','designation','designation_type','first_time_check','last_time_check']]
        chunk['identifier'] = chunk['identifier'].apply(lambda x: identifier_uuid(x))
        chunk.drop_duplicates(inplace=True)
        chunk.to_csv('/home/rli/dbusa_data/dbusa_designation.csv', index=False, header=header, mode='a')

        pbar.update()