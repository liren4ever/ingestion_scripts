import os
import pandas as pd
from uuid import uuid5, UUID

destination_path = '/home/rli/expn_data/'

fl_files = os.listdir(destination_path)

fl_files = [fl for fl in fl_files if fl.endswith('TXT')]

def identifier_uuid(text):
    namespace = UUID("00000000-0000-0000-0000-000000000000")
    uuid = uuid5(namespace, text)
    return uuid

def process_file(file_path):
    # Read the file
    encodings = ['utf-8', 'latin-1', 'windows-1252']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                content = file.read()

        # Modify the content
            modified_content = content.replace('\\,', '').replace('\\"', '').replace('\\', '').replace(',',' ')

        # Write the modified content back to the file
            with open(file_path, 'w') as file:
                file.write(modified_content)
        except UnicodeDecodeError:
            continue


for fl in fl_files:
    file_path = os.path.join(destination_path, fl)
    process_file(file_path)
    print(file_path)



output_header = True

length = len(fl_files)

cols = ["BUSINESS NAME","EXPERIAN BUSINESS ID","PBIN (BRANCH BIN)","ADDRESS","CITY","STATE","ZIP CODE","ZIP PLUS 4","COUNTRY CODE","PHONE NUMBER","GEO CODE LATITUDE","GEO CODE LONGITUDE","LOCATION CODE","PRIMARY SIC CODE - 4 DIGIT (DMO013)","BUSINESS TYPE CODE (DMO003)","URL","COUNTRY CODE"]

cols_change = {
    "BUSINESS NAME":"business_name",
    "EXPERIAN BUSINESS ID":"identifier_hq",
    "PBIN (BRANCH BIN)":"identifier",
    "ADDRESS":"address",
    "CITY":"city",
    "STATE":"state",
    "ZIP CODE":"postal",
    "ZIP PLUS 4":"zip4",
    "PHONE NUMBER":"phone",
    "GEO CODE LATITUDE":"latitude",
    "GEO CODE LONGITUDE":"longitude",
    "LOCATION CODE":"location_status",
    "PRIMARY SIC CODE - 4 DIGIT (DMO013)":"category_code",
    "BUSINESS TYPE CODE (DMO003)":"legal_type",
    "URL":"url",
    "COUNTRY CODE":"country_code"
}

for fl in fl_files:
    file_path = os.path.join(destination_path, fl)
    df = pd.read_csv(file_path, sep='|', dtype='str', on_bad_lines='warn', usecols=cols)
    df.rename(columns=cols_change, inplace=True)
    df['uuid'] = df['identifier'].apply(lambda x: identifier_uuid(x+'EXPN'))
    df['uuid_hq'] = df['identifier_hq'].apply(lambda x: identifier_uuid(x+'EXPN'))
    output_path = os.path.join(destination_path, 'expn.csv')

    df.to_csv(output_path, index=False, mode='a', header=output_header)

    output_header = False

    length -= 1
    print(length, fl)