import os
import pandas as pd
from datetime import datetime
from uuid import uuid5, UUID
from cleanco import typesources, matches
classification_sources = typesources()

today = datetime.today().strftime('%Y-%m-%d')

files = os.listdir('/home/rli/dnb_data/')
length = len(files)

def identifier_uuid(text):
    namespace = UUID("00000000-0000-0000-0000-000000000000")
    uuid = uuid5(namespace, text)
    return uuid


def process_file(file_path):
    # Read the file
    with open(file_path, 'r') as file:
        content = file.read()

    # Modify the content
    modified_content = content.replace('\\,', '').replace('\\"', '').replace('\\', '').replace('"', ' ').replace(',',' ')

    # Write the modified content back to the file
    with open(file_path, 'w') as file:
        file.write(modified_content)


fl_files = [fl for fl in files if fl.endswith('FL')]
for fl in fl_files:
    file_path = os.path.join('/home/rli/dnb_data/', fl)
    process_file(file_path)
    print(file_path)

header = True
# Read the CSV file
for file in fl_files:
    file_path = os.path.join('/home/rli/dnb_data/', file)
    print(file)
    data = pd.read_csv(file_path, header=None, sep='|', dtype='str', encoding='utf-8')
    data.columns = ['identifier', 'name', 'address', 'city', 'state', 'postal', 'alt_name', 'country', 'phone', 'location_status', 'identifier_hq']
    data['uuid'] = data['identifier'].apply(lambda x: identifier_uuid(x+'DNB'))
    data['uuid_hq'] = data['identifier_hq'].apply(lambda x: identifier_uuid(x+'DNB'))
    data['legal_type'] = data['name'].apply(lambda x : matches(str(x), classification_sources)[0] if matches(str(x), classification_sources) != [] else '')
    data['first_time_check'] = today
    data.to_csv('/home/rli/dnb_data/dnb.csv', index=False, mode='a', header=header)
    header = False
    length -= 1
    print(length, 'files remaining')
    # os.remove(file_path)

print("CSV file created")