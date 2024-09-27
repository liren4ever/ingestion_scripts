import requests
from datetime import datetime
import os
import zipfile
import csv
import pandas as pd

# Get the current date
today = datetime.today()
# Format the URL with the current year and month
url = f"https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-{today.year}-{today.month:02d}-01.zip"
# Define the local filename to save the downloaded file
local_filename = f"BasicCompanyDataAsOneFile-{today.year}-{today.month:02d}-01.zip"
local_path = os.path.join('/home/rli/uk_data', local_filename)

# Download the file
response = requests.get(url, stream=True)
if response.status_code == 200:
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded: {local_filename}")
else:
    print(f"Failed to download file: {url}")

# Unzip the file

with zipfile.ZipFile(local_path, 'r') as zip_ref:
    zip_ref.extractall('/home/rli/uk_data/')
print(f"Unzipped to: {'/home/rli/uk_data/'}")

# Define the paths
csv_filename = local_filename.replace('.zip', '.csv')
original_file_path = os.path.join('/home/rli/uk_data', csv_filename)
temp_file_path = os.path.join('/home/rli/uk_data', csv_filename.replace('.csv', '_temp.csv'))

# Open the original file for reading and the temp file for writing
with open(original_file_path, 'r', newline='', encoding='utf-8') as original_file, open(temp_file_path, 'w', newline='', encoding='utf-8') as temp_file:
    
    reader = csv.reader(original_file)
    writer = csv.writer(temp_file)
    
    # Read the header line
    headers = next(reader)
    # Trim leading spaces from the header names
    trimmed_headers = [header.strip() for header in headers]
    # Write the trimmed header to the temp file
    writer.writerow(trimmed_headers)
    
    # Copy the rest of the lines from the original file to the temp file
    for row in reader:
        writer.writerow(row)

# Replace the original file with the temp file
os.replace(temp_file_path, original_file_path)

print(f"Trimmed header spaces and saved to: {original_file_path}")


# Process the file name

cols =['CompanyNumber', 'IncorporationDate', 'CompanyName', 'PreviousName_1.CONDATE', 'PreviousName_1.CompanyName', 'PreviousName_2.CONDATE', 'PreviousName_2.CompanyName', 'PreviousName_3.CONDATE', 'PreviousName_3.CompanyName', 'PreviousName_4.CONDATE', 'PreviousName_4.CompanyName', 'PreviousName_5.CONDATE', 'PreviousName_5.CompanyName', 'PreviousName_6.CONDATE', 'PreviousName_6.CompanyName', 'PreviousName_7.CONDATE', 'PreviousName_7.CompanyName', 'PreviousName_8.CONDATE', 'PreviousName_8.CompanyName', 'PreviousName_9.CONDATE', 'PreviousName_9.CompanyName', 'PreviousName_10.CONDATE', 'PreviousName_10.CompanyName', 'ConfStmtLastMadeUpDate']

df = pd.read_csv(original_file_path, usecols=cols, dtype='str')

import pandas as pd
import os
import csv

# Define the paths
original_file_path = '/home/rli/uk_data/BasicCompanyDataAsOneFile-2024-09-01.csv'
temp_file_path = '/home/rli/uk_data/BasicCompanyDataAsOneFile-2024-09-01_temp.csv'

# Open the original file for reading and the temp file for writing
with open(original_file_path, 'r', newline='', encoding='utf-8') as original_file, \
     open(temp_file_path, 'w', newline='', encoding='utf-8') as temp_file:
    
    reader = csv.reader(original_file)
    writer = csv.writer(temp_file)
    
    # Read the header line
    headers = next(reader)
    # Trim leading spaces from the header names
    trimmed_headers = [header.strip() for header in headers]
    # Write the trimmed header to the temp file
    writer.writerow(trimmed_headers)
    
    # Copy the rest of the lines from the original file to the temp file
    for row in reader:
        writer.writerow(row)

# Replace the original file with the temp file
os.replace(temp_file_path, original_file_path)

print(f"Trimmed header spaces and saved to: {original_file_path}")

# Process the file name
cols = [
    'CompanyNumber', 'IncorporationDate', 'CompanyName',
    'PreviousName_1.CONDATE', 'PreviousName_1.CompanyName',
    'PreviousName_2.CONDATE', 'PreviousName_2.CompanyName',
    'PreviousName_3.CONDATE', 'PreviousName_3.CompanyName',
    'PreviousName_4.CONDATE', 'PreviousName_4.CompanyName',
    'PreviousName_5.CONDATE', 'PreviousName_5.CompanyName',
    'PreviousName_6.CONDATE', 'PreviousName_6.CompanyName',
    'PreviousName_7.CONDATE', 'PreviousName_7.CompanyName',
    'PreviousName_8.CONDATE', 'PreviousName_8.CompanyName',
    'PreviousName_9.CONDATE', 'PreviousName_9.CompanyName',
    'PreviousName_10.CONDATE', 'PreviousName_10.CompanyName',
    'ConfStmtLastMadeUpDate'
]

df = pd.read_csv(original_file_path, usecols=cols, dtype='str')

# Function to process each row
import pandas as pd

# Define the columns of interest
cols = [
    'CompanyNumber', 'IncorporationDate', 'CompanyName',
    'PreviousName_1.CONDATE', 'PreviousName_1.CompanyName',
    'PreviousName_2.CONDATE', 'PreviousName_2.CompanyName',
    'PreviousName_3.CONDATE', 'PreviousName_3.CompanyName',
    'PreviousName_4.CONDATE', 'PreviousName_4.CompanyName',
    'PreviousName_5.CONDATE', 'PreviousName_5.CompanyName',
    'PreviousName_6.CONDATE', 'PreviousName_6.CompanyName',
    'PreviousName_7.CONDATE', 'PreviousName_7.CompanyName',
    'PreviousName_8.CONDATE', 'PreviousName_8.CompanyName',
    'PreviousName_9.CONDATE', 'PreviousName_9.CompanyName',
    'PreviousName_10.CONDATE', 'PreviousName_10.CompanyName'
]

# Read the CSV file into a DataFrame
df = pd.read_csv('/home/rli/uk_data/BasicCompanyDataAsOneFile-2024-09-01.csv', usecols=cols, dtype='str')

# Function to process each row
def process_row(row):
    previous_names = []
    for i in range(1, 11):
        prev_name = row[f'PreviousName_{i}.CompanyName']
        prev_date = row[f'PreviousName_{i}.CONDATE']
        if pd.notna(prev_name):
            previous_names.append((prev_name, prev_date))
    
    if not previous_names:
        # All previous names are NaN
        start_date = row['IncorporationDate']
        previous_names.append((row['CompanyName'], start_date, None))
    else:
        # Set the incorporation date as the start date for the earliest previous name
        incorporation_date = row['IncorporationDate']
        previous_names[-1] = (previous_names[-1][0], incorporation_date, previous_names[-1][1])
    
    # Set start and end dates for previous names
    for i in range(len(previous_names) - 1):
        previous_names[i] = (previous_names[i][0], previous_names[i + 1][1], previous_names[i][1])
    
    # Add the company number and company name to each entry
    company_number = row['CompanyNumber']
    processed_entries = [(company_number, row['CompanyName'], name, start, end) for name, start, end in previous_names]
    
    return processed_entries

# Process the DataFrame
processed_data = []
for _, row in df.iterrows():
    processed_data.extend(process_row(row))

# Create a new DataFrame with the processed data
processed_df = pd.DataFrame(processed_data, columns=['CompanyNumber', 'CompanyName', 'PreviousName', 'StartDate', 'EndDate'])

