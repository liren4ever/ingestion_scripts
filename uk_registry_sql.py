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
local_path = os.path.join('/var/rel8ed.to/nfs/share/uk_company/', local_filename)

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
    zip_ref.extractall('/var/rel8ed.to/nfs/share/uk_company/')
print(f"Unzipped to: {'/var/rel8ed.to/nfs/share/uk_company/'}")

# Define the paths
csv_filename = local_filename.replace('.zip', '.csv')
original_file_path = os.path.join('/var/rel8ed.to/nfs/share/uk_company/', csv_filename)
temp_file_path = os.path.join('/var/rel8ed.to/nfs/share/uk_company/', csv_filename.replace('.csv', '_temp.csv'))

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

cols =['CompanyNumber', 'DissolutionDate', 'IncorporationDate', 'CompanyName', 'PreviousName_1.CONDATE', 'PreviousName_1.CompanyName', 'PreviousName_2.CONDATE', 'PreviousName_2.CompanyName', 'PreviousName_3.CONDATE', 'PreviousName_3.CompanyName', 'PreviousName_4.CONDATE', 'PreviousName_4.CompanyName', 'PreviousName_5.CONDATE', 'PreviousName_5.CompanyName', 'PreviousName_6.CONDATE', 'PreviousName_6.CompanyName', 'PreviousName_7.CONDATE', 'PreviousName_7.CompanyName', 'PreviousName_8.CONDATE', 'PreviousName_8.CompanyName', 'PreviousName_9.CONDATE', 'PreviousName_9.CompanyName', 'PreviousName_10.CONDATE', 'PreviousName_10.CompanyName', 'ConfStmtLastMadeUpDate']

df = pd.read_csv(original_file_path, usecols=cols, dtype='str')

df[df['PreviousName_10.CompanyName'].notna()]['CompanyNumber','PreviousName_10.CompanyName', 'IncorporationDate', 'PreviousName_10.CONDATE'].to_csv('/var/rel8ed.to/nfs/share/uk_company/uk_names.csv', index=False)