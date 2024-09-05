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

# Directory where the patent data will be stored

raw_data_dir = "/var/rel8ed.to/nfs/share/uspto/trademark_folder"

# Initialize an empty list to store the last 7 days' dates
last_seven_days_list = []

# Get today's date
today = datetime.today()

# Generate the last 7 days' dates 249
last_seven_days_list = [(today - timedelta(days=i)).strftime('%y%m%d') for i in range(1,2)]

# Download the patent data for the last 7 days and extract them

for file_date in last_seven_days_list:
    url = "https://bulkdata.uspto.gov/data/trademark/dailyxml/assignments/asb" + file_date + ".zip"
    file_name = "asb" + file_date + ".zip"
    file_path = os.path.join(raw_data_dir, file_name)
    if not os.path.exists(file_path):
        print("downloading file: " + file_name)
        urllib.request.urlretrieve(url, file_path)
        sleep(0.5)
    else:
        print("file already exists: " + file_name)


for file_date in last_seven_days_list:
    file_name = "asb" + file_date + ".zip"
    file_path = os.path.join(raw_data_dir, file_name)
    if not os.path.exists(file_path.replace('.zip', '.xml')): 
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            print("unzipping file: " + file_name)
            zip_ref.extractall(raw_data_dir)
    else:
        print("file already unzipped: " + file_name)


# Delete the previously processed file
if os.path.exists("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignee.csv"):
    os.remove("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignee.csv")

if os.path.exists("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignor.csv"):
    os.remove("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignor.csv")

if os.path.exists("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignment.csv"):
    os.remove("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignment.csv")

# Process the extracted XML files for assignee data

header = True

# for file_date in last_seven_days_list:
for file_date in ['19550103-20231231-01']:
    file_name = "asb" + file_date + ".xml"
    file_path = os.path.join(raw_data_dir, file_name)
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            print("processing file: " + file_name)
            data = f.read()
            bs_data = BeautifulSoup(data, "xml")
            corp = bs_data.find_all("assignment-entry")
            print("the length of file is: ", len(corp))

            if len(corp) == 0:
                continue
            else:
                assignee_rows = []
                assignor_rows = []
                assignment_rows = []

                for i in corp:

                    assignment = i.find_all("assignment")[0]
                    reel_no = get_text_or_empty(assignment, "reel-no").strip()
                    frame_no = get_text_or_empty(assignment, "frame-no").strip()
                    update_date = get_text_or_empty(assignment, "last-update-date").strip()
                    date_recorded = get_text_or_empty(assignment, "date-recorded").strip()

                    for ii in i.find_all("assignee"):
                        assignee = get_text_or_empty(ii, "person-or-organization-name").strip()
                        address_1 = get_text_or_empty(ii, "address-1").strip()
                        address_2 = get_text_or_empty(ii, "address-2").strip()
                        city = get_text_or_empty(ii, "city").strip()
                        province = get_text_or_empty(ii, "state").strip()
                        postalCode = get_text_or_empty(ii, "postcode").strip()
                        businessType = get_text_or_empty(ii, "legal-entity-text").strip()

                    for ii in i.find_all("property"):
                        serial_no = get_text_or_empty(ii, "serial-no").strip()
                        registration_no = get_text_or_empty(ii, "registration-no").strip()

                    for ii in i.find_all("assignor"):
                        assignor = get_text_or_empty(ii, "person-or-organization-name").strip()
                        execution_date = get_text_or_empty(ii, "execution-date").strip()
                        businessType = get_text_or_empty(ii, "legal-entity-text").strip()
                        countryCode = get_text_or_empty(ii, "nationality").strip()

                        assignee_row = [reel_no, frame_no, update_date, date_recorded, assignee, address_1, address_2, city, province, postalCode, businessType]
                        assignee_rows.append(assignee_row)

                        assignment_row = [reel_no, frame_no, update_date, date_recorded, serial_no, registration_no]
                        assignment_rows.append(assignment_row)

                        assignor_row = [reel_no, frame_no, update_date, date_recorded, assignor, execution_date, businessType, countryCode]
                        assignor_rows.append(assignor_row)



                assignee_df = pd.DataFrame(assignee_rows, columns=["reel_no", "frame_no", "update_date", "date_recorded", "name", "address_1", "address_2", "city", "province", "postalCode", "businessType"])
                assignee_df.to_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignee.csv", index=False, mode="a", header=header)

                assignment_df = pd.DataFrame(assignment_rows, columns=["reel_no", "frame_no", "update_date", "date_recorded", "serial_no", "registration_no"])
                assignment_df.to_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignment.csv", index=False, mode="a", header=header)

                assignor_df = pd.DataFrame(assignor_rows, columns=["reel_no", "frame_no", "update_date", "date_recorded", "name", "execution_date", "businessType", "countryCode"])
                assignor_df.to_csv("/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignor.csv", index=False, mode="a", header=header)

                header = False