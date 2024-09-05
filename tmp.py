import pandas as pd
from bs4 import BeautifulSoup

def get_text_or_empty(element, tag_name):
    try:
        return element.find_all(tag_name)[0].text
    except (IndexError, AttributeError):
        return ""

file_path = "path_to_your_file.xml"
output_csv = "/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignee.csv"

with open(file_path, "r", encoding="utf-8") as f:
    data = f.read()
    bs_data = BeautifulSoup(data, "xml")
    corp = bs_data.find_all("assignment-entry")
    
    if len(corp) == 0:
        exit()
    
    rows = []
    size = len(corp)
    header = True

    for i in corp:
        size -= 1
        print("remaining records " + str(size))
        
        assignment = i.find_all("assignment")[0]
        reel_no = get_text_or_empty(assignment, "reel-no")
        frame_no = get_text_or_empty(assignment, "frame-no")
        update_date = get_text_or_empty(assignment, "last-update-date")
        purge_indicator = get_text_or_empty(assignment, "purge-indicator")
        date_recorded = get_text_or_empty(assignment, "date-recorded")
        page_count = get_text_or_empty(assignment, "page-count")
        conveyance = get_text_or_empty(assignment, "conveyance-text")
        
        for ii in i.find_all("property"):
            serial_no = get_text_or_empty(ii, "serial-no")
            registration_no = get_text_or_empty(ii, "registration-no")
            
            row = [reel_no, frame_no, update_date, purge_indicator, date_recorded, page_count, conveyance, serial_no, registration_no]
            rows.append(row)
    
    df = pd.DataFrame(rows, columns=["reel_no", "frame_no", "update_date", "purge_indicator", "date_recorded", "page_count", "conveyance", "serial_no", "registration_no"])
    df.to_csv('/var/rel8ed.to/nfs/share/uspto/kumiai_data/trademark_assignment.csv', index=False, mode="a", header=header)
    header = False