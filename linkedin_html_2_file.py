import pandas as pd
from bs4 import BeautifulSoup
import json
import glob
from sqlalchemy import create_engine, text
from tqdm import tqdm

connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

def catch_exceptions_return_none(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Log the exception if needed (optional)
#             print(f"Exception caught: {e}")
            return None
    return wrapper
class LinkedinParser():
    def __init__(self,):
        pass
    def extract_base_values(self,soup):
        base_string = soup.find("script",{"type":"application/ld+json"})
        if base_string is not None:
            dict_values = json.loads(base_string.text.strip())
            @catch_exceptions_return_none
            def _extract(key,add_key=None):
                if add_key is None:
                    return dict_values[key]
                else:
                    return dict_values[key][add_key]
            return {
                "company_name": _extract("name"),
                "organization_type": _extract("@type"),
                "linkedin_url":_extract("url"),
                "about_us":_extract("description"),
                "nb_of_employees":_extract("numberOfEmployees","value"),
                "slogan":_extract("slogan"),
                "url":_extract("sameAs"),
                "hq_location_type": _extract("address","type"),
                "hq_street": _extract("address","streetAddress"),
                "hq_city": _extract("address","addressLocality"),
                "hq_region": _extract("address","addressRegion"),
                "hq_postal": _extract("address","postalCode"),
                "hq_country": _extract("address","addressCountry"),
            }
    @catch_exceptions_return_none
    def extract_locations(self,soup):
        return ";".join([" --- ".join([elt.text.strip() for elt in addr.find_all("p")]) for addr in soup.find(class_="locations").find_all("li")])
    @catch_exceptions_return_none
    def extract_industry(self,soup):
        return soup.find(class_="top-card-layout__headline break-words font-sans text-md leading-open text-color-text").text.strip()
    @catch_exceptions_return_none
    def extract_nb_of_followers(self,soup):
        return str(soup.find(class_="top-card-layout__first-subline font-sans text-md leading-open text-color-text-low-emphasis")).split("</span>")[1].split("</h3>")[0].split("followers")[0].strip()
    @catch_exceptions_return_none
    def extract_specialties(self,soup):
        return soup.find("div",{"data-test-id":"about-us__specialties"}).find("dd").text.strip()
    @catch_exceptions_return_none
    def extract_employees(self,soup):
        return ";".join([elt.text.strip() for elt in soup.find("section",{"data-test-id":"employees-at"}).find_all("h3")])
    @catch_exceptions_return_none
    def extract_tiles(self,soup):
        return ";".join([elt.text.strip() for elt in soup.find("section",{"data-test-id":"employees-at"}).find_all("h4")])
    @catch_exceptions_return_none
    def extract_similar_pages(self,soup):
        return ";".join([elt.find("a", href=True)["href"] for elt in soup.find("ul",{"data-impression-id":"similar-pages_show-more-less"}).find_all("li")])
    def extract_all(self,soup,index=0):
        base = self.extract_base_values(soup)
        extra = {
            "locations" : self.extract_locations(soup),
            "industry" : self.extract_industry(soup),
            "nb_of_followers" : self.extract_nb_of_followers(soup),
            "specialties" : self.extract_specialties(soup),
            "employees" : self.extract_employees(soup),
            "titles" : self.extract_tiles(soup),
            "similar_pages": self.extract_similar_pages(soup)
        }
        data = {}
        if base is not None:
            data = base
        if extra is not None:
            data = data | extra
        df = pd.DataFrame(data,index=[index]).replace("",None)
        df["locations"] = df["locations"].str.split(";")
        df["specialties"] = df["specialties"].str.split(",")
        df["employees"] = df["employees"].str.split(";")
        df["titles"] = df["titles"].str.split(";")
        df["similar_pages"] = df["similar_pages"].str.split(";")
        return df

# LinkedinParser().extract_all(BeautifulSoup(open(glob.glob("//var/rel8ed.to/share_files/linkedIn_main/*.html")[2]), 'html.parser'))
all_dfs = []
for file in glob.glob("/var/rel8ed.to/share_files/linkedIn_main/*.html"):
    with open(file, 'r') as f:
        soup = BeautifulSoup(f, 'html.parser')
        df = LinkedinParser().extract_all(soup)
        all_dfs.append(df)

final_df = pd.concat(all_dfs, ignore_index=True)
final_df.to_csv("/var/rel8ed.to/share_files/linkedin_raw.csv", index=False)


###
csv_path = '/var/rel8ed.to/share_files/linkedin_raw.csv'
chunk_size = 1000

# Count the total number of rows in the CSV file (excluding the header)
total_rows = sum(1 for row in open(csv_path)) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = "linkedin_raw"
primary_key_columns = [
    "linkedin_url",
]  # Composite primary key
update_columns = ['business_name', 'organization_type', 'about_us', 'nb_of_employees', 'slogan', 'url', 'hq_location_type', 'hq_street', 'hq_city', 'hq_region', 'hq_postal', 'hq_country', 'locations', 'industry', 'nb_of_followers', 'specialties', 'employees', 'titles', 'similar_pages']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing Linkedin") as pbar:
    for chunk in tqdm(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype="str",
        ),
        desc="Processing Linkedin",
    ):
        chunk = chunk.copy()
        chunk.rename(columns={'company_name':'business_name'}, inplace=True)
        chunk['nb_of_followers'] = chunk['nb_of_followers'].str.replace('follower','').replace(',','').str.strip()
        chunk['nb_of_employees'] = chunk['nb_of_employees'].str.replace(',','').str.strip()
        chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

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