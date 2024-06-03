import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from tqdm import tqdm
import subprocess

####
connection_string = "postgresql://postgres:rel8edpg@10.8.0.110:5432/rel8ed"
engine = create_engine(connection_string)

file_date = datetime.today().strftime('%Y-%m-%d')



files = os.listdir('/home/rli/usspending_data/')
files = [f for f in files if f.endswith('.csv')]
files = [f for f in files if 'Contracts' in f]

### processing name

cols = ['recipient_uei', 'recipient_name']

header = True

for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_name'].notnull()]
    df.rename(columns={'recipient_uei': 'identifier', 'recipient_name': 'business_name'}, inplace=True)
    df['name_type'] = 'main'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'business_name', 'name_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_name.csv', index=False, mode='a', header=header)

    header = False



cols = ['recipient_uei', 'recipient_doing_business_as_name']
for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_doing_business_as_name'].notnull()]
    df.rename(columns={'recipient_uei': 'identifier', 'recipient_doing_business_as_name': 'business_name'}, inplace=True)
    df['name_type'] = 'dba'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'business_name', 'name_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_name.csv', index=False, mode='a', header=header)



### loading data to database

csv_path = '/home/rli/usspending_data/usspending_name.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'federal_awards_name'
primary_key_columns = ['identifier', 'business_name']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):

        chunk = chunk.map(lambda x: x.strip() if isinstance(x, str) else x)
        chunk = chunk.astype(str)
        chunk.fillna('', inplace=True)

    # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join([f":{col}" for col in chunk.columns])  # Correct placeholders

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient='records'))

        pbar.update()


### processing address

cols = ['recipient_uei', 'recipient_country_code', 'recipient_address_line_1', 'recipient_address_line_2', 'recipient_city_name', 'recipient_state_code', 'recipient_zip_4_code']

header = True

for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_uei'].notnull()]
    df.fillna('', inplace=True)
    df['address'] = df[['recipient_address_line_1', 'recipient_address_line_2']].apply(lambda x: (' '.join(x.astype(str))).strip(), axis=1)
    df['postal'] = df.apply(lambda x: ('-'.join([x['recipient_zip_4_code'][:5], x['recipient_zip_4_code'][5:9]])).strip('-') if pd.notnull(x['recipient_zip_4_code']) and x['recipient_country_code'] == 'USA' else x['recipient_zip_4_code'], axis=1)
    df.rename(columns={'recipient_uei': 'identifier', 'recipient_city_name': 'city', 'recipient_state_code':'state', 'recipient_country_code':'country'}, inplace=True)
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'address', 'city', 'state', 'postal', 'country', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_address.csv', index=False, mode='a', header=header)

    header = False

### loading data to database

csv_path = '/home/rli/usspending_data/usspending_address.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'federal_awards_location'
primary_key_columns = ['identifier', 'address', 'city', 'state']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
        chunk.fillna('', inplace=True)

    # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join([f":{col}" for col in chunk.columns])  # Correct placeholders

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient='records'))

        pbar.update()


### processing identifier

cols = ['recipient_uei', 'recipient_parent_uei']

header = True

for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_parent_uei'].notnull()]
    df = df[df['recipient_uei']!=df['recipient_parent_uei']]
    df.rename(columns={'recipient_uei': 'identifier', 'recipient_parent_uei': 'identifier_hq'}, inplace=True)
    df['authority'] = 'Unique Entity Identifier'
    df['hq_authority'] = 'Unique Entity Identifier'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'identifier_hq', 'authority', 'hq_authority', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_identifier.csv', index=False, mode='a', header=header)

    header = False


cols = ['recipient_uei', 'recipient_duns']

for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_duns'].notnull()]
    df.rename(columns={'recipient_uei': 'identifier', 'recipient_duns': 'identifier_hq'}, inplace=True)
    df['authority'] = 'Unique Entity Identifier'
    df['hq_authority'] = 'DNB'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'identifier_hq', 'authority', 'hq_authority', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_identifier.csv', index=False, mode='a', header=header)



cols = ['recipient_uei', 'cage_code']

for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['cage_code'].notnull()]
    df.rename(columns={'recipient_uei': 'identifier', 'cage_code': 'identifier_hq'}, inplace=True)
    df['authority'] = 'Unique Entity Identifier'
    df['hq_authority'] = 'Commercial and Government Entity Code'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'identifier_hq', 'authority', 'hq_authority', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_identifier.csv', index=False, mode='a', header=header)


### loading data to database

csv_path = '/home/rli/usspending_data/usspending_identifier.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'federal_awards_identifier'
primary_key_columns = ['identifier', 'identifier_hq']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
        chunk.fillna('', inplace=True)

    # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join([f":{col}" for col in chunk.columns])  # Correct placeholders

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient='records'))

        pbar.update()


### processing category

cols = ['recipient_uei', 'naics_code', 'naics_description']

header = True

for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['naics_code'].notnull()]
    df.rename(columns={'recipient_uei': 'identifier', 'naics_code': 'category_code', 'naics_description':'category_name'}, inplace=True)
    df['category_type'] = 'NAICS'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'category_code', 'category_name', 'category_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_category.csv', index=False, mode='a', header=header)

    header = False

### loading data to database

csv_path = '/home/rli/usspending_data/usspending_category.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'federal_awards_category'
primary_key_columns = ['identifier', 'category_code', 'category_type']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
        chunk.fillna('', inplace=True)

    # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join([f":{col}" for col in chunk.columns])  # Correct placeholders

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient='records'))

        pbar.update()


### processing phone

cols = ['recipient_uei', 'recipient_phone_number']

header = True

for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_phone_number'].notnull()]
    df.rename(columns={'recipient_uei': 'identifier', 'recipient_phone_number': 'phone'}, inplace=True)
    df['phone_type'] = 'phone'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'phone', 'phone_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_phone.csv', index=False, mode='a', header=header)

    header = False


cols = ['recipient_uei', 'recipient_fax_number']

for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df = df[df['recipient_fax_number'].notnull()]
    df.rename(columns={'recipient_uei': 'identifier', 'recipient_fax_number': 'phone'}, inplace=True)
    df['phone_type'] = 'fax'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df = df[['identifier', 'phone', 'phone_type', 'first_time_check', 'last_time_check']]
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_phone.csv', index=False, mode='a', header=header)


### loading data to database

csv_path = '/home/rli/usspending_data/usspending_phone.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'federal_awards_phone'
primary_key_columns = ['identifier', 'phone']  # Composite primary key
update_columns = ['last_time_check']  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
        chunk.fillna('', inplace=True)

    # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join([f":{col}" for col in chunk.columns])  # Correct placeholders

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient='records'))

        pbar.update()



### processing activity

cols = ["recipient_uei", "contract_transaction_unique_key",
"contract_award_unique_key",
"award_id_piid",
"modification_number",
"transaction_number",
"parent_award_agency_id",
"parent_award_agency_name",
"parent_award_id_piid",
"parent_award_modification_number",
"federal_action_obligation",
"total_dollars_obligated",
"total_outlayed_amount_for_overall_award",
"base_and_exercised_options_value",
"current_total_value_of_award",
"base_and_all_options_value",
"potential_total_value_of_award",
"disaster_emergency_fund_codes_for_overall_award",
"outlayed_amount_from_COVID-19_supplementals_for_overall_award", 
"obligated_amount_from_COVID-19_supplementals_for_overall_award", 
"outlayed_amount_from_IIJA_supplemental_for_overall_award", 
"obligated_amount_from_IIJA_supplemental_for_overall_award", 
"action_date", 
"action_date_fiscal_year", 
"period_of_performance_start_date", 
"period_of_performance_current_end_date", 
"period_of_performance_potential_end_date", 
"ordering_period_end_date", 
"solicitation_date", 
"awarding_agency_code", 
"awarding_agency_name", 
"awarding_sub_agency_code", 
"awarding_sub_agency_name", 
"awarding_office_code", 
"awarding_office_name", 
"funding_agency_code", 
"funding_agency_name", 
"funding_sub_agency_code", 
"funding_sub_agency_name", 
"funding_office_code", 
"funding_office_name", 
"treasury_accounts_funding_this_award", 
"federal_accounts_funding_this_award", 
"object_classes_funding_this_award", 
"program_activities_funding_this_award", 
"foreign_funding", 
"foreign_funding_description", 
"sam_exception", 
"sam_exception_description", 
"award_or_idv_flag", 
"award_type_code", 
"award_type", 
"idv_type_code", 
"idv_type", 
"multiple_or_single_award_idv_code", 
"multiple_or_single_award_idv", 
"type_of_idc_code", 
"type_of_idc", 
"type_of_contract_pricing_code", 
"type_of_contract_pricing", 
"transaction_description", 
"prime_award_base_transaction_description", 
"action_type_code", 
"action_type", 
"solicitation_identifier", 
"number_of_actions", 
"inherently_governmental_functions", 
"inherently_governmental_functions_description", 
"product_or_service_code", 
"product_or_service_code_description", 
"contract_bundling_code", 
"contract_bundling", 
"dod_claimant_program_code", 
"dod_claimant_program_description",
"recovered_materials_sustainability_code", 
"recovered_materials_sustainability", 
"domestic_or_foreign_entity_code", 
"domestic_or_foreign_entity", 
"dod_acquisition_program_code", 
"dod_acquisition_program_description", 
"information_technology_commercial_item_category_code", 
"information_technology_commercial_item_category", 
"epa_designated_product_code", 
"epa_designated_product", 
"country_of_product_or_service_origin_code", 
"country_of_product_or_service_origin", 
"place_of_manufacture_code", 
"place_of_manufacture", 
"subcontracting_plan_code", 
"subcontracting_plan", 
"extent_competed_code", 
"extent_competed", 
"solicitation_procedures_code", 
"solicitation_procedures", 
"type_of_set_aside_code", 
"type_of_set_aside", 
"evaluated_preference_code", 
"evaluated_preference", 
"research_code", 
"research", 
"fair_opportunity_limited_sources_code", 
"fair_opportunity_limited_sources", 
"other_than_full_and_open_competition_code", 
"other_than_full_and_open_competition", 
"number_of_offers_received", 
"commercial_item_acquisition_procedures_code", 
"commercial_item_acquisition_procedures", 
"small_business_competitiveness_demonstration_program", 
"simplified_procedures_for_certain_commercial_items_code", 
"simplified_procedures_for_certain_commercial_items", 
"a76_fair_act_action_code", 
"a76_fair_act_action", 
"fed_biz_opps_code", 
"fed_biz_opps", 
"local_area_set_aside_code", 
"local_area_set_aside", 
"price_evaluation_adjustment_preference_percent_difference", 
"clinger_cohen_act_planning_code", 
"clinger_cohen_act_planning", 
"materials_supplies_articles_equipment_code", 
"materials_supplies_articles_equipment", 
"labor_standards_code", 
"labor_standards", 
"construction_wage_rate_requirements_code", 
"construction_wage_rate_requirements", 
"interagency_contracting_authority_code", 
"interagency_contracting_authority", 
"other_statutory_authority", 
"program_acronym", 
"parent_award_type_code", 
"parent_award_type", 
"parent_award_single_or_multiple_code", 
"parent_award_single_or_multiple", 
"major_program", 
"national_interest_action_code", 
"national_interest_action", 
"cost_or_pricing_data_code", 
"cost_or_pricing_data", 
"cost_accounting_standards_clause_code", 
"cost_accounting_standards_clause", 
"government_furnished_property_code", 
"government_furnished_property", 
"sea_transportation_code", 
"sea_transportation", 
"undefinitized_action_code", 
"undefinitized_action", 
"consolidated_contract_code", 
"consolidated_contract", 
"performance_based_service_acquisition_code", 
"performance_based_service_acquisition", 
"multi_year_contract_code", 
"multi_year_contract", 
"contract_financing_code", 
"contract_financing", 
"purchase_card_as_payment_method_code", 
"purchase_card_as_payment_method", 
"contingency_humanitarian_or_peacekeeping_operation_code", 
"contingency_humanitarian_or_peacekeeping_operation", 
"highly_compensated_officer_1_name", 
"highly_compensated_officer_1_amount", 
"highly_compensated_officer_2_name", 
"highly_compensated_officer_2_amount", 
"highly_compensated_officer_3_name", 
"highly_compensated_officer_3_amount", 
"highly_compensated_officer_4_name", 
"highly_compensated_officer_4_amount", 
"highly_compensated_officer_5_name", 
"highly_compensated_officer_5_amount", 
"usaspending_permalink", 
"initial_report_date", 
"last_modified_date"]

header = True

for f in files:
    df = pd.read_csv('/home/rli/usspending_data/' + f, usecols=cols, dtype=str)
    df.rename(columns={'recipient_uei': 'identifier'}, inplace=True)
    df['category_type'] = 'NAICS'
    df['first_time_check'] = file_date
    df['last_time_check'] = file_date
    df.drop_duplicates(inplace=True)
    df.to_csv('/home/rli/usspending_data/usspending_activity.csv', index=False, mode='a', header=header)

    header = False

### loading data to database

csv_path = '/home/rli/usspending_data/usspending_activity.csv'
chunk_size = 100

# Count the total number of rows in the CSV file (excluding the header)
total_rows = int(subprocess.check_output(['wc', '-l', csv_path]).split()[0]) - 1

# Calculate the total number of chunks
total_chunks = total_rows // chunk_size
if total_rows % chunk_size:
    total_chunks += 1

# Specify the table and the primary key columns
table_name = 'federal_awards_category'
primary_key_columns = ['contract_transaction_unique_key']  # Composite primary key
update_columns = ["contract_award_unique_key",
"award_id_piid",
"modification_number",
"transaction_number",
"parent_award_agency_id",
"parent_award_agency_name",
"parent_award_id_piid",
"parent_award_modification_number",
"federal_action_obligation",
"total_dollars_obligated",
"total_outlayed_amount_for_overall_award",
"base_and_exercised_options_value",
"current_total_value_of_award",
"base_and_all_options_value",
"potential_total_value_of_award",
"disaster_emergency_fund_codes_for_overall_award",
"outlayed_amount_from_COVID-19_supplementals_for_overall_award", 
"obligated_amount_from_COVID-19_supplementals_for_overall_award", 
"outlayed_amount_from_IIJA_supplemental_for_overall_award", 
"obligated_amount_from_IIJA_supplemental_for_overall_award", 
"action_date", 
"action_date_fiscal_year", 
"period_of_performance_start_date", 
"period_of_performance_current_end_date", 
"period_of_performance_potential_end_date", 
"ordering_period_end_date", 
"solicitation_date", 
"awarding_agency_code", 
"awarding_agency_name", 
"awarding_sub_agency_code", 
"awarding_sub_agency_name", 
"awarding_office_code", 
"awarding_office_name", 
"funding_agency_code", 
"funding_agency_name", 
"funding_sub_agency_code", 
"funding_sub_agency_name", 
"funding_office_code", 
"funding_office_name", 
"treasury_accounts_funding_this_award", 
"federal_accounts_funding_this_award", 
"object_classes_funding_this_award", 
"program_activities_funding_this_award", 
"foreign_funding", 
"foreign_funding_description", 
"sam_exception", 
"sam_exception_description", 
"award_or_idv_flag", 
"award_type_code", 
"award_type", 
"idv_type_code", 
"idv_type", 
"multiple_or_single_award_idv_code", 
"multiple_or_single_award_idv", 
"type_of_idc_code", 
"type_of_idc", 
"type_of_contract_pricing_code", 
"type_of_contract_pricing", 
"transaction_description", 
"prime_award_base_transaction_description", 
"action_type_code", 
"action_type", 
"solicitation_identifier", 
"number_of_actions", 
"inherently_governmental_functions", 
"inherently_governmental_functions_description", 
"product_or_service_code", 
"product_or_service_code_description", 
"contract_bundling_code", 
"contract_bundling", 
"dod_claimant_program_code", 
"dod_claimant_program_description",
"recovered_materials_sustainability_code", 
"recovered_materials_sustainability", 
"domestic_or_foreign_entity_code", 
"domestic_or_foreign_entity", 
"dod_acquisition_program_code", 
"dod_acquisition_program_description", 
"information_technology_commercial_item_category_code", 
"information_technology_commercial_item_category", 
"epa_designated_product_code", 
"epa_designated_product", 
"country_of_product_or_service_origin_code", 
"country_of_product_or_service_origin", 
"place_of_manufacture_code", 
"place_of_manufacture", 
"subcontracting_plan_code", 
"subcontracting_plan", 
"extent_competed_code", 
"extent_competed", 
"solicitation_procedures_code", 
"solicitation_procedures", 
"type_of_set_aside_code", 
"type_of_set_aside", 
"evaluated_preference_code", 
"evaluated_preference", 
"research_code", 
"research", 
"fair_opportunity_limited_sources_code", 
"fair_opportunity_limited_sources", 
"other_than_full_and_open_competition_code", 
"other_than_full_and_open_competition", 
"number_of_offers_received", 
"commercial_item_acquisition_procedures_code", 
"commercial_item_acquisition_procedures", 
"small_business_competitiveness_demonstration_program", 
"simplified_procedures_for_certain_commercial_items_code", 
"simplified_procedures_for_certain_commercial_items", 
"a76_fair_act_action_code", 
"a76_fair_act_action", 
"fed_biz_opps_code", 
"fed_biz_opps", 
"local_area_set_aside_code", 
"local_area_set_aside", 
"price_evaluation_adjustment_preference_percent_difference", 
"clinger_cohen_act_planning_code", 
"clinger_cohen_act_planning", 
"materials_supplies_articles_equipment_code", 
"materials_supplies_articles_equipment", 
"labor_standards_code", 
"labor_standards", 
"construction_wage_rate_requirements_code", 
"construction_wage_rate_requirements", 
"interagency_contracting_authority_code", 
"interagency_contracting_authority", 
"other_statutory_authority", 
"program_acronym", 
"parent_award_type_code", 
"parent_award_type", 
"parent_award_single_or_multiple_code", 
"parent_award_single_or_multiple", 
"major_program", 
"national_interest_action_code", 
"national_interest_action", 
"cost_or_pricing_data_code", 
"cost_or_pricing_data", 
"cost_accounting_standards_clause_code", 
"cost_accounting_standards_clause", 
"government_furnished_property_code", 
"government_furnished_property", 
"sea_transportation_code", 
"sea_transportation", 
"undefinitized_action_code", 
"undefinitized_action", 
"consolidated_contract_code", 
"consolidated_contract", 
"performance_based_service_acquisition_code", 
"performance_based_service_acquisition", 
"multi_year_contract_code", 
"multi_year_contract", 
"contract_financing_code", 
"contract_financing", 
"purchase_card_as_payment_method_code", 
"purchase_card_as_payment_method", 
"contingency_humanitarian_or_peacekeeping_operation_code", 
"contingency_humanitarian_or_peacekeeping_operation", 
"highly_compensated_officer_1_name", 
"highly_compensated_officer_1_amount", 
"highly_compensated_officer_2_name", 
"highly_compensated_officer_2_amount", 
"highly_compensated_officer_3_name", 
"highly_compensated_officer_3_amount", 
"highly_compensated_officer_4_name", 
"highly_compensated_officer_4_amount", 
"highly_compensated_officer_5_name", 
"highly_compensated_officer_5_amount", 
"usaspending_permalink", 
"initial_report_date", 
"last_modified_date", "last_time_check"]  # Columns to update in case of conflict

with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size, dtype='str'), desc="Processing chunks"):
        chunk.fillna('', inplace=True)

    # Construct the insert statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join([f":{col}" for col in chunk.columns])  # Correct placeholders

        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(chunk.columns)})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(primary_key_columns)}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
        """

        if chunk is not None and not chunk.empty:
            with engine.begin() as connection:
                connection.execute(text(insert_sql), chunk.to_dict(orient='records'))

        pbar.update()