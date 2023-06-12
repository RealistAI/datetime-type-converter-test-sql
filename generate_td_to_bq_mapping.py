"""
The tables in Teradata will not reside in the same datasets in BigQuery
We need to generate a mapping of each Teradata table and its corresponding
location in BigQuery.

The BigQuery dataset where a table needs to be stored is defined by the 
Business unit of the UC4 Job that creates it. We need to iterate through the 
UC4 jobs that we are migrating, find the tables/views that are created by it
and generate the mapping.

We will store that mapping in BigQuery
"""
from typing import Dict, List

from google.cloud import bigquery
import config
import logging
import re
import utils
from pathlib import Path
import csv

logging.basicConfig(level=config.LOGGING_LEVEL)
logger = logging.getLogger(__name__)


def get_created_tables_and_views(sql_path: Path) -> List[str]:
    """
    Given a SQL file path, find all CREATE statements and get the name of the
    table or view that is being created
    """

    assert sql_path.is_file(), f"Unable to find file: {sql_path}"
    assert sql_path.suffix == '.sql', f"File {sql_path} is not a SQL file"

    logger.info("  Checking for table/view creation statements in "\
            f"{sql_path}")
    with open(sql_path, 'r') as sql_file:
        data = sql_file.read()

    table_references = []
    # Find all the CREATE SET TABLE instasces
    matches = re.findall(
            r'(?:CREATE[\s\n]*SET[\s\n]*TABLE[\s\n]*)([a-zA-Z_$#\.]*)', data)

    for match in matches:
        logger.debug(f"   Found creation of table: {match}")
        table_references.append(match)

    # Find all the CREATE MULTISET TABLE instasces
    matches = re.findall(
            r'(?:CREATE[\s\n]*MULTISET[\s\n]*TABLE[\s\n]*)([a-zA-Z_$#\.]*)', data)

    for match in matches:
        logger.debug(f"   Found creation of table: {match}")
        table_references.append(match)

    # Find all the CREATE VIEW instasces
    matches = re.findall(
            r'(?:CREATE[\s\n]*VIEW[\s\n]*)([a-zA-Z_$#\.]*)', data)

    for match in matches:
        logger.debug(f"   Found creation of view: {match}")
        table_references.append(match)

    return table_references

def get_business_unit_map() -> dict:
    """
    Read the business_unit_map.csv from disk and convert it to a dictionary
    """
    business_unit_map = {}
    with open(config.BUSINESS_UNIT_DATASET_MAP_CSV_FILE, 'r') as csv_file:
        data = csv.reader(csv_file, delimiter=',')

        for row in data:
            assert len(row) == 2, "Malformed row {row} in "\
                    f"{config.BUSINESS_UNIT_DATASET_MAP_CSV_FILE}. The map is "\
                    "expected to have two elements seperated by commas. "

            # Set the map dictionary to have the key be the first element 
            # from the row, and the second be the value. For a row like 
            # 'RISK,risk_dataset' the dict would have the following element:
            # {
            #  "RISK": "risk_dataset"
            # }

            business_unit_map[row[0]] = row[1]


    return business_unit_map

def write_table_mapping_to_bigquery(client: bigquery.Client, table_map: Dict):
    """
    Parse the table map dictionary and write the mappings to BigQuery
    """

    logger.debug("Inserting the Teradata to BigQuery mappings into the "\
            f"'{config.TD_TO_BQ_MAPPING_TABLE}' table")
    query = []
    source_table_list = list(table_map.keys())
    # Delete the prevous records for this table
    
    # Create the list of tables that needs to be deleted in the format that 
    # BigQuery requires:
    # 'dataset_a.table_a',
    # 'dataset_a.table_b'

    parameters = ',\n'.join("'" + item + "'" for item in source_table_list)
    query.append(
            f"DELETE FROM {config.TD_TO_BQ_MAPPING_TABLE}\nWHERE "\
            f"teradata_table in ({parameters})")

    values_list = []
    for key, value in table_map.items():
        logger.debug(f" - Mapping '{key}' to '{value}'")
        # The transpiler will always convert the table and dataset names 
        # to lowercase. We must do the same in our mapping to ensure the 
        # transpiler recognizes the table names
        values_list.append(f"('{key.lower()}', '{value.lower()}')")

    query.append(f"INSERT INTO {config.TD_TO_BQ_MAPPING_TABLE} ("\
            "teradata_table, bigquery_table) \n" \
            f"VALUES{','.join(values_list)}")

    
    utils.submit_query(client=client, query=';\n'.join(query))
    logger.info("  Successfully wrote mappings to "\
            f"{config.TD_TO_BQ_MAPPING_TABLE}")
        

def map_table_references(client:bigquery.Client, table_references: List[str],
                         business_unit:str):
    """
    Given a list of table references and the business unit, create a map 
    of teradata tables to BigQuery tables and write it to a BigQuery Table
    """

    business_unit_map = get_business_unit_map()
    # Try to find the business_unit in the business_unit_map. Warn the user
    # if it isn't found
    mapped_dataset = business_unit_map.get(business_unit)
    
    assert mapped_dataset is not None, \
            f"The '{config.BUSINESS_UNIT_DATASET_MAP_CSV_FILE}' file does not "\
            f"contain a mapping for the '{business_unit}' business unit. You"\
            " must add it before you can continue"

    table_map = {}
    for tr in table_references:
        split_tr = tr.split('.')
        split_tr_length = len(split_tr)
        assert split_tr_length == 3 or split_tr_length == 2, "Malformed Table"\
                f" Reference. {tr} has {split_tr_length} elements. It shound "\
                "have either 2 or 3."

        if len(split_tr) == 3:
            table = split_tr[2]
        else:
            table = split_tr[1]

        logger.info(f"  Creating mapping: '{tr}' -> "\
                f"'{mapped_dataset}.{table}'")
        table_map[tr] = f"{mapped_dataset}.{table}"

    write_table_mapping_to_bigquery(client=client, table_map=table_map)



def setup():
    """
    This method ensures that all of the items required by this script are 
    created and available.
    """


    # Download the repo containing all of the SQLs
    utils.create_path_if_not_exists(config.BASE_PATH)
    utils.get_git_repo(repo=config.UC4_SQL_REPO,
                     base_path=config.BASE_PATH)
    # Make sure the UC4 Config file exists
    assert config.UC4_CSV_FILE.is_file(), "The uc4_jobs.csv file is expected "\
            "to be available here '{config.UC4_CSV_FILE}' but it does not"\
            " exist. Please refer to the README.md for instructions on how to"\
            " create it."

    # Make sure the Business Unit Dataset Map file exists
    assert config.BUSINESS_UNIT_DATASET_MAP_CSV_FILE.is_file(), \
            "The business_unit_dataset_map.csv file is expected te be "\
            f"available here '{config.BUSINESS_UNIT_DATASET_MAP_CSV_FILE}' "\
            "but it does not exist. Please refer to the README.md for "\
            "instructions on how to create it."

    client = bigquery.Client(project=config.METADATA_PROJECT)

    # Create the teradata_to_bigquery_map table if it doesn't exist
    query = f"CREATE TABLE IF NOT EXISTS {config.TD_TO_BQ_MAPPING_TABLE} (\n"\
            "  teradata_table STRING,\n"\
            "  bigquery_table STRING"\
            ")"


    utils.submit_query(client=client, query=query)

    return client

def main():
    """
    Read the csv provided by the user to get the list of UC4 jobs
    Read the JSON representation of those jobs stored in BigQuery to determine
    the SQL files that are referenced.
    Parse those SQLs to find all of the DDL statements
    Generate the mapping based on the business unit of the UC4 job.
    """

    logger.info("============================================================")
    logger.info("= Generating Teradata to BigQuery Mapping                  =")
    logger.info("============================================================")
    # Get the list of UC4 Jobs
    uc4_jobs = []
    logger.debug(f"Reading uc4 jobs from {config.UC4_CSV_FILE}")

    # Ensure the environment is set up correctly
    bigquery_client = setup()

    with open(config.UC4_CSV_FILE, 'r') as uc4_csv_file:
        data = uc4_csv_file.read()

    uc4_jobs = data.split('\n')

    # Get the JSON representation for these jobs from BigQuery
    for uc4_job in uc4_jobs:
        if uc4_job == "":
            continue
        logger.info(f"Generating mapping for tables owned by '{uc4_job}'")

        uc4_json = utils.get_uc4_json(client=bigquery_client, uc4_job_name=uc4_job)
        logger.debug(f"   JSON:{uc4_json}")
        
        business_unit = uc4_json.get('business_unit')
        assert business_unit is not None, "Malformed JSON. The JSON for "\
                f"{uc4_job} does not contain a 'business_unit' element"

        logger.info(f"The business unit for this UC4 Job is {business_unit}")

        assert uc4_json.get('sql_dependencies') is not None, "Malformed JSON."\
                f" {uc4_job} does not contain a 'sql_dependencies' element"

        # Extract the SQL dependencies from the JSON
        sql_dependencies = utils.extract_sql_dependencies(uc4_json['sql_dependencies'])

        # Get all of the table names from the DDL statements
        table_references = []
        for i in sql_dependencies:
            if i == "":
                continue

            sql_path = Path(config.SOURCE_SQL_PATH, i)
            table_references.extend(get_created_tables_and_views(sql_path))

        map_table_references(client=bigquery_client, 
                             table_references=table_references, 
                             business_unit=business_unit)

        logger.info("Successfully created mapping(s) for tables owned by "\
                f"'{uc4_job}'")
        logger.info("")






if __name__ == "__main__":
    main()
        
