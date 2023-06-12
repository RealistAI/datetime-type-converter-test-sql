import logging
import os
import shutil
from typing import Tuple
import uuid

import config
import utils
import json
from pathlib import Path
from google.cloud import bigquery

logging.basicConfig(level=config.LOGGING_LEVEL)
logger = logging.getLogger(__name__)


# We will add a uuid so we can handle runs in parallel
root_bucket  = f"{config.BQMS_GCS_BUCKET}/{uuid.uuid4()}"
BQMS_PREPROCESSED_PATH = f"{root_bucket}/preprocessed"
BQMS_POSTPROCESSED_PATH = f"{root_bucket}/postprocessed"
BQMS_TRANSLATED_PATH = f"{root_bucket}/translated"

def setup():
    """
    This method ensures that all of the items required by this script are 
    created and available.
    """


    # Make sure the BQMS Folder exists and is empty
    if os.path.exists(config.BQMS_FOLDER):
        shutil.rmtree(config.BQMS_FOLDER)

    for path in [
            config.BQMS_FOLDER,
            config.BQMS_INPUT_FOLDER,
            config.BQMS_OUTPUT_FOLDER,
            config.BQMS_CONFIG_FOLDER
            ]:
        utils.create_path_if_not_exists(path)
        
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
    query = f"CREATE TABLE IF NOT EXISTS {config.TRANSLATION_LOG_TABLE} (\n"\
            "  uc4_job  STRING,\n"\
            "  status STRING,\n"\
            "  message STRING,\n"\
            "  dry_run_sql STRING,\n"\
            "  referenced_sql_files STRING,\n"\
            "  timestamp TIMESTAMP\n"\
            ")"


    utils.submit_query(client=client, query=query)

    return client

def generate_bqms_config():
    """
    Generate the config needed to run the BQMS job
    """
    lines = []

    lines.append(f"translation_type: Translation_Teradata2BQ")
    lines.append("location: 'us'")
    lines.append(f"default_database: {config.BQMS_DEFAULT_DATABASE}")
    lines.append("")

    with open(config.BQMS_CONFIG_FILE, 'w+') as config_file:
        config_file.truncate(0)
        config_file.write('\n'.join(lines))

def split_table_reference(table_reference: str) -> Tuple[str, str]:
    """
    Given a table reference of dataset.table return the dataset and table as a
    tuple
    """
    split_reference = table_reference.split('.')

    assert len(split_reference) == 2, \
            "Malformed table reference {table_reference}. Should be a string "\
            "like dataset.table"

    return split_reference[0], split_reference[1]

def generate_object_mapping(client: bigquery.Client):
    """
    Pull the mappings from the Teradata to BigQuery Mapping table in BigQuery
    and build the corresponding Object Mapping.
    """

    object_map = {
    }
    name_map = []

    # Get the records from the mapping table
    query = "SELECT teradata_table, bigquery_table\n" \
            f"FROM {config.TD_TO_BQ_MAPPING_TABLE}"

    results = utils.submit_query(client=client, query=query)

            
    # For every row in the mapping table, add a name_map
    for result in results:
        teradata_table_ref = result.get('teradata_table')

        teradata_dataset, teradata_table_name = \
                split_table_reference(teradata_table_ref)

        bigquery_table_ref = result.get('bigquery_table')

        bigquery_dataset, bigquery_table_name = \
                split_table_reference(bigquery_table_ref)


        name_map.append(
                {
                    "source": {
                        "type": "RELATION",
                        "database": config.BQMS_DEFAULT_DATABASE,
                        "schema": teradata_dataset,
                        "relation": teradata_table_name
                    },
                    "target": {
                        "database": config.BQMS_DEFAULT_DATABASE,
                        "schema": bigquery_dataset,
                        "relation": bigquery_table_name
                    }

                }
        )

    object_map["name_map"] = name_map

    # Write the config file to disk
    with open(config.BQMS_OBJECT_MAPPING_FILE, 'w+') as mapping_file:
        mapping_file.write(json.dumps(object_map, indent=4))

def submit_job_to_bqms():
    """
    Submit our job and all of the config to BQMS
    """

    os.environ['BQMS_PROJECT'] = config.BQMS_PROJECT
    os.environ['BQMS_PREPROCESSED_PATH'] = BQMS_PREPROCESSED_PATH
    os.environ['BQMS_POSTPROCESSED_PATH'] = BQMS_POSTPROCESSED_PATH
    os.environ['BQMS_TRANSLATED_PATH'] = BQMS_TRANSLATED_PATH
    os.environ['BQMS_INPUT_PATH'] = str(config.BQMS_INPUT_FOLDER)
    os.environ['BQMS_CONFIG_PATH'] = str(config.BQMS_CONFIG_FILE)
    os.environ['BQMS_OBJECT_NAME_MAPPING_PATH'] = str(config.BQMS_OBJECT_MAPPING_FILE)
    os.system(f"python {Path(Path.cwd(), 'dwh-migration-tools/client/bqms_run/main.py')} --input {config.BQMS_INPUT_FOLDER} --output {config.BQMS_OUTPUT_FOLDER} --config {config.BQMS_CONFIG_FILE} -o {config.BQMS_OBJECT_MAPPING_FILE}")

    # Download all of the transpiled files to the output forder
    os.system(f'gsutil -m -o "GSUtil:parallel_process_count=1" cp -r {BQMS_TRANSLATED_PATH} {config.BQMS_OUTPUT_FOLDER}')


def write_log_to_table(client:bigquery.Client, uc4_job: str, result:str, 
                       message:str, dry_run_sql:str, referenced_sqls:str):
    """
    Log the results of the dry-run to the BigQuery log table.
    """

    # assuming we only need the first line of the 
    query = f"INSERT INTO {config.TRANSLATION_LOG_TABLE} " \
            "(uc4_job, status, message, dry_run_sql, referenced_sql_files, " \
            "timestamp)\n" \
            f"VALUES('{uc4_job}','{result}', \"\"\"{message}\"\"\", " \
            f"\"\"\"{dry_run_sql}\"\"\", \"\"\"{referenced_sqls}\"\"\", " \
            "CURRENT_TIMESTAMP());"

    print(query)
    utils.submit_query(client=client, query=query)

def validate_sqls(client: bigquery.Client, uc4_jobs: list[str],
                  uc4_sql_dependencies: dict):
    """
    We need to validate the SQLs that have been translated. It is important 
    that we submit all of the SQLs for a given UC4 job at the same time as 
    there may be cases where one SQL creates a table/view that is read by 
    another.
    
    We are going to collect all of the SQL code for the UC4 job provided and 
    submit it to BigQuery as a single statement.

    We will then write the dry-run result to BigQuery
    """

    # Iterate through the UC4 Jobs and read the SQL files from the output 
    # folder into a list
    for uc4_job in uc4_jobs:
        if uc4_job == "":
            continue

        sqls = []
        for sql_ref in uc4_sql_dependencies[uc4_job]:
            # Create a path to the output folder where the SQL should reside
            sql_path = Path(config.BQMS_OUTPUT_FOLDER, 'translated', sql_ref)

            logger.info(f"Collecting '{sql_path}' for dry-run")
            assert sql_path.exists, f"SQL Path referenced by {uc4_job} " \
                    "does not exist: {sql_path}"

            with open(sql_path, 'r') as sql_file:
                sqls.append(sql_file.read())

        # Dry run the SQLs
        query = '\n'.join(sqls)
        result, message  = utils.submit_dry_run(client=client,
                                                query=query)

        if result == 'SUCCEEEDED':
            logger.info(f"dry-run for {uc4_job} succeeded")
        else:
            logger.warning(f"dry-run for {uc4_job} failed.")

        
        sql_references = []
        for ref in uc4_sql_dependencies[uc4_job]:
            sql_references.append(str(ref))

        write_log_to_table(client=client, uc4_job=uc4_job, result=result,
                           message=message, dry_run_sql=query,
                           referenced_sqls='\n'.join(sql_references))





def main():
    """
    Given a uc4_jobs csv file,
    Get the SQL dependencies for the c4 
    Copy them into the input folder
    Generate the Object Mapping
    Run the BQMS
    """

    logger.info("============================================================")
    logger.info("= Translating UC4 SQLs                                     =")
    logger.info("============================================================")
    bigquery_client = setup()
    with open(config.UC4_CSV_FILE, 'r') as uc4_csv_file:
        data = uc4_csv_file.read()

    uc4_jobs = data.split('\n')

    # At the end of this process we want to have a dictionary of jobs and their
    # corresponding SQL references
    # {
    #   "UC4_JOB_1": [
    #      "path/to/sql/file_1.sql"
    #      "path/to/sql/file_2.sql"
    #   ],
    #   ...

    uc4_sql_dependencies = {}

    # Collect the SQL dependencies and copy them to the BQMS_INPUT_PATH
    for uc4_job in uc4_jobs:
        if uc4_job == "":
            continue

        sql_paths = []
        logger.info(f"Collecting SQLs referenced by {uc4_job}")
        uc4_json = utils.get_uc4_json(client=bigquery_client,
                                      uc4_job_name=uc4_job)

        assert uc4_json.get('sql_dependencies') is not None, "Malformed JSON." \
                f" {uc4_job} does not contain a 'sql_dependencies' element"

        sql_dependencies = utils.extract_sql_dependencies(
                uc4_json['sql_dependencies'])

        for sql in sql_dependencies:
            if sql == '':
              continue
            
            source_path = Path(config.SOURCE_SQL_PATH, sql)
            logger.info(f"  Found sql dependency: {source_path}")

            # Make sure the path actually exists
            assert source_path.exists(), \
                    f"Unable to find SQL dependency '{source_path}'. File does not "\
                    "exist."

            # Copy the SQL to the input folder
            dest_path = Path(config.BQMS_INPUT_FOLDER, sql)
            dest_path.parents[0].mkdir(parents=True, exist_ok=True)
            shutil.copy(source_path, dest_path.parents[0])
            logger.info(f"  Copied {source_path} to '{config.BQMS_INPUT_FOLDER}'") 

            # We want to add the unchaged SQL path to the list
            sql_paths.append(sql)

        uc4_sql_dependencies[uc4_job] = sql_paths
        logger.info("")

        
    # Generate the object mapping based on the data in the 
    # TERADATA_TO_BIGQUERY_MAP table.
    generate_object_mapping(client=bigquery_client)

    # Generate the BQMS config.yaml file
    generate_bqms_config()

    # Submit the job to the BQMS
    submit_job_to_bqms()

    # Perform the dry-runs
    validate_sqls(client=bigquery_client, uc4_jobs=uc4_jobs,
                  uc4_sql_dependencies=uc4_sql_dependencies)
    

if __name__ == "__main__":
    main()
