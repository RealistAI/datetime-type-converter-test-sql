import logging
import os
import shutil
from typing import Tuple
import uuid
import argparse
import parser
import csv

import config
import utils
import json
from pathlib import Path
from google.cloud import bigquery
import type_converter

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

        
def generate_object_mapping():
    """
    Pull the mappings from the Teradata to BigQuery Mapping table in BigQuery
    and build the corresponding Object Mapping.
    """

    object_map = {
    }
    name_map = []

    # Get the records from the mapping table
    with open(config.OBJECT_CSV_FILE, 'r') as object_csv:
        data = object_csv.readlines()
        for line in data:
            data_dict = {}
            line = line.strip()
            line_list = line.split(',')
            key = 0
            for i in line_list:
                data_dict.update({key: i})
                key += 1
        
            teradata_table = data_dict.get(0)
		
            teradata_dataset = data_dict.get(1)

            bigquery_table = data_dict.get(2)

            bigquery_dataset = data_dict.get(3)
				
            name_map.append(
                    {
                        "source": {
                            "type": "RELATION",
                            "database": config.BQMS_DEFAULT_DATABASE,
                            "schema": teradata_dataset,
                            "relation": teradata_table
                        },
                        "target": {
                            "database": config.BQMS_DEFAULT_DATABASE,
                            "schema": bigquery_dataset,
                            "relation": bigquery_table
                        }
        
                    }
                )
        object_csv.close()

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
    #os.environ['BQMS_OBJECT_NAME_MAPPING_PATH'] = str(config.BQMS_OBJECT_MAPPING_FILE)
    #os.system(f"python {Path(Path.cwd(), 'dwh-migration-tools/client/bqms_run/main.py')} --input {config.BQMS_INPUT_FOLDER} --output {config.BQMS_OUTPUT_FOLDER} --config {config.BQMS_CONFIG_FILE} -o {config.BQMS_OBJECT_MAPPING_FILE}")
    os.system(f"python {Path(Path.cwd(), 'dwh-migration-tools/client/bqms_run/main.py')} --input {config.BQMS_INPUT_FOLDER} --output {config.BQMS_OUTPUT_FOLDER} --config {config.BQMS_CONFIG_FILE}")

    # Download all of the transpiled files to the output forder
    os.system(f'gsutil -m -o "GSUtil:parallel_process_count=1" cp -r {BQMS_TRANSLATED_PATH} {config.BQMS_OUTPUT_FOLDER}')


def validate_sqls(client: bigquery.Client, uc4_jobs: list[str],
                  uc4_sql_dependencies: dict):
    """
    We need to validate the SQLs that have been translated. 
    """
    pass


def main():
    """

    """

    logger.info("============================================================")
    logger.info("= Translating SQLs                                         =")
    logger.info("============================================================")
    
    # Create the required folders
    setup()

    # Generate mapping
    generate_object_mapping()
    # Generate the BQMS config.yaml file
    generate_bqms_config()
    
    args_dict = parser.arg_parser()
    
    if os.path.isfile(config.TYPE_CONVERSION_CSV):
        type_converter.generate_type_conversions_config()

    shutil.copy(config.SQL_TO_TRANSLATE, Path(config.BQMS_INPUT_FOLDER, 'test.sql'))
    # Submit the job to the BQMS
    submit_job_to_bqms()

    # Perform the dry-runs
    #validate_sqls(client=bigquery_client, uc4_jobs=uc4_jobs,
    #              uc4_sql_dependencies=uc4_sql_dependencies)
    

if __name__ == "__main__":
    main()
