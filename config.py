from pathlib import Path
import logging

##############################################################################
# Common Config                                                              #
#############################################################################
LOGGING_LEVEL = logging.INFO
# The CSV File containing a list of UC4 Jobs
UC4_CSV_FILE = Path(Path.cwd(), "input_csvs", "uc4_jobs.csv")

# The GCP Project where the metadata will be stored
METADATA_PROJECT = "michael-gilbert-dev"
METADATA_DATASET = "uc4_conversion_metadata"

# The UC4 XML to JSON conversion stores the JSON in a BigQuery table. 
# This is its name
UC4_JSON_TABLE = f"{METADATA_PROJECT}.{METADATA_DATASET}.uc4_json"

UC4_SQL_REPO_NAME = "UC4_SQL"

# Repo containing the SQLS to be translated.
UC4_SQL_REPO = {
        "path": f"https://github.com/RealistAI/{UC4_SQL_REPO_NAME}.git",
        "branch": "master"
        }

BASE_PATH = Path(Path.home(), "required_repos")
##############################################################################
# Generate Teradata to BigQuery Mapping Config                               #
#############################################################################

# The table that stores the Teradata to BigQuery Mapping
TD_TO_BQ_MAPPING_TABLE = \
        f"{METADATA_PROJECT}.{METADATA_DATASET}.teradata_to_bigquery_mapping"

# A CSV file containing the mapping between business units and datasets
BUSINESS_UNIT_DATASET_MAP_CSV_FILE = Path(Path.cwd(), "input_csvs",
                                          "business_unit_dataset_map.csv")


##############################################################################
# Translate SQL Config                                                       #
#############################################################################

# This is where the Translate SQL will store the dry-run logs.
TRANSLATION_LOG_TABLE = \
        f"{METADATA_PROJECT}.{METADATA_DATASET}.translation_log"

# The GCP Project that houses the BQMS Service
BQMS_PROJECT= "michael-gilbert-dev"

# The name of the bucket that the BQMS can use for its transpilaitons
BQMS_GCS_BUCKET = "gs://dwh_preprocessed"

# A setting that determines the default database for tables if one is not 
# specified in the SQL
BQMS_DEFAULT_DATABASE = "michael-gilbert-dev"

# Specifies whether or not the BQMS should clean up temp files after running
BQMS_CLEAN_UP_TEMP_FILES = "True"

BQMS_FOLDER = Path(Path.home(), "bqms")

# The directory where the BQMS will look for the SQLs that need to be converted
# NOTE: Everything in this directory will be deleted each time the BQMS is run
BQMS_INPUT_FOLDER = Path(BQMS_FOLDER, "input")

# This is where the BQMS will write the transpilations to
# NOTE: Everything in this directory will be deleted each time the BQMS is run
BQMS_OUTPUT_FOLDER = Path(BQMS_FOLDER, "output")


# This is where the BQMS will look for the configuraitons
# NOTE: Everything in this directory will be deleted each time the BQMS is run
BQMS_CONFIG_FOLDER = Path(BQMS_FOLDER, "config")
# This is the main config file used by the BQMS
BQMS_CONFIG_FILE = Path(BQMS_CONFIG_FOLDER , "config.yaml")

# This file will contain the object mapping created by the Translate SQL script
BQMS_OBJECT_MAPPING_FILE = Path(BQMS_CONFIG_FOLDER , "object_mapping.json")


# The Translate SQL script will parse the UC4 Job JSON to determine which 
# SQLs are referenced and, therefore, which SQLS need to be transpiled. This 
# is the root directory where the script will look.
# NOTE: The SQL references in the UC4 jobs have the rest of the file path
SOURCE_SQL_PATH = Path(BASE_PATH, UC4_SQL_REPO_NAME, "teradata_sql")

# After successful translaiton and dry-run, the Translate SQL script will copy
# the SQL files back to this directory.
TARGET_SQL_PATH = Path(BASE_PATH, UC4_SQL_REPO_NAME, "bigquery_sql")

