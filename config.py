from pathlib import Path
import logging

##############################################################################
# Common Config                                                              #
#############################################################################
LOGGING_LEVEL = logging.INFO
##############################################################################
# Translate SQL Config                                                       #
#############################################################################

# The GCP Project that houses the BQMS Service
BQMS_PROJECT= "michael-gilbert-dev"

# The name of the bucket that the BQMS can use for its transpilaitons
BQMS_GCS_BUCKET = "gs://dwh_preprocessed"

# A setting that determines the default database for tables if one is not 
# specified in the SQL
BQMS_DEFAULT_DATABASE = "michael-gilbert-dev"

# Specifies whether or not the BQMS should clean up temp files after running
BQMS_CLEAN_UP_TEMP_FILES = "True"

BQMS_FOLDER = Path(Path.cwd(), "bqms")

# The directory where the BQMS will look for the SQLs that need to be converted
# NOTE: Everything in this directory will be deleted each time the BQMS is run
BQMS_INPUT_FOLDER = Path(BQMS_FOLDER, "input")

# This is where the BQMS will write the transpilations to
# NOTE: Everything in this directory will be deleted each time the BQMS is run
BQMS_OUTPUT_FOLDER = Path(BQMS_FOLDER, "output")
OBJECT_CSV_INPUT_FOLDER = Path(Path.cwd(), 'input_csv')

# This is where the BQMS will look for the configuraitons
# NOTE: Everything in this directory will be deleted each time the BQMS is run
BQMS_CONFIG_FOLDER = Path(BQMS_FOLDER, "config")
# This is the main config file used by the BQMS
BQMS_CONFIG_FILE = Path(BQMS_CONFIG_FOLDER , "config.yaml")

# This file will contain the object mapping created by the Translate SQL script
BQMS_OBJECT_MAPPING_FILE = Path(BQMS_CONFIG_FOLDER , "object_mapping.json")

SQL_TO_TRANSLATE = Path(Path.cwd(), "translate.sql")
OBJECT_CSV_FILE = Path(OBJECT_CSV_INPUT_FOLDER, "schema_map.csv")
TYPE_CONVERSION_CSV = Path(OBJECT_CSV_INPUT_FOLDER, "type_conversions.csv")
