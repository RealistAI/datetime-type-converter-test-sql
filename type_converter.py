import config
from pathlib import Path

def generate_timestamp_to_datetime_config(timezone: str):
    """
    Generate the config required to convert all timestamps to datetime
    """
    # We are going to build the config and write it to the input folder
    config_file_name = "timestamp-to-datetime.config.yaml"
    config_lines = []
    config_lines.append("type: experimental_object_rewriter")
    config_lines.append("global:")
    config_lines.append("  typeConvert:")
    config_lines.append("    timestamp:")
    config_lines.append("      target: DATETIME")
    config_lines.append(f"      timezone: {timezone}")

    with open(Path(config.BQMS_INPUT_FOLDER, config_file_name), 'w+') \
            as config_file:
        config_file.write('\n'.join(config_lines))

def generate_lowercase_to_uppercase_config():
    """
    Generate a config required to read a CSV for column names to wrap in 'upper()' so they are treated as not case sensitive.
    
    The CSV is required to be placed in the input_csv folder and named 'columns.csv' in this iteration.
    """
    with open(Path(config.COLUMN_CSV_FILE)) as csv_file:
        data = csv_file.readlines()
        config_file_name = f"column-case-converter.config.yaml"
        config_lines = []
        config_lines.append("type: object_rewriter")
        config_lines.append("attribute:")
        #config_lines.append("  case:")
        #config_lines.append("    all: UPPERCASE")
        
        for line in data:
            line = line.strip()
            columns = line.split(',')

            for column in columns:
                #regex = f"\\b{column}\\b"
                config_lines.append("  -")
                config_lines.append("    match:")
                #config_lines.append('     schema: my_dataset')
                config_lines.append(f'      attribute: {column}')
                config_lines.append("    type:")
                config_lines.append("      target: UPPERCASE")
                #config_lines.append("    database: UPPERCASE")
                #config_lines.append("        attribute: UPPERCASE")

        with open(Path(config.BQMS_INPUT_FOLDER, config_file_name), 'w+') \
            as config_file:
                config_file.write('\n'.join(config_lines))

def generate_char_to_varchar_config():
    config_file_name = "char-to-varchar.config.yaml"
    config_lines = []
    config_lines.append("type: experimental_type_converter")
    config_lines.append("global:")
    config_lines.append("  type_convert:")
    config_lines.append("    CHAR: VARCHAR")

    with open(Path(config.BQMS_INPUT_FOLDER, config_file_name), 'w+') \
        as config_file:
            config_file.write('\n'.join(config_lines))
