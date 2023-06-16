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
    config_file_name = "lowercase_to_uppercase.config.yaml"
    config_lines = []
    config_lines.append("type: object_rewriter")
    config_lines.append("global:")
    config_lines.append("  case:")
    config_lines.append("    all: UPPERCASE")

    with open(Path(config.BQMS_INPUT_FOLDER, config_file_name), 'w+') \
            as config_file:
        config_file.write('\n'.join(config_lines))
