import config
from pathlib import Path
import csv


def generate_type_conversions_config():
    """
    Generate the config required to convert all timestamps to datetime
    """
    with open(Path(config.TYPE_CONVERSION_CSV), 'r') as type_csv:
        data = csv.DictReader(type_csv)
        data_dict = {}
        for rows in data:
            row = rows
            data_dict.update(row)

        for key, value in data_dict.items():
            config_file_name = f"{key.lower()}-to-{value.lower()}.config.yaml"
            config_lines = []
            config_lines.append("type: experimental_object_rewriter")
            config_lines.append("global:")
            config_lines.append("  typeConvert:")
            config_lines.append(f"    {key.lower()}:")
            config_lines.append(f"      target: {value}")
            #config_lines.append(f"      timezone: {timezone}")
            with open(Path(config.BQMS_INPUT_FOLDER, config_file_name), 'w+') \
                    as config_file:
                config_file.write('\n'.join(config_lines))
            

