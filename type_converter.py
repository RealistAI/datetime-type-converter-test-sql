import config
from pathlib import Path


def generate_type_conversions_config():
    """
    Generate the config required to convert all timestamps to datetime
    """
    with open(Path(config.TYPE_CONVERSION_CSV), 'r') as type_csv:
        data = type_csv.readlines()
        data_dict = {}
        for rows in data:
            rows = rows.strip()
            lst = rows.split(',')
            key = lst[-2]
            value = lst[-1]
            data_dict.update({key:value})
        for key, value in data_dict.items():
            config_lines = []
            config_file_name = f"{key.lower()}-to-{value.lower()}.config.yaml"
            if key.lower() in ['timestamp', 'timestamptz']:
                config_lines.append("type: experimental_object_rewriter")
                config_lines.append("global:")
                config_lines.append("  typeConvert:")
                config_lines.append(f"    {key.lower()}:")
                config_lines.append(f"      target: {value}")
                config_lines.append("      timezone: America/Las_Angeles")

            else:
                config_lines.append("type: experimental_object_rewriter")
                config_lines.append("global:")
                config_lines.append("  typeConvert:")
                config_lines.append(f"    {key.lower()}:")
                config_lines.append(f"      target: {value}")
            
            with open(Path(config.BQMS_INPUT_FOLDER, config_file_name), 'w+') \
                    as config_file:
                config_file.write('\n'.join(config_lines))
   

def generate_custom_cast_config():
    """
    Generate the config required to do custom casts
    """
    with open(Path(config.CUSTOM_CAST_CSV), 'r') as type_csv:
        data = type_csv.readlines()
        config_lines = []
        config_file_name = "change-complex-type.config.yaml"

        config_lines.append("type: object_rewriter")
        config_lines.append("attribute:")

        for rows in data:
            rows = rows.strip()
            lst = rows.split('/')
            db = lst[0]
            tb = lst[1]
            column = lst[2]
            name = lst[3]
            source_to_target = lst[4]
            target_to_source = lst[5]
            
            config_lines.append("  -")
            config_lines.append(f'    match: "{db}.{tb}.{column}.{name}"')
            config_lines.append("    type:")
            config_lines.append("      target: DATE")
            config_lines.append(f"      sourceToTarget: {source_to_target}")
            config_lines.append(f"      targetToSource: {target_to_source}")
        
        with open(Path(config.BQMS_INPUT_FOLDER, config_file_name), 'w+') \
                as config_file:
            config_file.write('\n'.join(config_lines))
        
        

