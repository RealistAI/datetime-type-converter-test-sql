import config
from pathlib import Path


def generate_type_conversions_config():
    """
    Generate the config required to convert all timestamps to datetime
    """
    with open(Path(config.TYPE_CONVERSION_CSV), 'r') as type_csv:
        data = type_csv.readlines()
        data_dict = {}

        complex_config_lines = []
        complex_config_file_name = "change-complex-type.config.yaml"

        complex_config_lines.append("type: object_rewriter")
        complex_config_lines.append("attribute:")
        complex_config_lines.append("")
        
        with open(Path(config.BQMS_INPUT_FOLDER, complex_config_file_name), 'w+') as config_file:
            config_file.write('\n'.join(complex_config_lines))
            

        for rows in data:
            try:
                rows = rows.strip()
                lst = rows.split('|')
                tb = lst[0]
                column = lst[1]
                name = lst[2]
                key = lst[3]
                value = lst[4]
                source_to_target = lst[5]
                target_to_source = lst[6]
                data_dict.update({key:value})
            except Exception as e:
                print(e.args)
                continue
            
            config_lines = []
            config_file_name = f"{key.lower()}-to-{value.lower()}.config.yaml"

            if len(source_to_target) > 0:
                config_lines.append("  -")
                config_lines.append(f'    match: "{config.BQMS_PROJECT}.{tb}.{column}.{name}"')
                config_lines.append("    type:")
                config_lines.append(f"      target: {value}")
                config_lines.append(f"      sourceToTarget: {source_to_target}")
                config_lines.append(f"      targetToSource: {target_to_source}")
                config_lines.append(f"")
    
                with open(Path(config.BQMS_INPUT_FOLDER, complex_config_file_name), 'a+') \
                        as config_file:
                    config_file.write('\n'.join(config_lines))

            elif key.lower() in ['timestamp', 'timestamptz']:
                config_lines.append("type: experimental_object_rewriter")
                config_lines.append("global:")
                config_lines.append("  typeConvert:")
                config_lines.append(f"    {key.lower()}:")
                config_lines.append(f"      target: {value}")
                config_lines.append("      timezone: America/Las_Angeles")
                
                with open(Path(config.BQMS_INPUT_FOLDER, config_file_name), 'w+') \
                        as config_file:
                    config_file.write('\n'.join(config_lines))

            else:
                config_lines.append("type: experimental_object_rewriter")
                config_lines.append("global:")
                config_lines.append("  typeConvert:")
                config_lines.append(f"    {key.lower()}:")
                config_lines.append(f"      target: {value}")
            
                with open(Path(config.BQMS_INPUT_FOLDER, config_file_name), 'w+') \
                        as config_file:
                    config_file.write('\n'.join(config_lines))
   

