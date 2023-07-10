import config
import csv
from pathlib import Path


def generate_type_conversions_config():
    """
    Generate the correct configuration to convert the datatypes as specified
    in the TYPE_CONVERSION_CSV file.
    """
    config_file_name = "column-type-mapping.config.yaml"
    with open(Path(config.TYPE_CONVERSION_CSV), 'r') as type_csv:

        data = csv.reader(type_csv, delimiter=',', quotechar='"')


        config_lines = []

        config_lines.append("type: object_rewriter")
        config_lines.append("attribute:")

        for row in data:
            assert len(row) == 7, "The type_conversions csv is expected to "\
                    f"have seven elements, but this row has {len(row)}:\n{row}"

            database = row[0]
            table = row[1]
            column = row[2]
            _ = row[3]
            target_type = row[4]
            source_to_target = row[5]
            target_to_source = row[6]

            # Creating a complex mapping
            match = f"{config.BQMS_PROJECT}.{database}.{table}.{column}"
            config_lines.append("  -")
            config_lines.append(f'    match: "{match}"')
            config_lines.append("    type:")
            config_lines.append(f"      target: {target_type}")

            # Generate the sourceToTarget lines if required
            if source_to_target != "" and target_to_source != "":
                config_lines.append(f"      sourceToTarget: {source_to_target}")
                config_lines.append(f"      targetToSource: {target_to_source}")

    config_lines.append("")
    config_file_path = Path(config.BQMS_INPUT_FOLDER, config_file_name)

    # Write the file to disk
    with open(config_file_path, 'w+') as config_file:
        config_file.write('\n'.join(config_lines))


def generate_partition_config():
    """
    Generate the proper configuration to partition a table by a specified column
    """
    config_file_name = "partition-mapping.config.yaml"
    with open(Path(config.PARTITION_CSV), 'r') as partition_csv:
        data = csv.reader(partition_csv, delimiter=',')

        config_lines = []

        config_lines.append("type: object_rewriter")
        config_lines.append("relation:")

        for row in data:
            assert len(row) == 3, "The partition csv is expected to have"\
                    f"3 elements, but this row has {len(row)}:\n{row}"

            database = row[0]
            table = row[1]
            column = row[2]

            # Creating partition mapping
            match = f"{config.BQMS_PROJECT}.{database}.{table}"
            config_lines.append("  -")
            config_lines.append(f'    match: "{match}"')
            config_lines.append("    partition:")
            config_lines.append("      simple:")
            config_lines.append(f"        add: [{column}]")
        
        config_lines.append("")
        config_file_path = Path(config.BQMS_INPUT_FOLDER, config_file_name)
        
        # Write the file to disk
        with open(config_file_path, 'w+') as config_file:
            config_file.write('\n'.join(config_lines))



if __name__ == "__main__":
    generate_type_conversions_config()
    generate_partition_config()
