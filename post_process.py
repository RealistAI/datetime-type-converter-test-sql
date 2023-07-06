import os
import re
import glob

def replace_value():
    """
    This function currently serves 2 purposes which are tied together when looping through the downloaded sqls:
    1. it removes leftover padding from the char to varchar conversions.
    2. it removes the '$' from the beggining of the '$pypl-edw string artifact.
    These functions were combined so we only loop through the sql once.
    """
    os.chdir("./bqms/output/translated")
    for files in glob.glob("*.sql"):
        with open(files, 'r+') as targetfile:
            data = targetfile.readlines()
            lines = []
            for line in data:
                if "'" in line:
                    line = line.split("'")
                    line[1] = re.sub(r"\s+$", "", line[1])
                    line = "'".join(line)
                lines.append(re.sub("\$pypl-edw*", "pypl-edw", line))
        with open(files, 'w') as targetfile:
                targetfile.write("".join(lines))
