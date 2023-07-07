import os
import re
import glob

def replace_value():
    """
    This function removes the '$' from the beggining of the '$pypl-edw string artifact.
    """
    os.chdir("./bqms/output/translated")
    for files in glob.glob("*.sql"):
        with open(files, 'r+') as targetfile:
            data = targetfile.readlines()
            lines = []
            for line in data:
                #These commented lines are a relic of a different post-processing test
                #if "'" in line:
                #    line = line.split("'")
                #    line[1] = re.sub(r"\s+$", "", line[1])
                #    line = "'".join(line)
                lines.append(re.sub("\$pypl-edw*", "pypl-edw", line))
        with open(files, 'w') as targetfile:
                targetfile.write("".join(lines))
