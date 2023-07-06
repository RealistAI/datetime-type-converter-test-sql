import os
import re
import glob

def replace_value():
    os.chdir("./bqms/output/translated")
    for files in glob.glob("*.sql"):
        with open(files, 'r+') as targetfile:
            data = targetfile.readlines()
            lines = []
            for line in data:
                if "'" in line:
                    line = line.split("'")
                    reassemble = []
                    line[1] = re.sub(r"\s+$", "", line[1])
                    line = "'".join(line)
                    print(line)
                lines.append(re.sub("\$pypl-edw*", "pypl-edw", line))
        with open(files, 'w') as targetfile:
                targetfile.write("".join(lines))
