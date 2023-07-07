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
                lines.append(re.sub("\$pypl-edw*", "pypl-edw", line))
        with open(files, 'w') as targetfile:
                targetfile.write("".join(lines))
