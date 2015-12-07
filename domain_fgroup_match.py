__author__ = 'lab'
import json
import subprocess
import mmap

groups = {}

with open('/home/lab/Downloads/ecod.gpcr.reps.json', 'r') as f:
    reps = json.load(f)

for rep in reps:
    with open('/home/lab/Downloads/ecod.develop111.domains.txt', 'r') as f:
        s = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        index = s.find(rep)
        if index != -1:
            s.seek(index)
            line = s.readline()
            group = line.split('\t')[12]
            index = group.find(",")
            if index != -1:
                group = group.split(',')[0]
            if group not in groups:
                groups[group]=[]
            groups[group].append(rep)

    # awk_string = "awk -F'\\\\t' /"+rep.strip()+"/ \'{print $1}\' /home/lab/Downloads/ecod.develop111.domains.txt"
    # # p = subprocess.Popen(["awk", "-F'\\\t'", awk_string, "/home/lab/Downloads/ecod.develop111.domains.txt"], stdout=subprocess.PIPE, shell=True)
    # p = subprocess.Popen(awk_string, stdout=subprocess.PIPE, shell=True)
    # (output, err) = p.communicate()
    # if (err):
    #     print err
    #     break
    # output.strip()
    # if output not in groups:
    #     groups[output]=[]
    # groups[output].append(rep)

with open('/home/lab/Downloads/groups2reps.json', 'w') as f:
    json.dump(groups, f)