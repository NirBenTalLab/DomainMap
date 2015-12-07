__author__ = 'lab'

import json
import re

with open('/home/lab/Code/cdhit/nr100.clstr', 'r') as file:
    cluster = {}
    for line in file:
        if line.startswith('>'):
            cluster_rep = []
            continue
        index = line.find('>')
        if -1 == index:
            continue
        uid = line[index+1:index+10]
        uid = re.sub("[^0-9]", "", uid)
        cluster[uid] = cluster_rep
        if (line.find('*') != -1):
            cluster_rep.append(uid)

with open('/home/lab/Code/cdhit/nr100.clstr.json', 'w') as file:
    json.dump(cluster, file)