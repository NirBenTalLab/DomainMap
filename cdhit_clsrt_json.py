__author__ = 'lab'

import json
import re
import csv

with open('/home/lab/Downloads/ecod118/ecod.develop118.nr100.clstr', 'r') as file:
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

with open('/home/lab/Downloads/ecod118/ecod.develop118.nr100.clstr.csv', 'w') as file:
    # json.dump(cluster, file)
    csv_writer = csv.DictWriter(file, fieldnames=['domain', 'cluster_rep'])
    csv_writer.writeheader();
    for domain, (rep) in cluster.iteritems():
        csv_writer.writerow({'domain':domain, 'cluster_rep':rep[0]})