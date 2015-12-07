__author__ = 'lab'

import json

reps = {}

with open('/home/lab/Code/cdhit/nr100.clstr.json', 'r') as f:
    cluster = json.load(f)

with open('/home/lab/Downloads/ecod.alphabundles.domains.txt', 'r') as f:
    for line in f:
        try:
            reps[cluster[line.strip('\n')][0]] = 1
        except KeyError as e:
            print "Couldn't find rep for uid "+line

with open('/home/lab/Downloads/ecod.alphabundles.reps.json', 'w') as f:
    json.dump(reps.keys(), f)