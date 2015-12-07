__author__ = 'lab'
import json
import mmap
import csv

def getProbFromDomains(d1, d2):
    filename = "/home/lab/Downloads/gpcr_hhalign/"+d1+"."+d2+".hhalign"
    try:
        return float(getProbFromFile(filename))
    except IOError as e:
        try:
            filename = "/home/lab/Downloads/gpcr_hhalign/"+d2+"."+d1+".hhalign"
            return float(getProbFromFile(filename))
        except IOError as e2:
            print "Unable to open file"
            print e2
    return 0

def getProbFromFile(filename):
    prob = 0
    with open(filename, 'r') as f:
        s = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        prob_index = s.find("Probab=")
        eval_index = s.find("E-value=")
        if prob_index != -1 and eval_index != -1:
            s.seek(prob_index+7)
            prob = s.read(eval_index-7-prob_index)
        return prob

with open('/home/lab/Downloads/groups2reps.json', 'r') as f:
    groups = json.load(f)

groups_keys = groups.keys()
group_prob_sums = []

for i in range(0, len(groups_keys)):
    g1 = groups_keys[i]
    for j in range(i+1, len(groups_keys)):
        g2 = groups_keys[j]
        name = g1+"."+g2
        size = len(groups[g1]) * len(groups[g2])
        probsum = 0
        for d1 in groups[g1]:
            for d2 in groups[g2]:
                filename = "/home/lab/Downloads/gpcr_hhalign/"+g1+"."+g2+".hhalign"
                probsum = probsum + getProbFromDomains(d1, d2)
        group_prob_sums.append({"name": name, "size": size, "sum": probsum})

print group_prob_sums

with open("/home/lab/Downloads/gpcr_hhalign/groupsums.csv", 'w') as f:
    writer = csv.DictWriter(f, fieldnames=["name", "size", "sum"])
    writer.writeheader()
    writer.writerows(group_prob_sums)