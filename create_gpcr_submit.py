from __future__ import print_function
import json

# files_path = "/specific/a/home/cc/students/math/shlomoyakh/Documents/ecod_100/"
files_path = ""

with open('/home/lab/Downloads/ecod.alphabundles.reps.json', 'r') as f:
    reps = json.load(f)

with open('/home/lab/Downloads/ecod.alphabundles.reps.job', 'w') as f:
    reps_len = len(reps)
    for i in range(0,reps_len):
        for j in range(i+1, reps_len):
            output_name = reps[i]+"."+reps[j]+".hhalign"
            line = "Arguments = -v 0 -t "+files_path+reps[i]+".hhm -i "+files_path+reps[j]+".hhm -o "+files_path+output_name+""
            # f.write("Arguments = -t "+files_path+reps[i]+".hhm -i "+files_path+reps_len[j]+".hhm -o "+files_path+output_name+"\n")
            # f.write("Queue\n")
            print(line, file=f)
            print("Queue", file=f)