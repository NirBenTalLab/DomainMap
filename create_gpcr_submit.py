from __future__ import print_function
import json

# files_path = "/specific/a/home/cc/students/math/shlomoyakh/Documents/ecod_100/"
files_path = "/specific/safraf/bental/shlomoyakh/ecod112/hmm/"
output_path = "/specific/safraf/bental/shlomoyakh/ecod112/hhalign/"

with open('/home/lab/Downloads/ecod.alphabundles.reps.json', 'r') as f:
    reps = json.load(f)

# with open('/home/lab/Downloads/ecod.alphabundles.reps.job', 'w') as f:
#     reps_len = len(reps)
#     for i in range(0,reps_len):
#         for j in range(i+1, reps_len):
#             output_name = reps[i]+"."+reps[j]+".hhalign"
#             line = "Arguments = -v 0 -t "+files_path+reps[i][0:4]+"/"+reps[i]+".hhm -i "+files_path+reps[j][0:4]+"/"+reps[j]+".hhm -o "+output_path+output_name+""
#             # f.write("Arguments = -t "+files_path+reps[i]+".hhm -i "+files_path+reps_len[j]+".hhm -o "+files_path+output_name+"\n")
#             # f.write("Queue\n")
#             print(line, file=f)
#             print("Queue", file=f)

with open('/home/lab/Downloads/ecod.alphabundles.reps.dag', 'w') as f:
    reps_len = len(reps)
    parent_line = ""
    for i in range(0,reps_len):
        parent_line = "PARENT"
        for j in range(i+1, reps_len):
            if j == reps_len-1:
                parent_line = parent_line+" CHILD collate"
                print(parent_line, file=f)

            job_name = reps[i]+"."+reps[j]
            output_name = job_name+".hhalign"
            parent_line = parent_line +" job"+job_name
            # line = "Arguments = -v 0 -t "+files_path+reps[i][0:4]+"/"+reps[i]+".hhm -i "+files_path+reps[j][0:4]+"/"+reps[j]+".hhm -o "+output_path+output_name+""
            # f.write("Arguments = -t "+files_path+reps[i]+".hhm -i "+files_path+reps_len[j]+".hhm -o "+files_path+output_name+"\n")
            # f.write("Queue\n")
            template_hmm = files_path+reps[i][0:4]+"/"+reps[i]+".hhm"
            input_hmm = files_path+reps[j][0:4]+"/"+reps[j]+".hhm"
            output_alignment = output_path+output_name
            print("JOB job%s hhalign.submit" %(job_name), file=f)
            print('VARS job%s template=%s input=%s output=%s' %(job_name, template_hmm, input_hmm, output_alignment), file=f)
    print("JOB collate collate.submit", file=f)