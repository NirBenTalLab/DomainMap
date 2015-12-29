#!/usr/bin/env python2.7
from __future__ import print_function
import getopt
import linecache
import os
from subprocess import call
import re
import sys
import bsddb


HHLIB_ENV = '/specific/a/home/cc/students/math/shlomoyakh/Documents/hhsuite-2.0.16/lib/hh'
PATH_EXTRA = ':/specific/a/home/cc/students/math/shlomoyakh/Documents/hhsuite-2.0.16/bin:/specific/a/home/cc/students/math/shlomoyakh/Documents/hhsuite-2.0.16/lib/hh/scripts'
SEQS_DIR = '/specific/safraf/bental/shlomoyakh/ecod118/seqs/'
line_template = re.compile(r"^Probab=(?P<prob>[0-9.\-]+)\s*E-value=(?P<evalue>[0-9.e\-]+)\s*Score=(?P<score>[0-9.\-]+)\s*Aligned_cols=(?P<aligned_cols>[0-9.\-]+)\s*Identities=(?P<identities>[0-9.\-]+)%\s*Similarity=(?P<similarity>[0-9.\-]+)\s*Sum_probs=(?P<sum_probes>[0-9.\-]+)\s*$")
HHALIGN_CSV = '/specific/safraf/bental/shlomoyakh/ecod118/hhalign.csv'


def hhalign(query, template):
    env = dict(os.environ, HHLIB= HHLIB_ENV)
    env['PATH'] += PATH_EXTRA
    query_seq = "%s%09d.seq" % (SEQS_DIR, query)
    template_seq = "%s%09d.seq" % (SEQS_DIR, template)
    output_file_name = "/tmp/%s_%s.hhalign" % (query,template)
    call(["hhalign", "-i", query_seq, "-t", template_seq, "-v", "0", "-o", output_file_name], env=env)
    line = linecache.getline(output_file_name, 14)
    os.remove(output_file_name)
    return {'query': query, 'template': template, 'output': line}


def parse_hhalign(hhalign_result):
    m = line_template.match(hhalign_result['output'])
    try:
        hhalign_result.update(m.groupdict())
        return True
    except AttributeError as e:
        print("Parsing %09d with %09d resulted in %s" %(hhalign_result['query'], hhalign_result['template'], e.message))
        print("hhalign output is: %s" % hhalign_result['output'])
        return False


def update_hhalign_csv(hhalign_result):
    #with open(HHALIGN_CSV, 'a') as myfile:
    #    myfile.write("%(query)d,%(template)d,%(prob)s,%(evalue)s,%(score)s,%(aligned_cols)s,%(identities)s,%(similarity)s,%(sum_probes)s\n" % hhalign_result)
    db = bsddb.hashopen(HHALIGN_CSV, 'c')
    key = "%(query)d_%(template)d" % hhalign_result
    value = "%(prob)s,%(evalue)s,%(score)s,%(aligned_cols)s,%(identities)s,%(similarity)s,%(sum_probes)s" % hhalign_result
    db[key] = value
    db.sync()
    db.close()


def main(argv):
    query_uid = -1
    template_uid = -1
    try:
        opts, args = getopt.getopt(argv, "hi:t:", [["i=","t="]])
    except getopt.GetoptError:
        print('hhalign_2_neo4j.py -i <queryUID> -t <templateUID>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('hhalign_2_neo4j.py -i <queryUID> -t <templateUID>')
            sys.exit()
        elif opt in ("-i"):
            query_uid = int(arg)
        elif opt in ("-t"):
            template_uid = int(arg)
    if template_uid < 0 or query_uid < 0:
        print('hhalign_2_neo4j.py -i <queryUID> -t <templateUID>')
        sys.exit(2)
    result = hhalign(query_uid, template_uid)
    if parse_hhalign(result):
        update_hhalign_csv(result)
    else:
        print("Error parsing hhalign output for %09d, %09d" % (query_uid, template_uid))

if __name__ == "__main__":
    main(sys.argv[1:])

__author__ = 'lab'
