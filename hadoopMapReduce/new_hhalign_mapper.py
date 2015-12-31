from __future__ import print_function
import os
import sys
from pyspark import SparkContext
from itertools import groupby
from operator import itemgetter
import linecache
from subprocess import call
import re




HHLIB_ENV = '/specific/a/home/cc/students/math/shlomoyakh/Documents/hhsuite-2.0.16/lib/hh'
PATH_EXTRA = ':/specific/a/home/cc/students/math/shlomoyakh/Documents/hhsuite-2.0.16/bin:/specific/a/home/cc/students/math/shlomoyakh/Documents/hhsuite-2.0.16/lib/hh/scripts'
SEQS_DIR = '/specific/safraf/bental/shlomoyakh/ecod118/seqs/'
line_template = re.compile(r"^Probab=(?P<prob>[0-9.\-]+)\s*E-value=(?P<evalue>[0-9.e\-]+)\s*Score=(?P<score>[0-9.\-]+)\s*Aligned_cols=(?P<aligned_cols>[0-9.\-]+)\s*Identities=(?P<identities>[0-9.\-]+)%\s*Similarity=(?P<similarity>[0-9.\-]+)\s*Sum_probs=(?P<sum_probes>[0-9.\-]+)\s*$")
HHALIGN_CSV = '/specific/safraf/bental/shlomoyakh/ecod118/hhalign.csv'


def hhalign(uids):
    query, template = uids
    env = dict(os.environ, HHLIB= HHLIB_ENV)
    env['PATH'] += PATH_EXTRA
    query_seq = "%s%09d.seq" % (SEQS_DIR, query)
    template_seq = "%s%09d.seq" % (SEQS_DIR, template)
    output_file_name = "/tmp/%s_%s.hhalign" % (query,template)
    call(["hhalign", "-i", query_seq, "-t", template_seq, "-v", "0", "-o", output_file_name], env=env)
    line = linecache.getline(output_file_name, 14)
    os.remove(output_file_name)
    return parse_hhalign({'query': query, 'template': template, 'output': line})

def parse_hhalign(hhalign_result):
    m = line_template.match(hhalign_result['output'])
    try:
        hhalign_result.update(m.groupdict())
        return ("%(query)d,%(template)d,%(prob)s,%(evalue)s,%(score)s,%(aligned_cols)s,%(identities)s,%(similarity)s,%(sum_probes)s" % hhalign_result)
    except AttributeError as e:
        # print("Parsing %09d with %09d resulted in %s" %(hhalign_result['query'], hhalign_result['template'], e.message))
        # print("hhalign output is: %s" % hhalign_result['output'])
        return ("%(query)d,%(template)d,ERROR" % hhalign_result)


# def hhalign(uids):
#     query, template = uids
#     res = os.path.isfile('/specific/a/home/cc/students/math/shlomoyakh/Documents/hhsuite-2.0.16/bin/hhalign')
#     return "%09d,%09d,%r" %(int(query), int(template), res)


def filterUIDs(uids):
    return uids[0] < uids[1]


def print_input(a, b):
    return ("blank", a[1]+b[1])


def main():
    sc = SparkContext(appName="hhalign")
    uids = sc.textFile("/user/shlomoyakh/ecod118/2reps.csv").map(lambda s: int(s))
    pairs = uids.cartesian(uids).filter(filterUIDs)
    pairs.map(hhalign).saveAsTextFile("/user/shlomoyakh/ecod118/test_5")
    sc.stop()


if __name__ == "__main__":
    main()

__author__ = 'lab'