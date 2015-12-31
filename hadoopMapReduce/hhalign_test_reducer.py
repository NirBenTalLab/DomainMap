#!/usr/bin/env python2.7

"""A more advanced Reducer, using Python iterators and generators."""
from __future__ import print_function
from itertools import groupby
from operator import itemgetter
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


def hhalign(query, template, idx):
    res = os.path.isfile('/specific/a/home/cc/students/math/shlomoyakh/Documents/hhsuite-2.0.16/bin/hhalign')
    return {'query': query, 'template': template, "id": idx, 'res': res}
    # env = dict(os.environ, HHLIB= HHLIB_ENV)
    # env['PATH'] += PATH_EXTRA
    # query_seq = "%s%09d.seq" % (SEQS_DIR, query)
    # template_seq = "%s%09d.seq" % (SEQS_DIR, template)
    # output_file_name = "/tmp/%s_%s.hhalign" % (query,template)
    # call(["hhalign", "-i", query_seq, "-t", template_seq, "-v", "0", "-o", output_file_name], env=env)
    # line = linecache.getline(output_file_name, 14)
    # os.remove(output_file_name)
    # return {'query': query, 'template': template, 'output': line}


def parse_hhalign(hhalign_result):
    m = line_template.match(hhalign_result['output'])
    try:
        hhalign_result.update(m.groupdict())
        return True
    except AttributeError as e:
        print("Parsing %09d with %09d resulted in %s" %(hhalign_result['query'], hhalign_result['template'], e.message))
        print("hhalign output is: %s" % hhalign_result['output'])
        return False


# def update_hhalign_csv(hhalign_result):
    #with open(HHALIGN_CSV, 'a') as myfile:
    #    myfile.write("%(query)d,%(template)d,%(prob)s,%(evalue)s,%(score)s,%(aligned_cols)s,%(identities)s,%(similarity)s,%(sum_probes)s\n" % hhalign_result)
    # db = bsddb.hashopen(HHALIGN_CSV, 'c')
    # key = "%(query)d_%(template)d" % hhalign_result
    # value = "%(prob)s,%(evalue)s,%(score)s,%(aligned_cols)s,%(identities)s,%(similarity)s,%(sum_probes)s" % hhalign_result
    # db[key] = value
    # db.sync()
    # db.close()

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    # input comes from STDIN (standard input)
    # data = read_mapper_output(sys.stdin, separator=separator)
    # # groupby groups multiple word-count pairs by word,
    # # and creates an iterator that returns consecutive keys and their group:
    # #   current_word - string containing a word (the key)
    # #   group - iterator yielding all ["&lt;current_word&gt;", "&lt;count&gt;"] items
    # idx = 0
    # for queryuid , group in groupby(data, itemgetter(0)):
    #     try:
    #         # total_count = sum(int(count) for current_word, count in group)
    #         # print "%s%s%d" % (current_word, separator, total_count)
    #         for queryuid, templateuid in group:
    #             hhalign_result = hhalign(int(queryuid), int(templateuid), idx)
    #             # if parse_hhalign(hhalign_result):
    #             #     print("%(query)d,%(template)d,%(prob)s,%(evalue)s,%(score)s,%(aligned_cols)s,%(identities)s,%(similarity)s,%(sum_probes)s\n" % hhalign_result)
    #             print("%(query)d,%(template)d,%(id)s, %(res)r" % hhalign_result)
    #             idx += 1
    #     except ValueError:
    #         # count was not a number, so silently discard this item
    #         pass
    idx = 0
    for line in sys.stdin:
        line = line.strip()
        queryuid, templateuid = line.split(separator)
        hhalign_result = hhalign(int(queryuid), int(templateuid), idx)
        print("%(query)d,%(template)d,%(id)s, %(res)r" % hhalign_result)

if __name__ == "__main__":
    main()

__author__ = 'lab'
