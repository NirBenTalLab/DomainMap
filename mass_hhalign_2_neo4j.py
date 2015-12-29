from __future__ import print_function
import linecache
import os
import requests
import json
import base64
from string import Template
import argparse
import csv
from subprocess import call
import untangle
import subprocess
from xml.sax import SAXParseException
import ijson
import string
import multiprocessing
import py2neo
import re


HHLIB_ENV = '/home/lab/Code/HHSuite/hhsuite-2.0.16/lib/hh'
PATH_EXTRA = ':/home/lab/Code/HHSuite/hhsuite-2.0.16/bin:/home/lab/Code/HHSuite/hhsuite-2.0.16/lib/hh/scripts'
SEQS_DIR = '/home/lab/Downloads/ecod118/seqs/'

class Consumer(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            # print('%s: %s' % (proc_name, next_task))
            if next_task is None:
                # Poison pill means shutdown
                print('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                self.result_queue.put(None)
                break
            # print('%s: %s' % (proc_name, next_task))
            answer = next_task()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return


class Runnable(object):

    def __init__(self, query, template):
        self.query = query
        self.template = template

    def __call__(self, *args, **kwargs):
        return hhalign(self.query, self.template)

class Neo4jUpdater(multiprocessing.Process):

    def __init__(self, result_queue):
        multiprocessing.Process.__init__(self)
        self.result_queue = result_queue
        self.line_template = re.compile(r"^Probab=(?P<prob>[0-9.]+)\s*E-value=(?P<evalue>[0-9.e-]+)\s*Score=(?P<score>[0-9.]+)\s*Aligned_cols=(?P<aligned_cols>[0-9.]+)\s*Identities=(?P<identities>[0-9.]+)%\s*Similarity=(?P<similarity>[0-9.\-]+)\s*Sum_probs=(?P<sum_probes>[0-9.]+)\s*$")
        self.cypher = py2neo.Graph().cypher

    def run(self):
        proc_name = self.name
        while True:
            self.hhalign_result = self.result_queue.get()
            #TODO: parse result and using py2neo add the result to the graph
            if self.hhalign_result is None:
                # Poison pill means shutdown
                print('%s: Exiting' % proc_name)
                self.result_queue.task_done()
                break
            # print('%s: %s' % (proc_name, next_task))
            if (self.parse_hhalign()):
                self.updatedb()
            self.result_queue.task_done()
        return

    def parse_hhalign(self):
        m = self.line_template.match(self.hhalign_result['output'])
        try:
            self.hhalign_result.update(m.groupdict())
            return True
        except AttributeError as e:
            print("Parsing %09d with %09d resulted in %s" %(self.hhalign_result['query'], self.hhalign_result['template'], e.message))
            print("hhalign output is: %s" % self.hhalign_result['output'])
            return False

    def updatedb(self):
        self.cypher.execute("MATCH (query:ECODDomain), (template:ECODDomain) WHERE query.uid = {query} AND "
                            "template.uid = {template} WITH query, template CREATE UNIQUE (query)-[:HHALIGN {"
                            "prob: toFloat({prob}), evalue: toFloat({evalue}), score: toFloat({score}), aligned_cols: toFloat({aligned_cols}), identities: "
                            "toFloat({identities}),  similarity: toFloat({similarity}), sum_probes: toFloat({sum_probes})}]->(template)",
                            self.hhalign_result)


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

def main():

    # Establish communication queues
    tasks = multiprocessing.JoinableQueue(16)
    results = multiprocessing.JoinableQueue(32)

    # Start consumers
    num_consumers = multiprocessing.cpu_count()
    # num_consumers = 1
    print('Creating %d consumers' % num_consumers)

    hhAligners = [ Consumer(tasks, results)
                  for i in xrange(num_consumers) ]
    dbUpdaters = [ Neo4jUpdater(results)
                  for i in xrange(num_consumers) ]
    consumers = hhAligners + dbUpdaters

    for w in consumers:
        w.start()

    with open('/home/lab/Downloads/ecod118/alpha_bundles/reps_uids.csv', 'r') as file:
        lines = file.read().splitlines()
    for i in range(0, len(lines)):
        for j in range(i+1, len(lines)):
            tasks.put(Runnable(int(float(lines[i])), int(float(lines[j]))))
    # r = requests.post(NEO4J_CREATE_TRAN_URL, headers=generateheaders(),data=json.dumps(GET_PDB_IDS_NEO4J_STATEMENT),
    #                   stream=True)
    # parser = ijson.parse(r.raw)
    # i = 0
    # pdb_id_list = []
    # for prefix, event, value in parser:
    #     if (prefix, event) == ('results.item.data.item.row.item', 'string'):
    #         # process_pdb_id(value)
    #         tasks.put(MapPDBOrganism(value), True, None)
    #TODO: Run over domain ids and create tasks

    # Add a poison pill for each consumer
    for i in xrange(num_consumers):
        tasks.put(None)

    # Wait for all of the tasks to finish
    tasks.join()
    results.join()


if __name__ == "__main__":
    main()

__author__ = 'lab'