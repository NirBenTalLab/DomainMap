from __future__ import print_function
import requests
import json
import base64
from string import Template
import argparse
import csv
import untangle
import subprocess
from xml.sax import SAXParseException
import ijson
import string
import multiprocessing

# TODO:
# Build a pdb to unp mapping by using the following steps:
# 0. Pull pdb ids from neo4j
# 1. Get chain res numbering to unp res numbering from http://www.ebi.ac.uk/pdbe/api/mappings/uniprot/:pdbid
# 2. Get observed ranges from http://www.ebi.ac.uk/pdbe/api/pdb/entry/polymer_coverage/:pdbid
# 3. Get mutated residues from http://www.ebi.ac.uk/pdbe/api/pdb/entry/mutated_AA_or_NA/:pdbid
# 4. Commit mapping to neo4j

NEO4J_CREATE_TRAN_URL = "http://localhost:7474/db/data/transaction/commit"
NEO4J_USER_PASS = 'neo4j:Sheshi6'
PDB_UNP_MAP_URL = 'http://www.ebi.ac.uk/pdbe/api/mappings/uniprot/%s'
PDB_OBSERVED_RANGES = 'http://www.ebi.ac.uk/pdbe/api/pdb/entry/polymer_coverage/%s'
PDB_MUTATED_RESIDUES = 'http://www.ebi.ac.uk/pdbe/api/pdb/entry/mutated_AA_or_NA/%s'
GET_PDB_IDS_NEO4J_STATEMENT = {'statements': [{'statement': 'MATCH (p:PDBEntry)-[:SUBCHAIN]-(c:PDBChain) WHERE ((NOT (c)-[:SOURCE]-() OR NOT (c)-[:EXPRESSION]-() ) AND p.obsolete IS NULL ) RETURN DISTINCT p.id'}]}
pdbe_molecules = 'http://www.ebi.ac.uk/pdbe/api/pdb/entry/molecules/'


class Consumer(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                print('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                break
            # print('%s: %s' % (proc_name, next_task))
            answer = next_task()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return

class MapPDBOrganism(object):

    def __init__(self, pdb_id):
        self.pdb_id = pdb_id

    def __call__(self, *args, **kwargs):
        pdb_mol = self.get_pdb_molecules(self.pdb_id)
        if pdb_mol is not None and 200 == pdb_mol.status_code:
            pdbchain_organism = self.process_pdb_molecules(self.pdb_id, pdb_mol.json())
            if pdbchain_organism is not None:
                ret = self.createneo4jmapping(pdbchain_organism, NEO4J_CREATE_TRAN_URL)
                # if 0 == ret:
                    # print("%s finished successfully" % self.pdb_id, pdbchain_organism)
                return ret
            else:
                print("%s pdbchain_organism is None" %self.pdb_id)
        print("%s finished unsuccessfully" % self.pdb_id, pdb_mol)

    def get_pdb_molecules(self, pdb_id):
        try:
            pdbe_res = requests.get(pdbe_molecules+pdb_id, stream=False, timeout=10)
            return pdbe_res
        except requests.exceptions.Timeout as e:
            print(e)
            print("pdb_id %s hit timeout" % pdb_id)
            return None

    def process_pdb_molecules(self, pdb_id, pdb_entry):
        pdbchain_organism = None
        for entity in pdb_entry[pdb_id]:
            if 'molecule_type' in entity and 'source' in entity and 'in_chains' in entity \
                    and 'polypeptide(L)' == entity['molecule_type'] and len(entity['source'])>0:
                pdbchain_organism = {} if pdbchain_organism is None else pdbchain_organism
                for chain in entity['in_chains']:
                    pdbchain_organism[str(pdb_id+"."+chain)] = {'source_organism': {'tax_id': -1}, 'expression_organism': {'tax_id': -1}}
                    if entity['source'][0]['tax_id'] is not None:
                        source_organism = {'tax_id': entity['source'][0]['tax_id']}
                        pdbchain_organism[str(pdb_id+"."+chain)]['source_organism'] = source_organism

                    if entity['source'][0]['expression_host_tax_id'] is not None:
                        pdbchain_organism = {} if pdbchain_organism is None else pdbchain_organism
                        expression_organism = {'tax_id': entity['source'][0]['expression_host_tax_id']}
                        pdbchain_organism[str(pdb_id+"."+chain)]['expression_organism'] = expression_organism
        return pdbchain_organism

    def createneo4jmapping(self, pdbchain_organism, trans_location):
        source_organism_template = 'MATCH (c:PDBChain {id: {pdb_chain_id} }) WITH c' \
                    ' MERGE (o:Organism {tax_id: {source_organism}.tax_id } )' \
                   ' CREATE UNIQUE (c)-[:SOURCE]->(o)'
        expression_organism_template = 'MATCH (c:PDBChain {id: {pdb_chain_id} }) WITH c' \
                    ' MERGE (o:Organism {tax_id: {expression_organism}.tax_id } )' \
                   ' CREATE UNIQUE (c)-[:EXPRESSION]->(o)'
        templates_dict = {'source_organism_template': source_organism_template, 'expression_organism_template': expression_organism_template}
        statments_list = []

        for chain_id in pdbchain_organism:
            parameters = pdbchain_organism[chain_id]
            parameters_dict = {'pdb_chain_id': chain_id}
            for key in parameters:
                parameters_dict[key] = parameters[key]
                statement_dict = {'statement': templates_dict[key+'_template'],'parameters':parameters_dict}
                statments_list.append(statement_dict)

        statements_dict = {'statements': statments_list}

        r = requests.post(trans_location, headers=generateheaders(),data=json.dumps(statements_dict))
        r_obj = json.loads(r.text)
        if r_obj['errors']:
            print(r_obj['errors'])
            return 1
        else:
            return 0

def generateheaders():
        return {'Authorization': base64.b64encode(NEO4J_USER_PASS),
                'Accept': 'application/json; charset=UTF-8',
                'Content-Type': 'application/json',
                'X-Stream': 'true'}

def main():
    # Establish communication queues
    tasks = multiprocessing.JoinableQueue(16)
    results = multiprocessing.Queue()

    # Start consumers
    num_consumers = multiprocessing.cpu_count() * 2
    print('Creating %d consumers' % num_consumers)
    consumers = [ Consumer(tasks, results)
                  for i in xrange(num_consumers) ]
    for w in consumers:
        w.start()

    r = requests.post(NEO4J_CREATE_TRAN_URL, headers=generateheaders(),data=json.dumps(GET_PDB_IDS_NEO4J_STATEMENT),
                      stream=True)
    parser = ijson.parse(r.raw)
    i = 0
    pdb_id_list = []
    for prefix, event, value in parser:
        if (prefix, event) == ('results.item.data.item.row.item', 'string'):
            # process_pdb_id(value)
            tasks.put(MapPDBOrganism(value), True, None)

        # Add a poison pill for each consumer
    for i in xrange(num_consumers):
        tasks.put(None)

    # Wait for all of the tasks to finish
    tasks.join()


if __name__ == "__main__":
    main()


__author__ = 'lab'