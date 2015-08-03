#!/usr/bin/env python


from __future__ import print_function
import requests
import json
import base64

import ijson

import multiprocessing
import match
import os
import sys

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
GET_PDB_IDS_NEO4J_STATEMENT = {'statements': [{'statement': "MATCH (a:XGroup)<-[:BELONGS*]-(d:Domain)-[r:MATCHES]-(c:PDBChain)-[:SUBCHAIN]-(p:PDBEntry) WHERE a.type =~ '(?i).*rossmann-like.*' RETURN d, r, c ORDER BY d.domainID"}]}
PDBE_SS = 'http://www.ebi.ac.uk/pdbe/api/pdb/entry/secondary_structure/'
ECOD_PDB_FILE = 'http://prodata.swmed.edu/ecod/complete/structure?id='


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
            # self.result_queue.put(answer)
        return


class betaSheetOrder(object):

    def __init__(self, data_list, counter):
        self.domain = data_list[0]
        self.mapping = data_list[1]
        self.pdb_chain = data_list[2]["id"]
        self.counter = counter

    def __call__(self, *args, **kwargs):
        # get secondary structure from pdbe http://www.ebi.ac.uk/pdbe/api/pdb/entry/secondary_structure/:pdbid
        pdb_ss = self.get_pdb_ss(self.pdb_chain[0:4])
        if pdb_ss == None:
            return None
        # map SS to domain
        strands = self.get_matching_strands(self.mapping, self.pdb_chain, pdb_ss)

        if len(strands) < 4:
            print("Domain: %s has less than 4 beta-strands - skipping" % self.domain["domainID"], file=sys.stderr)
            return None

        pdb_file = self.download_domain_pdb(self.domain["domainID"])
        if None == pdb_file:
            print("Couldn't download pdb file for domain %s" % self.domain["domainID"], file=sys.stderr)
            return None
        rmsd_1_3 = self.calculate_raw_rmsd(self.pdb_chain[5:6], pdb_file, strands[0], strands[2])
        rmsd_1_4 = self.calculate_raw_rmsd(self.pdb_chain[5:6], pdb_file, strands[0], strands[3])
        rmsd_3_4 = self.calculate_raw_rmsd(self.pdb_chain[5:6], pdb_file, strands[2], strands[3])
        if (None == rmsd_1_3) or (None == rmsd_1_4):
            return None
        beta_strand_order = ""
        if (rmsd_1_3 <= rmsd_1_4) and (rmsd_3_4 <= rmsd_1_4):
            beta_strand_order = "Un rossmann-like order found: 213-456"
            print("%d, %s, %s" %(self.counter, self.domain["domainID"], beta_strand_order), file=sys.stdout)
            sys.stdout.flush()
        else:
            beta_strand_order = "Rossmann-like order found: 321-456"
            print("%d, %s, %s" %(self.counter, self.domain["domainID"], beta_strand_order), file=sys.stdout)
            sys.stdout.flush()

        # print({'domainID': self.domain["domainID"], 'strand_order': beta_strand_order})

        try:
            os.remove(pdb_file)
        except OSError as e:
            print ("Error: %s - %s." % (e.filename,e.strerror))

        return {'domainID': self.domain["domainID"], 'strand_order': beta_strand_order}
        # calculate beta strand RMSD 1 to 4, 1 to 3 by downloading domain pymol file from ecod

    @staticmethod
    def get_pdb_ss(pdb_id):
        counter = 0
        while counter<3:
            try:
                pdbe_ss = requests.get(PDBE_SS+pdb_id, stream=False, timeout=10)
                if 200 == pdbe_ss.status_code:
                    return pdbe_ss.json()
                else:
                    print("couldn't download %s SS, status code: %s" %(pdb_id, pdbe_ss.status_code), file=sys.stderr)
                    counter+=1
            except requests.exceptions.Timeout as e:
                print(e, file=sys.stderr)
                print("pdb_id %s hit timeout" % pdb_id, file=sys.stderr)
                counter+=1

        return None

    @staticmethod
    def get_matching_strands(mapping, pdb_chain, pdb_ss):
        matching_strands = []
        pdb_id = pdb_chain[0:4]
        chain_id = pdb_chain[5:6]
        current_chain = None
        for molecule in pdb_ss[pdb_id]['molecules']:
            for chain in molecule['chains']:
                if chain_id == chain["chain_id"]:
                    current_chain = chain
                    break
        if current_chain is not None:
            for strand in current_chain["secondary_structure"]["strands"]:
                if ( (int(mapping["pdbresnum_start"]) <= int(strand["start"]["author_residue_number"])) &
                         (int(strand["end"]["author_residue_number"]) <= int(mapping["pdbresnum_end"])) ):
                    matching_strands.append(strand)
            return sorted(matching_strands, key=lambda strand: strand["start"]["author_residue_number"])
        return None

    @staticmethod
    def download_domain_pdb(domain_id):
        local_filename = '/tmp/'+domain_id
        counter = 0
        while counter<3:
            try:
                r = requests.get(ECOD_PDB_FILE+domain_id, stream=True)
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                            f.flush()
                return local_filename
            except Exception as e:
                print(e, file=sys.stderr)
                print("Couldn't download pdb file for %s" % domain_id, file=sys.stderr)
                counter+=1
        return None

    @staticmethod
    def calculate_raw_rmsd(chain_id,pdb_file, strand_1, strand_2):
        strand_1_res = [("%s:%d" %(chain_id, strand_1["start"]["author_residue_number"]), "%s:%d" %(chain_id, strand_1["end"]["author_residue_number"]))] # '"[(\\\'%s:%s\\\', \\\'%s:%s\\\')]"' %( chain_id, strand_1["start"]["residue_number"], chain_id, strand_1["end"]["residue_number"])
        strand_2_res = [("%s:%d" %(chain_id, strand_2["start"]["author_residue_number"]), "%s:%d" %(chain_id, strand_2["end"]["author_residue_number"]))] # '"[(\\\'%s:%s\\\', \\\'%s:%s\\\')]"' %( chain_id, strand_2["start"]["residue_number"], chain_id, strand_2["end"]["residue_number"])
        try:
            return match.get_raw_rmsd(pdb_file, pdb_file, strand_1_res, strand_2_res, ['CA']);
        except Exception as e:
            print(e, file=sys.stderr)
            print("Encoutred an error computing rmsd for %s " %(pdb_file), file=sys.stderr)
            return None
#
# class MapPDBOrganism(object):
#
#     def __init__(self, pdb_id):
#         self.pdb_id = pdb_id
#
#     def __call__(self, *args, **kwargs):
#         pdb_mol = self.get_pdb_molecules(self.pdb_id)
#         if pdb_mol is not None and 200 == pdb_mol.status_code:
#             pdbchain_organism = self.process_pdb_molecules(self.pdb_id, pdb_mol.json())
#             if pdbchain_organism is not None:
#                 ret = self.createneo4jmapping(pdbchain_organism, NEO4J_CREATE_TRAN_URL)
#                 # if 0 == ret:
#                     # print("%s finished successfully" % self.pdb_id, pdbchain_organism)
#                 return ret
#             else:
#                 print("%s pdbchain_organism is None" %self.pdb_id)
#         print("%s finished unsuccessfully" % self.pdb_id, pdb_mol)
#
#     @staticmethod
#     def get_pdb_molecules(pdb_id):
#         try:
#             pdbe_res = requests.get(pdbe_molecules+pdb_id, stream=False, timeout=10)
#             return pdbe_res
#         except requests.exceptions.Timeout as e:
#             print(e)
#             print("pdb_id %s hit timeout" % pdb_id)
#             return None
#
#     def process_pdb_molecules(self, pdb_id, pdb_entry):
#         pdbchain_organism = None
#         for entity in pdb_entry[pdb_id]:
#             if 'molecule_type' in entity and 'source' in entity and 'in_chains' in entity \
#                     and 'polypeptide(L)' == entity['molecule_type'] and len(entity['source'])>0:
#                 pdbchain_organism = {} if pdbchain_organism is None else pdbchain_organism
#                 for chain in entity['in_chains']:
#                     pdbchain_organism[str(pdb_id+"."+chain)] = {'source_organism': {'tax_id': -1}, 'expression_organism': {'tax_id': -1}}
#                     if entity['source'][0]['tax_id'] is not None:
#                         source_organism = {'tax_id': entity['source'][0]['tax_id']}
#                         pdbchain_organism[str(pdb_id+"."+chain)]['source_organism'] = source_organism
#
#                     if entity['source'][0]['expression_host_tax_id'] is not None:
#                         pdbchain_organism = {} if pdbchain_organism is None else pdbchain_organism
#                         expression_organism = {'tax_id': entity['source'][0]['expression_host_tax_id']}
#                         pdbchain_organism[str(pdb_id+"."+chain)]['expression_organism'] = expression_organism
#         return pdbchain_organism
#
#     def createneo4jmapping(self, pdbchain_organism, trans_location):
#         source_organism_template = 'MATCH (c:PDBChain {id: {pdb_chain_id} }) WITH c' \
#                     ' MERGE (o:Organism {tax_id: {source_organism}.tax_id } )' \
#                    ' CREATE UNIQUE (c)-[:SOURCE]->(o)'
#         expression_organism_template = 'MATCH (c:PDBChain {id: {pdb_chain_id} }) WITH c' \
#                     ' MERGE (o:Organism {tax_id: {expression_organism}.tax_id } )' \
#                    ' CREATE UNIQUE (c)-[:EXPRESSION]->(o)'
#         templates_dict = {'source_organism_template': source_organism_template, 'expression_organism_template': expression_organism_template}
#         statments_list = []
#
#         for chain_id in pdbchain_organism:
#             parameters = pdbchain_organism[chain_id]
#             parameters_dict = {'pdb_chain_id': chain_id}
#             for key in parameters:
#                 parameters_dict[key] = parameters[key]
#                 statement_dict = {'statement': templates_dict[key+'_template'],'parameters':parameters_dict}
#                 statments_list.append(statement_dict)
#
#         statements_dict = {'statements': statments_list}
#
#         r = requests.post(trans_location, headers=generateheaders(),data=json.dumps(statements_dict))
#         r_obj = json.loads(r.text)
#         if r_obj['errors']:
#             print(r_obj['errors'])
#             return 1
#         else:
#             return 0
#


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
    print('Creating %d consumers' % num_consumers, file=sys.stderr)
    consumers = [ Consumer(tasks, results)
                  for i in xrange(num_consumers) ]
    for w in consumers:
        w.start()

    r = requests.post(NEO4J_CREATE_TRAN_URL, headers=generateheaders(),data=json.dumps(GET_PDB_IDS_NEO4J_STATEMENT),
                      stream=True)
    parser = ijson.parse(r.raw)
    i = 0

    buildingObject = False
    pdb_id_list = []
    builder = None
    counter = 0
    for prefix, event, value in parser:
        if (not buildingObject) & ((prefix, event) == ('results.item.data.item', 'start_map')):
            buildingObject = True
            builder = ijson.ObjectBuilder()
            builder.event(event, value)
        elif buildingObject & ((prefix, event) == ('results.item.data.item', 'end_map')):
            buildingObject = False
            builder.event(event, value)
            # put builder.value as object to work on
            # process_pdb_id(value)
            tasks.put(betaSheetOrder(builder.value["row"], counter), True, None)
            counter+=1
        elif buildingObject:
            builder.event(event, value)

    print("%d domains found" %counter, file=sys.stderr)
        # Add a poison pill for each consumer
    for i in xrange(num_consumers):
        tasks.put(None)

    # Wait for all of the tasks to finish
    tasks.join()

    # result = results.get()
    # while result:
    #     print('Result:', result)
    #     result = results.get()


if __name__ == "__main__":
    main()


__author__ = 'lab'