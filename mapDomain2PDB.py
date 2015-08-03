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

__author__ = 'lab'

PDB_UNP_ALIGN_URL = 'http://www.rcsb.org/pdb/rest/das/pdb_uniprot_mapping/alignment?query='
UNP_FASTA_URL = 'http://www.uniprot.org/uniprot/'
ECOD_FASTA_URL = 'http://prodata.swmed.edu/ecod/complete/sequence?id='
UNP_FASTA_FILE = "/tmp/unp.fasta"
ECOD_FASTA_FILE = "/tmp/ecod.fasta"
SWALIGN_CMD = "java -jar NWAlign.jar "
NEO4J_CREATE_TRAN_URL = "http://localhost:7474/db/data/transaction/commit"
NEO4J_USER_PASS = 'neo4j:Sheshi6'
ECOD_PDB_RES_MAP_URL = 'http://prodata.swmed.edu/ecod/data/%s/%s.residues.xml'
ECOD_PDB_RES_MAP_FILE = '/home/lab/Downloads/ecod_domain_res/data/ecod/domain_data/%s/%s/%s.residues.xml'

def main():
    args = parse_args()
    source = args.ecod_source
    # csv_destination = args.csv_destination
    # destination = args.destination
    csv_reader = csv.DictReader(source, delimiter=",")
    domain_unp_mapping = {}
    for row in csv_reader:
        try:
            domain_uid = "%09d" % int(row['Uid'])
            domain_id = row['Domain_id']
            pdb_id = domain_id[1:5]
            domain_to_pdb_res_map = create_mapping(domain_uid)
            createneo4jmapping(domain_uid, pdb_id, domain_to_pdb_res_map, NEO4J_CREATE_TRAN_URL)
        except SAXParseException as e:
                print('ECOD entry '+domain_uid+" raised a SAX exception while matching against UNP entry")
                print(e)
        except Exception as e:
                print('ECOD entry '+domain_uid+" raised an exception while matching against UNP entry")
                print(e)

def parse_args():
    parser = argparse.ArgumentParser(description="Import ECOD to pdb and uniprot mapping.")
    # exclusive_group = parser.add_mutually_exclusive_group(required=True)
    # batch_group = exclusive_group.add_argument_group("Batch processing", "description")
    parser.add_argument("-ecod_src", "--ecod_source", help="Source file with ECOD IDs", type=file, required=True)
    # parser.add_argument("-pdb_unp_map", "--pdb_unp_mapping", help="Source file with PDB to UNP residue mapping", type=file, required=True)
    # parser.add_argument("-csvdst", "--csv_destination", help="CSV mapping file destination", type=argparse.FileType('w'),
    #                    required=True)
    # parser.add_argument("-dst", "--destination", help="Mapping file destination", type=argparse.FileType('w'),
    #                     required=True)
    args = parser.parse_args()
    return args

def create_mapping(domain_uid):
    res_mapping = {}
    xml_map = untangle.parse(ECOD_PDB_RES_MAP_URL % (domain_uid ,domain_uid))

    # TODO:
    # Mapping can be to the same chain but skip a few residues.
    # In this case a new match needs to be created!
    # See for example: http://prodata.swmed.edu/ecod/complete/domain/e3pymA2, http://prodata.swmed.edu/ecod/data/000147122/000147122.residues.xml
    # The mapping is split at resdiue 149 to 323 :-(
    for residue in xml_map.domain_residue_doc.residue_list.residue:
        chain_id = residue['chain_id']
        seq_id = int(residue['seq_id'])
        if residue['chain_id'] in res_mapping:
            if seq_id < res_mapping[chain_id]['seq_start']:
                res_mapping[chain_id]['seq_start'] = seq_id
                res_mapping[chain_id]['pdbresnum_start'] = residue['pdb_resnum']
            elif seq_id > res_mapping[chain_id]['seq_end']:
                res_mapping[chain_id]['seq_end'] = seq_id
                res_mapping[chain_id]['pdbresnum_end'] = residue['pdb_resnum']
        else:
            res_mapping[chain_id] = {}
            res_mapping[chain_id]['seq_start'] = seq_id
            res_mapping[chain_id]['pdbresnum_start'] = residue['pdb_resnum']
            res_mapping[chain_id]['seq_end'] = seq_id
            res_mapping[chain_id]['pdbresnum_end'] = residue['pdb_resnum']

    return res_mapping

def createneo4jmapping(domain_uid, pdb_id, res_mapping, trans_location):
    unp_statment_template = 'MATCH (d:Domain {uid: {domain_uid} })' \
                ', (c:PDBChain {id: {pdbchain_id} })' \
               ' CREATE UNIQUE (d)-[:MATCHES {props} ]->(c)'
    statments_list = []

    for chain_id in res_mapping:
        parameters_dict = {'domain_uid': int(domain_uid),'pdbchain_id': pdb_id+"."+chain_id}
        props = {
                'seq_start': res_mapping[chain_id]['seq_start'],
                'pdbresnum_start': res_mapping[chain_id]['pdbresnum_start'],
                'seq_end': res_mapping[chain_id]['seq_end'],
                'pdbresnum_end': res_mapping[chain_id]['pdbresnum_end']
                }
        parameters_dict['props'] = props
        statement_dict = {'statement': unp_statment_template,'parameters':parameters_dict}
        statments_list.append(statement_dict)

    statements_dict = {'statements': statments_list}

    r = requests.post(trans_location, headers=generateheaders(),data=json.dumps(statements_dict))
    r_obj = json.loads(r.text)
    if r_obj['errors']:
        print(r_obj['errors'])

def generateheaders():
    return {'Authorization': base64.b64encode(NEO4J_USER_PASS),
            'Accept': 'application/json; charset=UTF-8',
            'Content-Type': 'application/json',}

if __name__ == "__main__":
    main()