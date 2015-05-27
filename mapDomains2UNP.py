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

def main():
    args = parse_args()
    source = args.source
    # csv_destination = args.csv_destination
    # destination = args.destination
    csv_reader = csv.DictReader(source, delimiter="\t")
    domain_unp_mapping = {}
    for row in csv_reader:
        domain_uid = row['Uid']
        domain_id = row['Domain_id']
        chain_ids = get_chain_id(row)
        for chain_id in chain_ids:
            try:
                aligned_uniprot = get_aligned_uniprot(chain_id)
                uniprot_fasta = get_uniprot_fasta(aligned_uniprot)
                domain_fasta = get_domain_fasta(domain_id)
                alignment = get_uniprot_domain_alignment()
                alignment_map = json.loads(alignment);
                if float(alignment_map['sequence_identity']) >= 0.9:
                    neo4j_map_ecod2unp(int(domain_uid), aligned_uniprot, alignment_map['matches'])
                else:
                    print('ECOD entry '+domain_uid+" Doesn't match against UNP entry "+aligned_uniprot+
                          " sequence_identity is: "+alignment_map['sequence_identity']+"\n")
            except SAXParseException as e:
                print('ECOD entry '+domain_uid+" raised a SAX exception while matching against UNP entry\n")
                print(e)
            except TypeError as e:
                print('ECOD entry '+domain_uid+" raised a Type error exception while matching against UNP entry"+aligned_uniprot+"\n sequence_identity: "+str(alignment_map['sequence_identity'])+"\n")
                print(e)

def parse_args():
    parser = argparse.ArgumentParser(description="Import ECOD to pdb and uniprot mapping.")
    # exclusive_group = parser.add_mutually_exclusive_group(required=True)
    # batch_group = exclusive_group.add_argument_group("Batch processing", "description")
    parser.add_argument("-src", "--source", help="Source file with SCOPE IDs", type=file, required=True)
    # parser.add_argument("-csvdst", "--csv_destination", help="CSV mapping file destination", type=argparse.FileType('w'),
    #                    required=True)
    # parser.add_argument("-dst", "--destination", help="Mapping file destination", type=argparse.FileType('w'),
    #                     required=True)
    args = parser.parse_args()
    return args

def get_chain_id(row):
    pdb_id = row['Domain_id'][1:5]
    chain_list = []
    if "." == row['Chain_id']:
        for chain_res in row['PDB_residue_range'].split(","):
            chain_list.append(pdb_id+"."+chain_res.split(":")[0])
    else:
        chain_list.append(pdb_id+"."+row['Chain_id'])
    return chain_list

def get_aligned_uniprot(chain_id):
    obj = untangle.parse(PDB_UNP_ALIGN_URL+chain_id)
    for alignObject in obj.dasalignment.alignment.alignObject:
        if 'UniProt' == alignObject['dbSource']:
            return alignObject['dbAccessionId']

def get_uniprot_fasta(aligned_uniprot):
    fasta = requests.get(UNP_FASTA_URL+aligned_uniprot+'.fasta').text
    unp_fa = open(UNP_FASTA_FILE, 'w')
    print(fasta, file=unp_fa)
    unp_fa.close()

def get_domain_fasta(domain_id):
    fasta = requests.get(ECOD_FASTA_URL+domain_id).text
    ecod_fa = open(ECOD_FASTA_FILE, 'w')
    print(fasta, file=ecod_fa)
    ecod_fa.close()

def get_uniprot_domain_alignment():
    return subprocess.check_output([SWALIGN_CMD+UNP_FASTA_FILE+' '+ECOD_FASTA_FILE, ], shell=True)

def neo4j_map_ecod2unp(domain_id, aligned_uniprot, matches_list):
    createmappings(domain_id, aligned_uniprot, matches_list, NEO4J_CREATE_TRAN_URL)

def createmappings(domain_id, aligned_uniprot, matches_list, trans_location):
    unp_statment_template = 'MATCH (d:Domain {uid: $domain_uid})' \
                ' MERGE (u:UniprotEntry {accession: "$unp_acc"})' \
               ' CREATE UNIQUE (d)-[:MATCHES {uniprot_start: $uniprot_start, uniprot_end: $uniprot_end, ecod_start: $ecod_start, ecod_end: $ecod_end}]->(u)'
    unp_template = Template(unp_statment_template)
    statments_list = []

    for match in matches_list:
        unp_statment = unp_template.substitute(domain_uid=domain_id, unp_acc=aligned_uniprot,
                                               uniprot_start=match['uniprot_start'], uniprot_end=match['uniprot_end'],
                                               ecod_start=match['ecod_start'], ecod_end=match['ecod_end'])
        statments_list.append({'statement': unp_statment})

    statements_dict = {'statements': statments_list}

    r = requests.post(trans_location, headers=generateheaders(),data=json.dumps(statements_dict))
    r_obj = json.loads(r.text)
    if r_obj['errors']:
        print("Errors occured while mapping ecod "+domain_id+" to uniprot "+aligned_uniprot+"\n")
        print(r_obj['errors'])

def generateheaders():
    return {'Authorization': base64.b64encode(NEO4J_USER_PASS),
            'Accept': 'application/json; charset=UTF-8',
            'Content-Type': 'application/json'}

if __name__ == "__main__":
    main()