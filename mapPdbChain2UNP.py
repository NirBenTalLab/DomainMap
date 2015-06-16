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
GET_PDB_IDS_NEO4J_STATEMENT = {'statements': [{'statement': 'MATCH (p:PDBEntry) RETURN p'}]}

def generateheaders():
    return {'Authorization': base64.b64encode(NEO4J_USER_PASS),
            'Accept': 'application/json; charset=UTF-8',
            'Content-Type': 'application/json',
            'X-Stream': 'true'}

def main():
    r = requests.post(NEO4J_CREATE_TRAN_URL, headers=generateheaders(),data=json.dumps(GET_PDB_IDS_NEO4J_STATEMENT))
    parser = ijson.parse(r)
    for prefix, event, value in parser:
        print(prefix, event, value)

if __name__ == "__main__":
    main()


__author__ = 'lab'