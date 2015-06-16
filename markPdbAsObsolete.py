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

GET_PDB_OBSOLETE_URL = "http://www.rcsb.org/pdb/rest/getObsolete"
NEO4J_CREATE_TRAN_URL = "http://localhost:7474/db/data/transaction/commit"
NEO4J_USER_PASS = 'neo4j:Sheshi6'
NEO4J_SET_OBSOLETE_PROPERTY_STATEMENT = "MATCH (p:PDBEntry {id: {pdb_id} }) SET p.obsolete=true"

def main():
    obseletes = untangle.parse(GET_PDB_OBSOLETE_URL)
    for pdb_entry in obseletes.obsolete.PDB:
        pdb_id = pdb_entry['structureId']
        mark_as_obsolete(pdb_id.lower())

def mark_as_obsolete(pdb_id):
    statment_dict = {'statement': NEO4J_SET_OBSOLETE_PROPERTY_STATEMENT, 'parameters': {'pdb_id': pdb_id}}
    statements_dict = {'statements': [statment_dict]}

    r = requests.post(NEO4J_CREATE_TRAN_URL, headers=generateheaders(),data=json.dumps(statements_dict))
    r_obj = json.loads(r.text)
    if r_obj['errors']:
        print(r_obj['errors'])

def generateheaders():
    return {'Authorization': base64.b64encode(NEO4J_USER_PASS),
            'Accept': 'application/json; charset=UTF-8',
            'Content-Type': 'application/json',
            'X-Stream': 'true'}

if __name__ == "__main__":
    main()