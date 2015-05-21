__author__ = 'lab'

import csv
import argparse
import requests
import json
import base64
from string import Template

NEO4J_CREATE_TRAN_URL = "http://localhost:7474/db/data/transaction"
NEO4J_USER_PASS = 'neo4j:Sheshi6'

def parse_args():
    parser = argparse.ArgumentParser(description="Import ECOD to pdb and uniprot mapping.")
    # exclusive_group = parser.add_mutually_exclusive_group(required=True)
    # batch_group = exclusive_group.add_argument_group("Batch processing", "description")
    parser.add_argument("-src", "--source", help="Source file with SCOPE IDs", type=file, required=True)
    # parser.add_argument("-csvdst", "--csv_destination", help="CSV mapping file destination", type=argparse.FileType('w'),
    #                    required=True)
    parser.add_argument("-dst", "--destination", help="Mapping file destination", type=argparse.FileType('w'),
                        required=True)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    source = args.source
    # csv_destination = args.csv_destination
    # destination = args.destination
    csv_reader = csv.DictReader(source, delimiter="\t")
    mapDict = build_domMap(csv_reader);
    createNEO4JMapping(mapDict)

def build_domMap(csv_reader):
    dom2chainMap = {}
    dom2unpMap = {}
    for row in csv_reader:
        dom2chainMap[row['uid']] = dom2chainMap[row['uid']] if dom2chainMap[row['uid']] else {}
        getPdbChainResMap(row, dom2chainMap)
        dom2unpMap[row['uid']] = dom2chainMap[row['uid']] if dom2chainMap[row['uid']] else {}
        getUNPResMap(row, dom2unpMap)
    return {'chain': dom2chainMap, 'unp': dom2unpMap}

def getPdbChainResMap(row, dom2chainMap):
    chainresarr = row['pdb_range'].split(',')
    for chainres in chainresarr:
        chain,res = chainres.split(':')
        pdbchain = row['ecod_domain_id'][1:4]+chain
        dom2chainMap[row['uid']][pdbchain] = dom2chainMap[row['uid']][pdbchain] if dom2chainMap[row['uid']][pdbchain] else []
        resarr = res.rsplit('-',1)
        if resarr not in dom2chainMap[row['uid']][pdbchain]:
            dom2chainMap[row['uid']][pdbchain].append(resarr)

def getUNPResMap(row, dom2unpMap):
    unp = row['unp_acc']
    unp_res_arr = row['unp_range'].split('','')
    for unp_res in unp_res_arr:
        dom2unpMap[row['uid']][unp] = dom2unpMap[row['uid']][unp] if dom2unpMap[row['uid']][unp] else []
        resarr = unp_res.rsplit('-',1)
        if resarr not in dom2unpMap[row['uid']][unp]:
            dom2unpMap[row['uid']][unp].append(resarr)

def generateHeaders():
    return {'Authorization': base64.encode(NEO4J_USER_PASS),
            'Accept': 'application/json; charset=UTF-8',
            'Content-Type': 'application/json'}

def createNEO4JMapping(mapdict):
    trans_location = createNEO4JTran()
    createMappings(mapdict, trans_location)
    commitTran(trans_location)

def createNEO4JTran():
    r = requests.post(NEO4J_CREATE_TRAN_URL, headers=generateHeaders())
    return r.headers['Location']

def commitTran(trans_location):
    r = requests.post(trans_location+'/commit', headers=generateHeaders())

def createMappings(mapdict, trans_location):
    pdb_statment_template = 'MATCH (d:Domain {uid: $domain_uid), (c:PDBChain {id: $pdbchain_id}), ' \
               ' CREATE (d)-[:MATCHES {res: $pdb_res}]->(c)'
    unp_statment_template = 'MATCH (d:Domain {uid: $domain_uid), (u:UniprotEntry {accession: $unp_acc)' \
               ' CREATE (d)-[:MATCHES {res: $unp_res}]->(u)'
    pdb_template = Template(pdb_statment_template)
    unp_template = Template(unp_statment_template)
    for domain_uid in mapdict['chain'].keys():
        statments_arr = []
        for pdbchain_id in mapdict['chain'][domain_uid]:
            pdb_statment = pdb_statment.substitute(domain_uid=domain_uid, pdbchain_id=pdbchain_id,
                                                   res=mapdict['chain'][domain_uid][pdbchain_id])
            statments_arr.push({'statement': pdb_statment})
        for unp_acc in mapdict['unp'][domain_uid]:
            unp_statment = unp_statment.substitue(domain_uid=domain_uid, unp_acc=unp_acc,
                                                  res=mapdict['unp'][domain_uid][unp_acc])
            statments_arr.push({'statement': unp_statment})
        statements_dict = {'statements': statments_arr}
        r = requests.post(trans_location, headers=generateHeaders(),data=json.dumps(statements_dict))