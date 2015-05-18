__author__ = 'lab'

import csv
import argparse
import requests

NEO4J_URL = "http://localhost:7474"

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
    dom2chainMap = build_dom2chainMap(csv_reader);
    chain2unpMap = build_chain2unpMap(csv_reader);

def build_dom2chainMap(csv_reader):
    dom2chainMap = {}
    chain2unpMap = {}
    for row in csv_reader:
        dom2chainMap[row['uid']] = dom2chainMap[row['uid']] if dom2chainMap[row['uid']] else {}
        chainresarr = row['pdb_range'].split(',')
        for chainres in chainresarr:
            chain,res = chainres.split(':')
            pdbchain = row['ecod_domain_id'][1:4]+chain
            #TODO - init dom2chainMap[row['uid']][pdbchain]
            dom2chainMap[row['uid']][pdbchain] =
            resarr = res.rsplit('-',1)
            #TODO - check if this residue batch is already mapped if it is don't add it to chain to domain mapping

