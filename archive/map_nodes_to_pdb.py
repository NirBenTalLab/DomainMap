__author__ = 'lab'

import csv
import argparse
import json

SCOPE_PDB_MAPPING_DIC = ""


def parse_args():
    parser = argparse.ArgumentParser(description="Create scope to pdb mapping.")
    # exclusive_group = parser.add_mutually_exclusive_group(required=True)
    # batch_group = exclusive_group.add_argument_group("Batch processing", "description")
    parser.add_argument("-src", "--source", help="Source file with SCOPE IDs", type=file, required=True)
    parser.add_argument("-dst", "--destination", help="Mapping file destination", type=argparse.FileType('w'),
                        required=True)
    parser.add_argument("-m", "--map", help="SCOPE to PDB mapping file", type=argparse.FileType('rw'),
                        default="scope_pdb_map.json")
    args = parser.parse_args()
    return args


def create_nodes_map(source, destination):
    global SCOPE_PDB_MAPPING_DIC
    scope_reader = csv.reader(source, delimiter=",")
    my_dict = {}
    for row in scope_reader:
        try:
            # pdb_entry = SCOPE_PDB_MAPPING_DIC[row[3]]
            # print pdb_entry
            my_dict[row[3]] = SCOPE_PDB_MAPPING_DIC[row[3]]
        except:
            print "key " + row[3] + " could not be found!"
    json.dump(my_dict, destination, indent=4, separators=(',', ':'))


def main():
    args = parse_args()
    global SCOPE_PDB_MAPPING_DIC
    SCOPE_PDB_MAPPING_DIC = json.load(args.map)
    create_nodes_map(args.source, args.destination)

if __name__ == "__main__":
    main()