__author__ = 'lab'

import csv
import argparse
import json

SCOPE_PDB_MAPPING_DIC = None


def parse_args():
    parser = argparse.ArgumentParser(description="Create scope to pdb mapping.")
    # exclusive_group = parser.add_mutually_exclusive_group(required=True)
    # batch_group = exclusive_group.add_argument_group("Batch processing", "description")
    # parser.add_argument("-src", "--source", help="Source file with SCOPE IDs", type=file, required=True)
    # parser.add_argument("-dst", "--destination", help="Mapping file destination", type=argparse.FileType('w'),
    #                     required=True)
    parser.add_argument("-m", "--map", help="SCOPE to PDB mapping file", type=argparse.FileType('rw'),
                        default="nodes_sites_map.json")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    global SCOPE_PDB_MAPPING_DIC
    SCOPE_PDB_MAPPING_DIC = json.load(args.map)
    counter = 0
    empty_sites_counter = 0
    no_sites_counter = 0
    for key in SCOPE_PDB_MAPPING_DIC.keys():
        if "sites" in SCOPE_PDB_MAPPING_DIC[key].keys():
            if len(SCOPE_PDB_MAPPING_DIC[key]["sites"]) > 0:
                counter += 1
            else:
                empty_sites_counter += 1
        else:
            no_sites_counter += 1
    print str(counter) + " nodes out of " + str(len(SCOPE_PDB_MAPPING_DIC.keys())) + " nodes have sites assigned"
    print str(empty_sites_counter) + " nodes out of " + str(len(SCOPE_PDB_MAPPING_DIC.keys())) \
        + " nodes have an empty sites array"
    print str(no_sites_counter) + " nodes out of " + str(len(SCOPE_PDB_MAPPING_DIC.keys())) \
        + " nodes do not have sites assigned"

if __name__ == "__main__":
    main()