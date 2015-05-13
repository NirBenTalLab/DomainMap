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
    parser.add_argument("-m", "--map", help="SCOPE to PDB mapping file", type=argparse.FileType('r'),
                        default="nodes_sites_map.json")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    global SCOPE_PDB_MAPPING_DIC
    SCOPE_PDB_MAPPING_DIC = json.load(args.map)
    major_types = []
    minor_types = []
    for key in SCOPE_PDB_MAPPING_DIC.keys():
        if "sites" in SCOPE_PDB_MAPPING_DIC[key].keys() and len(SCOPE_PDB_MAPPING_DIC[key]["sites"]) > 0:
            for site in SCOPE_PDB_MAPPING_DIC[key]["sites"]:
                if site["name"] not in major_types:
                    major_types.append(site["name"])
                if site["desc"] not in minor_types:
                    minor_types.append(site["desc"])

    print major_types
    print minor_types


if __name__ == "__main__":
    main()