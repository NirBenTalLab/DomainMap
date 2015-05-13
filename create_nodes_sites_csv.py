__author__ = 'lab'

import csv
import argparse
import json

SCOPE_PDB_MAPPING_DIC = None
CSV_DICT = \
    {
        'id': None,
        'modified residue': None,
        'binding site': None,
        'glycosylation site': None,
        'disulfide bond': None,
        'metal ion-binding site': None,
        'initiator methionine': None,
        'active site': None,
        'cross-link': None,
        'site': None,
        'non-standard amino acid': None
    }

EMPTY_CSV_DICT = \
    {
        'id': None,
        'modified residue': False,
        'binding site': False,
        'glycosylation site': False,
        'disulfide bond': False,
        'metal ion-binding site': False,
        'initiator methionine': False,
        'active site': False,
        'cross-link': False,
        'site': False,
        'non-standard amino acid': False
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Create scope to pdb mapping.")
    # exclusive_group = parser.add_mutually_exclusive_group(required=True)
    # batch_group = exclusive_group.add_argument_group("Batch processing", "description")
    # parser.add_argument("-src", "--source", help="Source file with SCOPE IDs", type=file, required=True)
    parser.add_argument("-dst", "--destination", help="Mapping file destination", type=argparse.FileType('w'),
                        required=True)
    parser.add_argument("-m", "--map", help="SCOPE to PDB mapping file", type=argparse.FileType('r'),
                        default="nodes_sites_map.json")
    args = parser.parse_args()
    return args


def populate_node_sites_dict(node_id, sites):
    if sites is None:
        global EMPTY_CSV_DICT
        EMPTY_CSV_DICT['id'] = node_id
        return EMPTY_CSV_DICT
    else:
        global CSV_DICT
        for site_type in CSV_DICT.keys():
            CSV_DICT[site_type] = False
            for site in sites:
                if site_type == site['name']:
                    CSV_DICT[site_type] = True
                    break
        CSV_DICT['id'] = node_id
        return CSV_DICT


def main():
    global CSV_DICT
    global SCOPE_PDB_MAPPING_DIC

    args = parse_args()
    SCOPE_PDB_MAPPING_DIC = json.load(args.map)
    major_types = []
    csv_writer = csv.DictWriter(args.destination, fieldnames=CSV_DICT.keys(), delimiter=",")
    csv_writer.writeheader()
    for key in SCOPE_PDB_MAPPING_DIC.keys():
        sites = None
        if "sites" in SCOPE_PDB_MAPPING_DIC[key].keys() and len(SCOPE_PDB_MAPPING_DIC[key]["sites"]) > 0:
            sites = SCOPE_PDB_MAPPING_DIC[key]["sites"]
        csv_writer.writerow(populate_node_sites_dict(key, sites))


if __name__ == "__main__":
    main()