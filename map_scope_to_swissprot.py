__author__ = 'lab'

import urllib2
import argparse
import json
from xml.dom import minidom

PDB_UNIPROT_MAPPING_URL = "http://www.rcsb.org/pdb/rest/das/pdb_uniprot_mapping/alignment?query="
SCOPE_PDB_MAPPING_DIC = ""


def parse_args():
    parser = argparse.ArgumentParser(description="Map scope id to uniprot accession number.")
    # exclusive_group = parser.add_mutually_exclusive_group(required=True)
    # batch_group = exclusive_group.add_argument_group("Batch processing", "description")
    parser.add_argument("-src", "--source", help="Source file with SCOPE IDs", type=file)
    parser.add_argument("-dst", "--destination", help="Mapping file destination", type=argparse.FileType('w'))
    # parser.add_argument("-i", "--id", help="Map a single SCOPE ID", type=str)
    parser.add_argument("-m", "--map", help="SCOPE to PDB mapping file", type=argparse.FileType('rw'),
                        default="scope_pdb_map.json")
    args = parser.parse_args()
    return args


def return_uniprot_id(pdb_entry):
    pdb_str = (pdb_entry['pdb_id']+'.'+pdb_entry['chain']).upper()
    usock = urllib2.urlopen(PDB_UNIPROT_MAPPING_URL+pdb_str)
    try:
        xml_dom = minidom.parse(usock)
    except:
        return None
    for segment in xml_dom.getElementsByTagName('segment'):
        if pdb_str == segment.attributes["intObjectId"].value:
            continue
        else:
            return segment.attributes["intObjectId"].value


def main():
    args = parse_args()
    global SCOPE_PDB_MAPPING_DIC
    SCOPE_PDB_MAPPING_DIC = json.load(args.map)
    total = len(SCOPE_PDB_MAPPING_DIC.keys())
    counter = 0
    for i in range(total):
    # for key in SCOPE_PDB_MAPPING_DIC.keys():
        if not (i % 100):
            print str(i) + " done out of " + str(total)
        key = SCOPE_PDB_MAPPING_DIC.keys()[i]
        uniprot_id = return_uniprot_id(SCOPE_PDB_MAPPING_DIC[key])
        if uniprot_id is None:
            counter = counter + 1
            print key + " has no uniprot mapping - skipping..."
            continue
        SCOPE_PDB_MAPPING_DIC[key]["uniprot_id"] = uniprot_id
    print str(counter) + " entries have been skipped"
    json.dump(SCOPE_PDB_MAPPING_DIC, args.destination, indent=4, separators=(',', ':'))
    # uniprot_id = return_uniprot_id(SCOPE_PDB_MAPPING_DIC[args.id])
    # print(uniprot_id)
    # SCOPE_PDB_MAPPING_DIC[args.id]["uniprot_id"] = uniprot_id
    # print SCOPE_PDB_MAPPING_DIC[args.id]


if __name__ == "__main__":
    main()