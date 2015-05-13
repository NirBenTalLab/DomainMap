__author__ = 'lab'

import csv
import argparse
import json
import re


def parse_args():
    parser = argparse.ArgumentParser(description="Create scope to pdb mapping.")
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
    destination = args.destination
    create_map(source, destination)


def create_map(source, destination):
    scope_reader = csv.reader(source, delimiter="\t")
    # scope_pdb_map_writer = csv.writer(csv_destination, delimiter="\t")
    my_dict = {}
    for row in scope_reader:
        # print row
        if row[3] == "-":
            continue
        pdb_entries = parse_pdb_id_and_chain(row)
        # print ([row[3]] + parsed_pdb)
        # scope_pdb_map_writer.writerow([row[3]] + parsed_pdb)
        for entry in pdb_entries:
            my_dict[row[3]] = entry
    json.dump(my_dict, destination, separators=(',', ':'))


def parse_pdb_id_and_chain(row):
    pdb_entries = []
    pdb_id_chain = row[4].split(' ')
    pdb_id = pdb_id_chain[0]
    amino_acid_chains = pdb_id_chain[1].split(',')
    for aa_chain in amino_acid_chains:
        try:
            chain, amino_acids = aa_chain.split(':', 1)
            if amino_acids:
                first, last = amino_acids.rsplit('-', 1)
            else:
                first = ''
                last = ''
            pdb_entries.append(
                {
                    'pdb_id': pdb_id,
                    'chain': chain,
                    'amino_acids': {
                        'first': first,
                        'last': last
                    }
                }
            )
        except ValueError:
            print "Bad row entry: " + ' '.join(row)
    return pdb_entries


if __name__ == "__main__":
    main()