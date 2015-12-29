#!/usr/bin/env python2.7

"""A more advanced Mapper, using Python iterators and generators."""

import sys


def read_input(inputfile, separator):
    lines = inputfile.readlines()
    for i in range(0, len(lines)):
        for j in range(i+1, len(lines)):
            yield {'query': lines[i].strip(), 'template': lines[j].strip(), 'separator': separator}
    # for idx, ival in enumerate(inputfile):
    #     # split the line into words
    #     for jdx, jval in enumerate(inputfile[idx+1:]):
    #         yield {'query': ival, 'template': jval, 'separator': separator}


def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin, separator)
    for uid_couple in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print("%(query)s%(separator)s%(template)s") %(uid_couple)

if __name__ == "__main__":
    main()

__author__ = 'lab'
