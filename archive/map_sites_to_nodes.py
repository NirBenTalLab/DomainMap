__author__ = 'lab'

# Pull protein data from uniprot http://www.uniprot.org/uniprot/[uniprot_id].xml
# Pull protein data from pdb http://www.rcsb.org/pdb/protein/[uniprot_id]?type=json

import json
import urllib2
from xml.dom import minidom
import argparse
import contextlib
import socket

SCOPE_PDB_MAPPING_DIC = ""
UNIPROT_XML_URL = "http://www.uniprot.org/uniprot/"
PDB_JSON_URL = "http://www.rcsb.org/pdb/protein/"
UNIPROT_EXPERIMENTAL_EVIDENCE_CODE = "ECO:0000269"


import time
from functools import wraps


def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                except:
                    msg = "Unhandled exception, Retrying in %d seconds..." % (mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


@retry((socket.timeout, urllib2.HTTPError), tries=4, delay=3, backoff=2)
def get_pdb_data(uniprot_id):
    print "Getting pdb data for " + uniprot_id
    with contextlib.closing(urllib2.urlopen(PDB_JSON_URL+uniprot_id+"?type=json", timeout=10)) as usock:
        return json.load(usock)
    # with contextlib.closing(urllib2.urlopen(PDB_JSON_URL+uniprot_id+"?type=json", timeout=10)) as usock:
    #     return json.load(usock)
    # usock = urllib2.urlopen(PDB_JSON_URL+uniprot_id+"?type=json")
    # return json.load(usock)


@retry((socket.timeout, urllib2.HTTPError), tries=4, delay=3, backoff=2)
def get_up_data(uniprot_id):
    print "Getting uniprot data for " + uniprot_id
    with contextlib.closing(urllib2.urlopen(UNIPROT_XML_URL+uniprot_id+".xml", timeout=10)) as usock:
        return minidom.parse(usock)
    # with contextlib.closing(urllib2.urlopen(UNIPROT_XML_URL+uniprot_id+".xml", timeout=10)) as usock:
    #     return minidom.parse(usock)
    # usock = urllib2.urlopen(UNIPROT_XML_URL+uniprot_id+".xml")
    # return minidom.parse(usock)


def match_sites_to_node(scop_node, up_data):
    try:
        print "Matching sites to node " + scop_node["pdb_id"] + scop_node["chain"]
        chain = find_pdb_chain(scop_node, up_data)
        return filter_sites(scop_node, chain, up_data["upsites"])
    except:
        return None



def find_pdb_chain(scop_node, up_data):
    print "Finding pdb chain for node"
    pdb_tracks = up_data["tracks"]
    for track in pdb_tracks:
        if (track["pdbID"].upper() == scop_node["pdb_id"].upper()) and (track["chainID"].upper() == scop_node["chain"].upper()):
            return track


def filter_sites(scop_node, pdb_chain, up_sites):
    print "Filtering sites for node"
    matched_sites = filter_sites_based_on_position(scop_node, pdb_chain, up_sites)
    verified_sites = verify_up_sites_based_on_evidence(matched_sites, scop_node)
    return verified_sites


def verify_up_sites_based_on_evidence(sites, scop_node):
    print "Verifying sites based on evidence type"
    verified_sites = []
    xml_dom = get_up_data(scop_node["uniprot_id"])
    for site in sites:
        evidence_keys = find_site_evidence_key(site, xml_dom)
        if not evidence_keys:
            continue
        evidence_keys = evidence_keys.split(" ")
        for evidence_key in evidence_keys:
            if evidence_key_is_experimental(evidence_key, xml_dom):
                verified_sites.append(site)
                break
    return verified_sites


def find_site_evidence_key(site, xml_dom):
    print "Finding evidence key for site " + site["name"]
    for feature in xml_dom.getElementsByTagName("feature"):
        if feature.getAttribute("type") == site["name"]:
            if site["start"] == site["end"]:
                up_position = None
                for position in feature.getElementsByTagName("position"):
                    try:
                        up_position = position.getAttribute("position")
                    except:
                        continue
                if up_position and int(up_position) == site["start"]:
                    return feature.getAttribute("evidence")
            else:
                start_position = None
                end_position = None
                for position in feature.getElementsByTagName("begin"):
                    try:
                        start_position = position.getAttribute("position")
                    except:
                        continue
                for position in feature.getElementsByTagName("end"):
                    try:
                        end_position = position.getAttribute("position")
                    except:
                        continue
                if start_position and end_position and int(start_position) == site["start"] and int(end_position) == site["end"]:
                    return feature.getAttribute("evidence")
    return None


def evidence_key_is_experimental(evidence_key, xml_dom):
    print "Checking if evidence type is experimental"
    for evidence in xml_dom.getElementsByTagName("evidence"):
        if evidence.getAttribute("key") == evidence_key:
            if evidence.getAttribute("type") == UNIPROT_EXPERIMENTAL_EVIDENCE_CODE:
                return True
    return False


def filter_sites_based_on_position(scop_node, pdb_chain, up_sites):
    print "Filtering sites based on position"
    matched_sites = []
    for site in up_sites["tracks"]:
        start = site["start"]
        end = site["end"]
        if not is_site_scop_valid(start, end, scop_node):
            print "Site " + str(start) + " is scop valid"
            continue
        if start == end:
            if is_site_pdb_valid(start, pdb_chain):
                print "Site " + str(start) + " is pdb valid"
                matched_sites.append(site)
        elif is_site_pdb_valid(start, pdb_chain) and is_site_pdb_valid(end, pdb_chain):
            print "Site " + str(start) + " is pdb valid"
            matched_sites.append(site)
    return matched_sites

def is_site_scop_valid(start, end, scop_node):
    scop_start = scop_node["amino_acids"]["first"]
    if scop_start:
        scop_end = scop_node["amino_acids"]["last"]
        if not (int(scop_start) <= start and end <= int(scop_end)):
            return False
    return True


def is_site_pdb_valid(position, pdb_chain):
    for sub_chain in pdb_chain["ranges"]:
        if sub_chain["start"] <= position <= sub_chain["end"] and "mismatch" in sub_chain and sub_chain["mismatch"] == "true":
            return False
    return True


def parse_args():
    parser = argparse.ArgumentParser(description="Create scope to pdb mapping.")
    parser.add_argument("-dst", "--destination", help="Mapping file destination", type=argparse.FileType('w'),
                        required=True)
    parser.add_argument("-m", "--map", help="SCOPE to PDB mapping file", type=argparse.FileType('r'),
                        default="nodes_uniprot_map.json")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    global SCOPE_PDB_MAPPING_DIC
    SCOPE_PDB_MAPPING_DIC = json.load(args.map)
    total = len(SCOPE_PDB_MAPPING_DIC.keys())
    counter = 0
    for i in range(total):
        if not (i % 100):
            print str(i) + " done out of " + str(total)
        key = SCOPE_PDB_MAPPING_DIC.keys()[i]
        scop_node = SCOPE_PDB_MAPPING_DIC[key]
        try:
            up_id = scop_node["uniprot_id"]
        except KeyError:
            continue
        up_data = get_pdb_data(up_id)
        matched_sites = match_sites_to_node(scop_node, up_data)
        if matched_sites is not None:
            SCOPE_PDB_MAPPING_DIC[key]["sites"] = matched_sites
    json.dump(SCOPE_PDB_MAPPING_DIC, args.destination, indent=4, separators=(',', ':'))


if __name__ == "__main__":
    main()