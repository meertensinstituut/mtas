#!/usr/bin/env python

# This script indexes documents that are provided in the following way:
# - one directory containing metadata records in JSON
# - one directory containing document contents (e.g., FoLiA) in gzipped XML
#
# The script, when called as python index_multi.py metadata/ contents/,
# indexes each file metadata/$file.json by injecting contents/$file.xml.gz
# into it, if present. It continues to index metadata even when there is
# no content, reflecting the Nederlab use case.

import errno
import gzip
import json
import os
import os.path
import sys

import requests # third-party dependency: pip install requests


# Configure these.
BASE_URL = 'http://localhost:8983/solr'
#CORE_NAME = 'corename_not_configured'

# The Mtas content field. The following is used by Nederlab;
# see your Solr core's schema.xml.
#MTAS_FIELD = 'NLContent_mtas'


UPDATE_URL = BASE_URL + '/' + CORE_NAME + '/update'


def inject_optional_content(payload, content_path):
    try:
        payload[0][MTAS_FIELD] = gzip.open(content_path).read()
    except IOError as e:
        # No such file is ok; all other errors are fatal.
        if e.errno == errno.ENOENT:
            pass
        else:
            raise


def post_document(metadata_path, content_path):
    """POSTs a combined document (metadata + content) to Solr/Mtas."""

    payload = json.load(open(metadata_path))

    # The payload of a Solr update must be a list of documents. Since we
    # upload a single document here, we make sure that payload is a singleton
    # list.
    if isinstance(payload, list):
        assert len(payload) == 1
    else:
        payload = [payload]

    inject_optional_content(payload, content_path)

    resp = requests.post(UPDATE_URL + '?wt=json', json=payload).json()
    if 'error' in resp:
        raise Exception(resp['error'])


def post_multi(metadata_dir, content_dir):
    for filename in os.listdir(metadata_dir):
        base, ext = os.path.splitext(filename)
        if ext != '.json':
            continue

        content = os.path.join(content_dir, base + '.xml.gz')
        filename = os.path.join(metadata_dir, filename)
        print(filename)
        post_document(filename, content)

    commit()


def commit():
    # Update without data commits pending updates.
    requests.get(UPDATE_URL + '?commit=true')


if __name__ == '__main__':
    post_multi(sys.argv[1], sys.argv[2])
