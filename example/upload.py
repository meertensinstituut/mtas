import json
import sys

import requests # third-party dependency: pip install requests


# Configure these.
BASE_URL = 'http://localhost:8983/solr'
#CORE_NAME = 'corename_not_configured'

# The Mtas content field. The following is used by Nederlab;
# see your Solr core's schema.xml.
#MTAS_FIELD = 'NLContent_mtas'


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

    # Inject the actual content. Replace open by gzip.open if necessary.
    payload[0][MTAS_FIELD] = open(content_path).read()

    # Remove commit=true for extra performance.
    url = BASE_URL + '/' + CORE_NAME + '/update?wt=json&commit=true'

    requests.post(url, json=payload)


if __name__ == '__main__':
    post_document(sys.argv[1], sys.argv[2])
