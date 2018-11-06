# Multi Tier Annotation Search

See [meertensinstituut.github.io/mtas/](https://meertensinstituut.github.io/mtas/) for more documentation and instructions.

---

If you have Solr with Mtas running, you can POST documents to it using your favorite tool/library.
The documents and their metadata must be JSON-formatted, with fields matching the Solr schema.
The schema may contain a "magical" field of type `mtas.solr.schema.MtasPreAnalyzedField` that is picked
up by Mtas for processing.

E.g., if you have a document in `document.json`, you can upload this with cURL as:

    curl -H "Content-Type: application/json" -XPOST \
        "http://localhost:8983/solr/$CORENAME/update?wt=json&commit=true" -d @document.json

Solr should reply with status 0, as usual. The commit can be omitted for extra performance.

If you have JSON files containing only metadata and data in separate files (say `metadata.json` and
`content.xml`), you need to inject the content into the JSON prior to uploading. The Python script in
`example/upload.py` shows how to do this.

---

One of the primary use cases for Mtas, the [Nederlab project](http://www.nederlab.nl/), currently<sup>1</sup> provides access, both in terms of metadata and annotated text, to over 74 million items for search and analysis as specified below. 

|                 | Total          | Mean      | Min   | Max        |
|-----------------|---------------:|----------:|------:|-----------:|
| Solr index size | 2,715 G        | 60.3 G    | 75 k  | 288 G      |
| Solr documents  | 74,762,559     | 1,661,390 | 119   | 11,912,415 |

Collections are added and updated regularly by adding new cores, replacing cores and/or merging new cores with existing ones. Currently, the data is divided over 44 separate cores. For 41,437,881 of these documents, annotated text varying in size from 1 to over 3.5 million words is included:

|                 | Total           | Mean         | Min   | Max        |
|-----------------|----------------:|-------------:|------:|-----------:|
| Words           | 18,494,454,357  | 446          | 1     | 3,537,883  |
| Annotations     | 95,921,919,849  | 2,314        | 4     | 23,589,831 |


---
<sup><a name="footnote">1</a></sup> <small>situation June 2018</small>

# Copyright and license

Copyright 2017-2018 Koninklijke Nederlandse Academie van Wetenschappen

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
