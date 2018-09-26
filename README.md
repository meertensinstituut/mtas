# Multi Tier Annotation Search

See [meertensinstituut.github.io/mtas/](https://meertensinstituut.github.io/mtas/) for more documentation and instructions.

---

A [docker](https://hub.docker.com/r/textexploration/mtas/) image providing a Solr based demonstration scenario with indexing and querying of some sample documents is available. To pull and run

```console
docker pull textexploration/mtas
docker run -t -i -p 8080:80 --name mtas textexploration/mtas
```

Or to build and run

```console
docker build -t mtas https://raw.githubusercontent.com/textexploration/mtas/master/docker/Dockerfile
docker run -t -i -p 8080:80 --name mtas mtas
```

This will provide a website on port 8080 on the ip of your docker host with 
more information. 

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
