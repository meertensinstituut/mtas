# Multi Tier Annotation Search

See [meertensinstituut.github.io/mtas/](https://meertensinstituut.github.io/mtas/)
or build and run a Docker image providing a demonstration 
scenario with indexing and querying of some sample documents.

```console
docker build -t mtas https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/Dockerfile
docker run -t -i -p 8080:80 --name mtas mtas
```

This will provide a website on port 8080 on the ip of your docker host with 
more information.

---

One of the primary use cases for Mtas, the [Nederlab project](https://www.nederlab.nl/), currently<sup>[1](#footnote1)</sup> provides access, both in terms of metadata and 
annotated text, to over 15 million items for search and analysis as specified below. 

|                 | Total          | Mean    | Min   | Max        |
|-----------------|---------------:|--------:|------:|-----------:|
| Solr index size | 1,146 G        | 49.8 G  | 268 k | 163 G      |
| Solr documents  | 15,859,099     | 689,526 | 201   | 3,616,544  |

Collections are added and updated regularly by adding new cores, replacing cores and/or merging new cores with existing ones. Currently, the data is divided over 23 separate cores. For 14,663,457 of these documents, annotated text varying in size from 1 to over 3.5 million words is included.

|                 | Total          | Mean    | Min   | Max        |
|-----------------|---------------:|--------:|------:|-----------:|
| Words           | 9,584,448,067  | 654     | 1     | 3,537,883  |
| Annotations     | 36,486,292,912 | 2,488   | 4     | 23,589,831 |

---
<a name="footnote1">1</a> : <small>situation january 2017</small>