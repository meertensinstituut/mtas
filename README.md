# Multi Tier Annotation Search

See [textexploration.github.io/mtas/](https://textexploration.github.io/mtas/) for more documentation and instructions.

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

One of the primary use cases for Mtas, the [Nederlab project](http://www.nederlab.nl/), currently<sup>1</sup> provides access, both in terms of metadata and annotated text, to over 15 million items for search and analysis as specified below. 

|                 | Total          | Mean    | Min   | Max        |
|-----------------|---------------:|--------:|------:|-----------:|
| Solr index size | 1,146 G        | 49.8 G  | 268 k | 163 G      |
| Solr documents  | 15,859,099     | 689,526 | 201   | 3,616,544  |

Collections are added and updated regularly by adding new cores, replacing cores and/or merging new cores with existing ones. Currently, the data is divided over 23 separate cores. For 14,663,457 of these documents, annotated text varying in size from 1 to over 3.5 million words is included:

|                 | Total          | Mean    | Min   | Max        |
|-----------------|---------------:|--------:|------:|-----------:|
| Words           | 9,584,448,067  | 654     | 1     | 3,537,883  |
| Annotations     | 36,486,292,912 | 2,488   | 4     | 23,589,831 |

---
<sup><a name="footnote">1</a></sup> <small>situation january 2017</small>