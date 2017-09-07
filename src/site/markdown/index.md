#Multi Tier Annotation Search

In recent years, multiple solutions have come available providing search on huge amounts of plain text and metadata. Scalable searchability on annotated text however still appears to be problematic. Using Mtas, we not only take advantage of the strength from Lucene and Solr, but extend queries with [CQL](search_cql.html) conditions on annotated text

> `[pos="LID"] [pos="ADJ"]? [lemma="amsterdam"]`
>
> `<entity="location/> within (<s/> containing [lemma="utrecht"])`

Parsers for several [document formats](indexing_formats.html) are provided, each with extended possibilities for [configuration](indexing_configuration.html), and advanced query [features](features.html) like [statistics](search_component_stats.html), [termvectors](search_component_termvector.html) and [kwic](search_component_kwic.html) are available.

Source code and releases are available on [GitHub](https://github.com/meertensinstituut/mtas/), see [installation instructions](installation.html) on how to get started.

---

**Nederlab** 

One of the primary use cases for Mtas, the [Nederlab project](http://www.nederlab.nl/), currently<sup>1</sup> provides access, both in terms of metadata and 
annotated text, to over 15 million items for search and analysis as specified below. 

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
