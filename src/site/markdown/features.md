#Features

Annotational layers and structure are added to the existing [Lucene](https://lucene.apache.org/) approach of creating and searching indexes, and furthermore present an implementation as [Solr](https://lucene.apache.org/solr/) plugin providing both searchability and scalability.

**Indexation of multiple document formats is facilitated**:

* Supports indexing [FoLiA](indexing_formats_folia.html), [TEI](indexing_formats_tei.html), [CRM](indexing_formats_crm.html) and [Sketch](indexing_formats_sketch.html).
* Custom [mapping](indexing_formats.html) from the original document format to the index structure.
* [Configure](search_configuration.html) multiple document formats and mappings within the same core.

**Extension of search capabilities**:

* Supports [CQL](search_cql.html) query language.
* [Statistics](search_stats.html) on number of [words](search_query_stats_positions.html), [tokens](search_query_stats_tokens.html) and [spans](search_query_stats_spans.html).
* Usage of [functions](search_functions.html) to produce statistics for custom defined relations between multiple spans and/or number of words.
* [Facets](search_query_facet.html) with [statistics](search_stats.html) on hits.
* [Kwic](search_query_kwic.html), [Lists](search_query_list.html), [Document](search_query_document.html), [termvectors](search_query_termvector.html) and [grouping](search_query_group.html) for spans.


**Supports existing Solr capabilities**:

* Can be used as plugin for [Apache Solr](http://lucene.apache.org/solr/).
* Supports existing Solr functionality including distributed search with [sharding](search_sharding.html).


