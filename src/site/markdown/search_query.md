# Query

To perform specific Mtas queries in Solr requests, the following parameter should be used.

| Parameter   |  Value | Obligatory  |
|-------------|--------|-------------|
| mtas        | true   | yes         |

See [statistics](search_query_stats.html), 
[kwic](search_query_kwic.html), [list](search_query_list.html), [document](search_query_document.html), [termvector](search_query_termvector.html), [facet](search_query_facet.html), [group](search_query_group.html) and [prefix](search_query_prefix.html) for more details and examples.

---

**Regular queries**

Besides from specific Mtas queries in Solr requests, also [CQL](search_cql.html) can be used in regular queries by [configuring](search_configuration.html) the Mtas query parser in solrconfig.xml. 

*Example 1*

Search for documents containing the word "de" with a query.

`q={!mtas_cql+field%3D"text"+query%3D"[t%3D\"de\"]"}&fl=*&start=0&rows=0&wt=json&indent=true`

``` json
"response":{"numFound":1664241,"start":0,"docs":[]
  }
```

*Example 2*

Search for documents containing the word "de" with a filter query.

`fq={!mtas_cql+field%3D"text"+query%3D"[t%3D\"de\"]"}&q=*%3A*&fl=*&start=0&rows=0&wt=json&indent=true`

``` json
"response":{"numFound":1664241,"start":0,"docs":[]
  }
```



