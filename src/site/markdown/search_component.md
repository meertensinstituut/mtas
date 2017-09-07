# Search Component

To perform specific Mtas queries in Solr requests, the following parameter should be used.

| Parameter   |  Value | Obligatory  |
|-------------|--------|-------------|
| mtas        | true   | yes         |

See [statistics](search_component_stats.html), 
[kwic](search_component_kwic.html), [list](search_component_list.html), [document](search_component_document.html), [termvector](search_component_termvector.html), [facet](search_component_facet.html), [group](search_component_group.html), [prefix](search_component_prefix.html) and [collection](search_component_collection.html) for more details and examples.

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



