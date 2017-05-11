# Facets

Mtas can produce facets on metadata for Mtas queries. To get this information, in Solr requests, besides the parameter to enable [Mtas queries](search_query.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.facet            | true   | yes         |


**Lucene**

To produce facets on metadata [directly in Lucene](installation_lucene.html), *ComponentFacet* together with the provided *collect* method can be used.