#Termvector

Mtas can produce termvectors for the (filtered) set of documents. To get this information in Solr requests, besides the parameter to enable [Mtas queries](search_query.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.termvector       | true   | yes         |

**Lucene**

To use termvectors [directly in Lucene](installation_lucene.html), *ComponentTermvector* together with the provided *collect* method can be used. 


