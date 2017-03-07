# Grouping

Mtas can group results for Mtas queries within the (filtered) set of documents. To get this information in Solr requests, besides the parameter to enable [Mtas queries](search_query.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.group            | true   | yes         |

**Lucene**

To group results [directly in Lucene](installation_lucene.html), *ComponentGroup* together with the provided *collect* method can be used.
