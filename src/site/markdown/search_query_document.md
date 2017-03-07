# Document

Mtas can produce statistics on used terms for the listed documents. To get this information in Solr requests, besides the parameter to enable [Mtas queries](search_query.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.document            | true   | yes         |

**Lucene**

To get statistics on used terms for the listed documents [directly in Lucene](installation_lucene.html), *ComponentDocument* together with the provided *collect* method can be used.
