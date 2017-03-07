#List

Mtas can retrieve list of hits for Mtas queries within the (filtered) set of documents. To get this information in Solr requests, besides the parameter to enable [Mtas queries](search_query.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.list             | true   | yes         |


**Lucene**

To get a list of hits [directly in Lucene](installation_lucene.html), *ComponentList* together with the provided *collect* method can be used.
