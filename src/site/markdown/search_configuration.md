#Configuration

To enable the use of the functionality from Mtas within Solr search requests, some adjustments have to be made within the solrconfig.xml: the provided Mtas searchComponent, queryParser and requestHandler have to be included.

**Mtas searchComponent**

The `mtas.solr.handler.component.MtasSolrSearchComponent` can be declared with

```console
<searchComponent name="mtas" class="mtas.solr.handler.component.MtasSolrSearchComponent"/>
```

and added to the select requestHandler by inserting the following within the 
`<requestHandler/>` with name `"/select"`:

``` console 
<arr name="last-components">
  <str>mtas</str>
</arr>
```   

This enables the handling of all Mtas specific arguments within a select request to a Solr core.

**Mtas queryParser**

The `mtas.solr.search.MtasSolrCQLQParserPlugin` has to be included to enable the use of [CQL queries](search_parser_cql.html):

```console
<queryParser name="mtas_cql" class="mtas.solr.search.MtasSolrCQLQParserPlugin"/>
``` 

The `mtas.solr.search.MtasSolrJoinQParserPlugin` has to be included to enable the use of [join queries](search_parser_join.html):

```console
<queryParser name="mtas_join" class="mtas.solr.search.MtasSolrJoinQParserPlugin"/>
``` 

**Mtas requestHandler**

Adding the `mtas.solr.handler.MtasRequestHandler` enables additional Mtas functionality that doesn't belong in the select requestHandler.

```console
<requestHandler name="/mtas" class="mtas.solr.handler.MtasRequestHandler" />
```

