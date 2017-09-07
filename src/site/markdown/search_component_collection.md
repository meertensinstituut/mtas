#Collection

Mtas provides a method to join query results, based on (temporary) storing lists of values, and reusing these stored lists or collections into following queries.  

To manage collections, in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.collection       | true   | yes         |

Multiple actions can be performed within the same request. To distinguish them, a unique identifier has to be provided for each of the required operations. 

##Create

To make a new collection based on the set of unique values from one or multiple fieldnames, the `create` action can be used.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.collection.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.collection.\<identifier\>.action        | create   |           | yes         |
| mtas.collection.\<identifier\>.identifier      | \<string\>   | one or more comma separated fieldnames     | yes         |

The values will be restricted to the set occurring within the listed fields for the set of documents matching the request. The provided identifier should be an unique string that can be used later on in other requests to refer to this set of data. Sharding is fully supported, i.e. the values are collected from all participating shards, and stored on both the main core and all these shards.



