#Sharding

All [mtas queries](search_query.html) support sharding.

**Example**

Query simultaneously for the distribution of the word "de" on two cores

**Request and response** 

`q=*%3A*&mtas=true&mtas.stats=true&mtas.stats.spans=true&mtas.stats.spans.0.field=text&mtas.stats.spans.0.query.0.type=cql&mtas.stats.spans.0.query.0.value=[t%3D"de"]&mtas.stats.spans.0.key=example+-+sharding&mtas.stats.spans.0.type=distribution()&rows=0&wt=json&shards=localhost%3A8080%2Fsolr%2Fcore1%2F%2Clocalhost%3A8080%2Fsolr%2Fcore2%2F%2C&indent=true`

``` json
"mtas":{
    "stats":{
      "spans":[{
          "key":"example - sharding",
          "distribution()":{
            "[0,1819]":2064420,
            "[1820,3639]":224,
            "[3640,5459]":103,
            "[5460,7279]":42,
            "[7280,9099]":10,
            "[9100,10919]":1,
            "[10920,12739]":3,
            "[12740,14559]":1,
            "[14560,16379]":2,
            "[16380,18199]":2}}]}}
```

