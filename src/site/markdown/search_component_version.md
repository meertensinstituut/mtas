#Version

Mtas provides a method to display information about `artifactId`, `groupId`, `version` and `timestamp` from the build.  

To include this information, in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.version          | true   | yes         |


---

**Example**  
Inlcude build information


**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.version=true`

```json
"mtas":{
    "version":{
      "groupId":"org.textexploration.mtas",
      "artifactId":"mtas",
      "version":"7.3.1.2-SNAPSHOT",
      "timestamp":"2018-06-16 12:34"}}
```

### Sharding

Sharding is supported, build information from the shards is automatically included.

**Example**  
Inlcude build information from shards

**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.version=true&shards=localhost:8983/solr/core1,localhost:8983/solr/core2`

```json
"mtas":{
    "version":{
      "groupId":"org.textexploration.mtas",
      "artifactId":"mtas",
      "version":"7.3.1.2-SNAPSHOT",
      "timestamp":"2018-06-16 12:34",
      "shards":[{
          "shard":"localhost:8983/solr/core1",
          "version":{
            "groupId":"org.textexploration.mtas",
            "artifactId":"mtas",
            "version":"7.3.1.2-SNAPSHOT",
            "timestamp":"2018-06-16 12:34"}},
        {
          "shard":"localhost:8983/solr/core2",
          "version":{
            "groupId":"org.textexploration.mtas",
            "artifactId":"mtas",
            "version":"7.3.1.2-SNAPSHOT",
            "timestamp":"2018-06-16 12:34"}}]}}
```


