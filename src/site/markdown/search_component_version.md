#Version

Mtas provides a method to display information about `artifactId`, `groupId`, `version` and `timestamp` from the build.  

To include this information, in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.version       | true   | yes         |

---

**Example**  
Inlcude Mtas build information


**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.version=true`

```json
"mtas":{
    "version":{
      "groupId":"org.textexploration.mtas",
      "artifactId":"mtas",
      "version":"7.3.1.1-SNAPSHOT",
      "timestamp":"2018-06-10 12:25"}}
```


