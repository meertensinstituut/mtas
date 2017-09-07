#List

Mtas can retrieve list of hits for Mtas queries within the (filtered) set of documents. To get this information, in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.list             | true   | yes         |

List results on multiple spans can be produced within the same request. To distinguish them, a unique identifier has to be provided for each of the required lists.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.list.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.list.\<identifier\>.field       | \<string\>   | Mtas field                      | yes         |
| mtas.list.\<identifier\>.query.type       | \<string\>   | query language: [cql](search_cql.html)  | yes         |
| mtas.list.\<identifier\>.query.value      | \<string\>   | query: [cql](search_cql.html)            | yes         |
| mtas.list.\<identifier\>.query.prefix     | \<string\>   | default prefix            | no         |
| mtas.list.\<identifier\>.query.ignore      | \<string\>   | ignore query: [cql](search_cql.html)            | no         |
| mtas.list.\<identifier\>.query.maximumIgnoreLength      | \<integer\>   | maximum number of succeeding occurrences to ignore            | no         |
| mtas.list.\<identifier\>.prefix       | \<string\>   | comma separated list of prefixes                      | no         |
| mtas.list.\<identifier\>.number       | \<double\>   | maximum number of items in list                         | no         |
| mtas.list.\<identifier\>.start       | \<double\>   | offset for selection of items in list                       | no         |
| mtas.list.\<identifier\>.left       | \<double\>   | number of positions left of hit                      | no         |
| mtas.list.\<identifier\>.right      | \<double\>   | number of positions right of hit                      | no         |
| mtas.list.\<identifier\>.output       | \<string\>   | "token" or "hit"                      | no         |

## Variables

The query may contain one or more variables, and the value(s) of these variables have to be defined 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.list.\<identifier\>.query.variable\<identifier variable\>.name      | \<string\>   | name of variable                 | yes        |
| mtas.list.\<identifier\>.query.variable\<identifier variable\>.value      | \<string\>   | comma separated list of values  | yes        |

---

## Examples
1. [Token](#token) : List of tokens with prefix *t*, *pos* and *s* for adjectives followed by a noun
2. [Hit](#hit) : List of hits with prefix *t*, *pos* and *s* for articles followed by an adjective and a noun
3. [Left and Right](#left-and-right) : List of tokens with prefix *t* and *s* for sentences starting with an article, expanded to the left and the right
---

<a name="token"></a>  

### Token

**Example**  
List with output type *token* and prefixes *t*, *pos* and *s* for adjectives followed by a noun

**CQL**  
`[pos="ADJ"][pos="N"]`

**Request and response**  
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%5Bpos%3D%5C%22ADJ%5C%22%5D%5Bpos%3D%5C%22N%5C%22%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.list=true&mtas.list.0.field=text&mtas.list.0.query.type=cql&mtas.list.0.query.value=%5Bpos%3D%22ADJ%22%5D%5Bpos%3D%22N%22%5D&mtas.list.0.key=adjective%2Bnoun&mtas.list.0.prefix=t%2Cpos%2Cs&mtas.list.0.output=token&mtas.list.0.number=2&mtas.list.0.start=0&mtas.list.0.left=0&mtas.list.0.right=0&fl=%2A&rows=0&wt=json&indent=true`

```json
"mtas":{
    "list":[{
        "key":"adjective+noun",
        "number":2,
        "list":[{
            "documentKey":"44e5620c-011c-11e4-b0ff-51bcbd7c379f",
            "documentHitPosition":0,
            "documentHitTotal":239,
            "documentMinPosition":0,
            "documentMaxPosition":6385,
            "startPosition":29,
            "endPosition":30,
            "tokens":[{
                "mtasId":191,
                "prefix":"t",
                "value":"beknopte",
                "positionStart":29,
                "positionEnd":29,
                "parentMtasId":337},
              {
                "mtasId":197,
                "prefix":"pos",
                "value":"ADJ",
                "positionStart":29,
                "positionEnd":29},
              {
                "mtasId":199,
                "prefix":"t",
                "value":"levensschets",
                "positionStart":30,
                "positionEnd":30,
                "parentMtasId":337},
              {
                "mtasId":204,
                "prefix":"pos",
                "value":"N",
                "positionStart":30,
                "positionEnd":30},
              {
                "mtasId":337,
                "prefix":"s",
                "value":"",
                "positionStart":7,
                "positionEnd":49,
                "parentMtasId":1152}]},
          {
            "documentKey":"44e5620c-011c-11e4-b0ff-51bcbd7c379f",
            "documentHitPosition":1,
            "documentHitTotal":239,
            "documentMinPosition":0,
            "documentMaxPosition":6385,
            "startPosition":56,
            "endPosition":57,
            "tokens":[{
                "mtasId":380,
                "prefix":"t",
                "value":"gebied",
                "positionStart":57,
                "positionEnd":57,
                "parentMtasId":610},
              {
                "mtasId":387,
                "prefix":"pos",
                "value":"N",
                "positionStart":57,
                "positionEnd":57},
              {
                "mtasId":373,
                "prefix":"t",
                "value":"velerlei",
                "positionStart":56,
                "positionEnd":56,
                "parentMtasId":610},
              {
                "mtasId":378,
                "prefix":"pos",
                "value":"ADJ",
                "positionStart":56,
                "positionEnd":56},
              {
                "mtasId":610,
                "prefix":"s",
                "value":"",
                "positionStart":50,
                "positionEnd":90,
                "parentMtasId":1152}]}]}]}
```

                    
<a name="hit"></a>  

### Hit

**Example**  
List with output type *hit* and prefixes *t*, *pos* and *s* for articles followed by an adjective and a noun

**CQL**  
`[pos="LID"][pos="ADJ"][pos="N"]`

**Request and response**
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%5Bpos%3D%5C%22LID%5C%22%5D%5Bpos%3D%5C%22ADJ%5C%22%5D%5Bpos%3D%5C%22N%5C%22%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.list=true&mtas.list.0.field=text&mtas.list.0.query.type=cql&mtas.list.0.query.value=%5Bpos%3D%22LID%22%5D%5Bpos%3D%22ADJ%22%5D%5Bpos%3D%22N%22%5D&mtas.list.0.key=article%2Badjective%2Bnoun&mtas.list.0.prefix=t%2Cpos%2Cs&mtas.list.0.output=hit&mtas.list.0.number=2&mtas.list.0.start=0&mtas.list.0.left=0&mtas.list.0.right=0&fl=%2A&rows=0&wt=json&indent=true`

```json
"mtas":{
    "list":[{
        "key":"article+adjective+noun",
        "number":2,
        "list":[{
            "documentKey":"44e5620c-011c-11e4-b0ff-51bcbd7c379f",
            "documentHitPosition":0,
            "documentHitTotal":80,
            "documentMinPosition":0,
            "documentMaxPosition":6385,
            "startPosition":210,
            "endPosition":212,
            "hit":{
              "210":[["t",
                  "het"],
                ["pos",
                  "LID"],
                ["s",
                  null]],
              "211":[["t",
                  "Middelbaar"],
                ["pos",
                  "ADJ"],
                ["s",
                  null]],
              "212":[["t",
                  "Onderwijs"],
                ["pos",
                  "N"],
                ["s",
                  null]]}},
          {
            "documentKey":"44e5620c-011c-11e4-b0ff-51bcbd7c379f",
            "documentHitPosition":1,
            "documentHitTotal":80,
            "documentMinPosition":0,
            "documentMaxPosition":6385,
            "startPosition":237,
            "endPosition":239,
            "hit":{
              "237":[["t",
                  "het"],
                ["pos",
                  "LID"],
                ["s",
                  null]],
              "238":[["t",
                  "Middelbaar"],
                ["pos",
                  "ADJ"],
                ["s",
                  null]],
              "239":[["t",
                  "Onderwijs"],
                ["pos",
                  "N"],
                ["s",
                  null]]}}]}]}
```

---

<a name="left-and-right"></a>  

### Left and Right

**Example**  
List with output type *token* and prefixes *t* and *s* for sentences starting with an article, expanded two positions to the left and one position to the right

**CQL**  
`<s>[pos="LID"]`

**Request and response** 
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%3Cs%3E%5Bpos%3D%5C%22LID%5C%22%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.list=true&mtas.list.0.field=text&mtas.list.0.query.type=cql&mtas.list.0.query.value=%3Cs%3E%5Bpos%3D%22LID%22%5D&mtas.list.0.key=sentence+starting+with+article&mtas.list.0.prefix=t%2Cs&mtas.list.0.output=token&mtas.list.0.number=2&mtas.list.0.start=0&mtas.list.0.left=2&mtas.list.0.right=1&fl=%2A&rows=0&wt=json&indent=true`

```json
"mtas":{
    "list":[{
        "key":"sentence starting with article",
        "number":2,
        "list":[{
            "documentKey":"44e5620c-011c-11e4-b0ff-51bcbd7c379f",
            "documentHitPosition":0,
            "documentHitTotal":18,
            "documentMinPosition":0,
            "documentMaxPosition":6385,
            "startPosition":378,
            "endPosition":378,
            "tokens":[{
                "mtasId":2534,
                "prefix":"t",
                "value":"leven",
                "positionStart":379,
                "positionEnd":379,
                "parentMtasId":2914},
              {
                "mtasId":2517,
                "prefix":"t",
                "value":".",
                "positionStart":377,
                "positionEnd":377,
                "parentMtasId":2526},
              {
                "mtasId":2527,
                "prefix":"t",
                "value":"Het",
                "positionStart":378,
                "positionEnd":378,
                "parentMtasId":2914},
              {
                "mtasId":2914,
                "prefix":"s",
                "value":"",
                "positionStart":378,
                "positionEnd":433,
                "parentMtasId":2915},
              {
                "mtasId":2512,
                "prefix":"t",
                "value":"Landbouwkundige",
                "positionStart":376,
                "positionEnd":376,
                "parentMtasId":2526},
              {
                "mtasId":2526,
                "prefix":"s",
                "value":"",
                "positionStart":307,
                "positionEnd":377,
                "parentMtasId":2915}]},
          {
            "documentKey":"44e5620c-011c-11e4-b0ff-51bcbd7c379f",
            "documentHitPosition":1,
            "documentHitTotal":18,
            "documentMinPosition":0,
            "documentMaxPosition":6385,
            "startPosition":878,
            "endPosition":878,
            "tokens":[{
                "mtasId":5794,
                "prefix":"t",
                "value":"De",
                "positionStart":878,
                "positionEnd":878,
                "parentMtasId":5999},
              {
                "mtasId":5801,
                "prefix":"t",
                "value":"eerzucht",
                "positionStart":879,
                "positionEnd":879,
                "parentMtasId":5999},
              {
                "mtasId":5999,
                "prefix":"s",
                "value":"",
                "positionStart":878,
                "positionEnd":908,
                "parentMtasId":6305},
              {
                "mtasId":5779,
                "prefix":"t",
                "value":"bewaarheid",
                "positionStart":876,
                "positionEnd":876,
                "parentMtasId":5792},
              {
                "mtasId":5786,
                "prefix":"t",
                "value":".",
                "positionStart":877,
                "positionEnd":877,
                "parentMtasId":5792},
              {
                "mtasId":5792,
                "prefix":"s",
                "value":"",
                "positionStart":857,
                "positionEnd":877,
                "parentMtasId":5793}]}]}]}
```

---

**Lucene**

To get a list of hits [directly in Lucene](installation_lucene.html), *ComponentList* together with the provided *collect* method can be used.
