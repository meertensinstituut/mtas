# Kwic

Mtas can produce keywords in context (kwic) for Mtas queries within the listed documents. To get this information, in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.kwic             | true   | yes         |

Keyword in context results on multiple spans can be produced within the same request. To distinguish them, a unique identifier has to be provided for each of the required kwics. 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.kwic.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.kwic.\<identifier\>.field       | \<string\>   | Mtas field                      | yes         |
| mtas.kwic.\<identifier\>.query.type       | \<string\>   | query language: [cql](search_cql.html)  | yes         |
| mtas.kwic.\<identifier\>.query.value      | \<string\>   | query: [cql](search_cql.html)            | yes         |
| mtas.kwic.\<identifier\>.query.prefix     | \<string\>   | default prefix            | no         |
| mtas.kwic.\<identifier\>.query.ignore      | \<string\>   | ignore query: [cql](search_cql.html)            | no         |
| mtas.kwic.\<identifier\>.query.maximumIgnoreLength      | \<integer\>   | maximum number of succeeding occurrences to ignore            | no         |
| mtas.kwic.\<identifier\>.prefix       | \<string\>   | comma separated list of prefixes                      | no         |
| mtas.kwic.\<identifier\>.number       | \<double\>   | maximum number for selection of items for each document                      | no         |
| mtas.kwic.\<identifier\>.start       | \<double\>   | offset for selection of items for each document                       | no         |
| mtas.kwic.\<identifier\>.left       | \<double\>   | number of positions left of hit                      | no         |
| mtas.kwic.\<identifier\>.right      | \<double\>   | number of positions right of hit                      | no         |
| mtas.kwic.\<identifier\>.output       | \<string\>   | "token" or "hit"                      | no         |


## Variables

The query may contain one or more variables, and the value(s) of these variables have to be defined 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.kwic.\<identifier\>.query.variable.\<identifier variable\>.name      | \<string\>   | name of variable                 | yes        |
| mtas.kwic.\<identifier\>.query.variable.\<identifier variable\>.value      | \<string\>   | comma separated list of values  | yes        |

---

## Examples
1. [Token](#token) : List of tokens with prefix *t*, *pos* and *s* for adjectives followed by a noun
2. [Hit](#hit) : List of hits with prefix *t*, *pos* and *s* for articles followed by an adjective and a noun
3. [Left and Right](#left-and-right) : List of tokens with prefix *t* and *s* for sentences starting with an article, expanded to the left and the right
---

<a name="token"></a>  

### Token

**Example**  
Keyword in context with output type *token* and prefixes *t*, *pos* and *s* for adjectives followed by a noun

**CQL**  
`[pos="ADJ"][pos="N"]`

**Request and response**  
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%5Bpos%3D%5C%22ADJ%5C%22%5D%5Bpos%3D%5C%22N%5C%22%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.kwic=true&mtas.kwic.0.field=text&mtas.kwic.0.query.type=cql&mtas.kwic.0.query.value=%5Bpos%3D%22ADJ%22%5D%5Bpos%3D%22N%22%5D&mtas.kwic.0.key=adjective%2Bnoun&mtas.kwic.0.prefix=t%2Cpos%2Cs&mtas.kwic.0.output=token&mtas.kwic.0.number=2&mtas.kwic.0.start=0&mtas.kwic.0.left=0&mtas.kwic.0.right=0&fl=%2A&start=0&rows=1&wt=json&indent=true`

```json
"mtas":{
    "kwic":[{
        "key":"adjective+noun",
        "list":[{
            "documentKey":"61d2a1b3-9068-4815-ba4d-3370e5a809d7",
            "documentTotal":31,
            "documentMinPosition":0,
            "documentMaxPosition":673,
            "list":[{
                "startPosition":0,
                "endPosition":1,
                "tokens":[{
                    "mtasId":8,
                    "prefix":"t",
                    "value":"fusiebedrijf",
                    "positionStart":1,
                    "positionEnd":1,
                    "parentMtasId":81},
                  {
                    "mtasId":15,
                    "prefix":"pos",
                    "value":"N",
                    "positionStart":1,
                    "positionEnd":1},
                  {
                    "mtasId":81,
                    "prefix":"s",
                    "value":"",
                    "positionStart":0,
                    "positionEnd":8,
                    "parentMtasId":82},
                  {
                    "mtasId":0,
                    "prefix":"t",
                    "value":"Nieuw",
                    "positionStart":0,
                    "positionEnd":0,
                    "parentMtasId":81},
                  {
                    "mtasId":5,
                    "prefix":"pos",
                    "value":"ADJ",
                    "positionStart":0,
                    "positionEnd":0}]},
              {
                "startPosition":5,
                "endPosition":6,
                "tokens":[{
                    "mtasId":45,
                    "prefix":"t",
                    "value":"Belgische",
                    "positionStart":5,
                    "positionEnd":5,
                    "parentMtasId":81},
                  {
                    "mtasId":51,
                    "prefix":"pos",
                    "value":"ADJ",
                    "positionStart":5,
                    "positionEnd":5},
                  {
                    "mtasId":55,
                    "prefix":"t",
                    "value":"energiemarkt",
                    "positionStart":6,
                    "positionEnd":6,
                    "parentMtasId":81},
                  {
                    "mtasId":62,
                    "prefix":"pos",
                    "value":"N",
                    "positionStart":6,
                    "positionEnd":6},
                  {
                    "mtasId":81,
                    "prefix":"s",
                    "value":"",
                    "positionStart":0,
                    "positionEnd":8,
                    "parentMtasId":82}]}]}]}]}
```

                    
<a name="hit"></a>  

### Hit

**Example**  
Keyword in context with output type *hit* and prefixes *t*, *pos* and *s* for articles followed by an adjective and a noun

**CQL**  
`[pos="LID"][pos="ADJ"][pos="N"]`

**Request and response**
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%5Bpos%3D%5C%22LID%5C%22%5D%5Bpos%3D%5C%22ADJ%5C%22%5D%5Bpos%3D%5C%22N%5C%22%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.kwic=true&mtas.kwic.0.field=text&mtas.kwic.0.query.type=cql&mtas.kwic.0.query.value=%5Bpos%3D%22LID%22%5D%5Bpos%3D%22ADJ%22%5D%5Bpos%3D%22N%22%5D&mtas.kwic.0.key=article%2Badjective%2Bnoun&mtas.kwic.0.prefix=t%2Cpos%2Cs&mtas.kwic.0.output=hit&mtas.kwic.0.number=2&mtas.kwic.0.start=0&mtas.kwic.0.left=0&mtas.kwic.0.right=0&fl=%2A&start=0&rows=1&wt=json&indent=true`

```json
"mtas":{
    "kwic":[{
        "key":"article+adjective+noun",
        "list":[{
            "documentKey":"61d2a1b3-9068-4815-ba4d-3370e5a809d7",
            "documentTotal":21,
            "documentMinPosition":0,
            "documentMaxPosition":673,
            "list":[{
                "hit":{
                  "92":[["t",
                      "De"],
                    ["pos",
                      "LID"],
                    ["s",
                      null]],
                  "93":[["t",
                      "nieuwe"],
                    ["pos",
                      "ADJ"],
                    ["s",
                      null]],
                  "94":[["t",
                      "fusiegroep"],
                    ["pos",
                      "N"],
                    ["s",
                      null]]}},
              {
                "hit":{
                  "106":[["t",
                      "De"],
                    ["pos",
                      "LID"],
                    ["s",
                      null]],
                  "107":[["t",
                      "Belgische"],
                    ["pos",
                      "ADJ"],
                    ["s",
                      null]],
                  "108":[["t",
                      "regering"],
                    ["pos",
                      "N"],
                    ["s",
                      null]]}}]}]}]}
```

---

<a name="left-and-right"></a>  

### Left and Right

**Example**  
Keyword in context with output type *token* and prefixes *t* and *s* for sentences starting with an article, expanded two positions to the left and one position to the right

**CQL**  
`<s>[pos="LID"]`

**Request and response** 
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%3Cs%3E%5Bpos%3D%5C%22LID%5C%22%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.kwic=true&mtas.kwic.0.field=text&mtas.kwic.0.query.type=cql&mtas.kwic.0.query.value=%3Cs%3E%5Bpos%3D%22LID%22%5D&mtas.kwic.0.key=sentence+starting+with+article&mtas.kwic.0.prefix=t%2Cs&mtas.kwic.0.output=token&mtas.kwic.0.number=2&mtas.kwic.0.start=0&mtas.kwic.0.left=2&mtas.kwic.0.right=1&fl=%2A&start=0&rows=1&wt=json&indent=true`

```json
"mtas":{
    "kwic":[{
        "key":"sentence starting with article",
        "list":[{
            "documentKey":"61d2a1b3-9068-4815-ba4d-3370e5a809d7",
            "documentTotal":10,
            "documentMinPosition":0,
            "documentMaxPosition":673,
            "list":[{
                "startPosition":14,
                "endPosition":14,
                "tokens":[{
                    "mtasId":136,
                    "prefix":"t",
                    "value":"fusiegroep",
                    "positionStart":15,
                    "positionEnd":15,
                    "parentMtasId":295},
                  {
                    "mtasId":295,
                    "prefix":"s",
                    "value":"",
                    "positionStart":14,
                    "positionEnd":36,
                    "parentMtasId":417},
                  {
                    "mtasId":128,
                    "prefix":"t",
                    "value":"De",
                    "positionStart":14,
                    "positionEnd":14,
                    "parentMtasId":295},
                  {
                    "mtasId":113,
                    "prefix":"t",
                    "value":"afslanking",
                    "positionStart":13,
                    "positionEnd":13,
                    "parentMtasId":126},
                  {
                    "mtasId":107,
                    "prefix":"t",
                    "value":"tot",
                    "positionStart":12,
                    "positionEnd":12,
                    "parentMtasId":126},
                  {
                    "mtasId":126,
                    "prefix":"s",
                    "value":"",
                    "positionStart":9,
                    "positionEnd":13,
                    "parentMtasId":127}]},
              {
                "startPosition":92,
                "endPosition":92,
                "tokens":[{
                    "mtasId":729,
                    "prefix":"t",
                    "value":".",
                    "positionStart":91,
                    "positionEnd":91,
                    "parentMtasId":737},
                  {
                    "mtasId":746,
                    "prefix":"t",
                    "value":"nieuwe",
                    "positionStart":93,
                    "positionEnd":93,
                    "parentMtasId":853},
                  {
                    "mtasId":738,
                    "prefix":"t",
                    "value":"De",
                    "positionStart":92,
                    "positionEnd":92,
                    "parentMtasId":853},
                  {
                    "mtasId":853,
                    "prefix":"s",
                    "value":"",
                    "positionStart":92,
                    "positionEnd":105,
                    "parentMtasId":1114},
                  {
                    "mtasId":723,
                    "prefix":"t",
                    "value":"Parijs",
                    "positionStart":90,
                    "positionEnd":90,
                    "parentMtasId":737},
                  {
                    "mtasId":737,
                    "prefix":"s",
                    "value":"",
                    "positionStart":59,
                    "positionEnd":91,
                    "parentMtasId":1114}]}]}]}]}
```

---

##Lucene

To use keywords in context [directly in Lucene](installation_lucene.html), *ComponentKwic* together with the provided *collect* method can be used.
