# Grouping

Mtas can group results for Mtas queries within the (filtered) set of documents. To get this information, in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.group            | true   | yes         |

Multiple group results can be produced within the same request. To distinguish them, a unique identifier has to be provided for each of the required document results.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.group.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.group.\<identifier\>.field       | \<string\>   | Mtas field                      | yes         |
| mtas.group.\<identifier\>.number      | \<integer\>   | number of results                      | no         |
| mtas.group.\<identifier\>.start      | \<integer\>   | offset list of results                      | no         |

## Query

A query on the defined Mtas field has to be defined

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.group.\<identifier\>.query.type       | \<string\>   | query language: [cql](search_cql.html)  | yes         |
| mtas.group.\<identifier\>.query.value      | \<string\>   | query: [cql](search_cql.html)            | yes         |
| mtas.group.\<identifier\>.query.prefix     | \<string\>   | default prefix            | no         |
| mtas.group.\<identifier\>.query.ignore      | \<string\>   | ignore query: [cql](search_cql.html)            | no         |
| mtas.group.\<identifier\>.query.maximumIgnoreLength      | \<integer\>   | maximum number of succeeding occurrences to ignore            | no         |

### Variables

The query may contain one or more variables, and the value(s) of these variables have to be defined 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.group.\<identifier\>.query.variable.\<identifier variable\>.name      | \<string\>   | name of variable                 | yes        |
| mtas.group.\<identifier\>.query.variable.\<identifier variable\>.value      | \<string\>   | comma separated list of values  | yes        |

### Group

Finally, the exact grouping has to be specified. Specification of the prefixes can be made for
 
* positions inside the hit, covering all positions
* specified positions inside the hit defined from the left or right
* specified positions inside the hit defined from the left or right, which may exceed the hit boundaries to respectively right or left
* positions left or right from the hit

---

**Grouping inside hit**

The most simple form is grouping over a list of specified prefixes occurring at the  position(s) inside the hit.
 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.group.\<identifier\>.grouping.hit.inside.prefixes   | \<string\>   | comma seperated list of prefixes  | yes         |

---

**Grouping left inside hit**

To group over specified prefixes occurring at positions specified from the left side inside the hit, *insideLeft* can be used.
 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.group.\<identifier\>.grouping.hit.insideLeft.\<identifier insideLeft\>.prefixes   | \<string\>   | comma seperated list of prefixes  | yes         |
| mtas.group.\<identifier\>.grouping.hit.insideLeft.\<identifier insideLeft\>.position   | \<integer\>(-\<integer\>)   | position(s)  | yes         |

---

**Grouping right inside hit**

To group over specified prefixes occurring at positions specified from the right side inside the hit, *insideRight* can be used.
 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.group.\<identifier\>.grouping.hit.insideRight.\<identifier insideRight\>.prefixes   | \<string\>   | comma seperated list of prefixes  | yes         |
| mtas.group.\<identifier\>.grouping.hit.insideRight.\<identifier insideRight\>.position   | \<integer\>(-\<integer\>)   | position(s)  | yes         |

---

**Grouping left hit**

To group over specified prefixes occurring at positions specified from the left side of the hit, optionally exceeding the right hit boundary, *left* is available.
 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.group.\<identifier\>.grouping.hit.left.\<identifier left\>.prefixes   | \<string\>   | comma seperated list of prefixes  | yes         |
| mtas.group.\<identifier\>.grouping.hit.left.\<identifier left\>.position   | \<integer\>(-\<integer\>)   | position(s)  | yes         |

---

**Grouping right hit**

To group over specified prefixes occurring at positions specified from the right side of the hit, optionally exceeding the left hit boundary, *right* is available.
 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.group.\<identifier\>.grouping.hit.right.\<identifier right\>.prefixes   | \<string\>   | comma seperated list of prefixes  | yes         |
| mtas.group.\<identifier\>.grouping.hit.right.\<identifier right\>.position   | \<integer\>(-\<integer\>)   | position(s)  | yes         |

---

## Examples
1. [Inside hit](#inside_hit) : grouping based on prefixes inside the hit.
2. [Left inside hit](#inside_left_hit) : grouping based on prefixes occurring at positions specified from the left, inside the hit.
3. [Right inside hit](#inside_right_hit) : grouping based on prefixes occurring at positions specified from the right, inside the hit.
4. [Left hit](#left_hit) : grouping based on prefixes occurring at positions specified from the left, not necessarily inside the hit.
5. [Right hit](#right_hit) : grouping based on prefixes occurring at positions specified from the right, not necessarily inside the hit.
6. [Left](#left) : grouping based on prefixes occurring at positions at the left side from the hit.
7. [Right](#right) : grouping based on prefixes occurring at positions at the right side from the hit.

---

<a name="inside hit"></a>  

### Inside hit

**Example**  
Grouping over prefix `lemma` for CQL query `[pos="LID"]`.

**Request and response**  
`q=*:*&rows=0&mtas=true&mtas.group=true&mtas.group.0.field=text&mtas.group.0.query.type=cql&mtas.group.0.query.value=[pos="LID"]&mtas.group.0.grouping.hit.inside.prefixes=lemma&mtas.group.0.number=5&wt=json&indent=true`

``` json
"mtas":{
    "group":[{
        "key":"0",
        "listTotal":523,
        "list":[{
            "mean":156.32153403822628,
            "sum":20062462,
            "n":128341,
            "group":{"hit":{"0":[{
                    "prefix":"lemma",
                    "value":"de"}]}},
            "key":"| [lemma=\"de\"] |"},
          {
            "mean":55.123732635459874,
            "sum":6698195,
            "n":121512,
            "group":{"hit":{"0":[{
                    "prefix":"lemma",
                    "value":"het"}]}},
            "key":"| [lemma=\"het\"] |"},
          {
            "mean":46.594516509433966,
            "sum":5531701,
            "n":118720,
            "group":{"hit":{"0":[{
                    "prefix":"lemma",
                    "value":"een"}]}},
            "key":"| [lemma=\"een\"] |"}]}]}
```

<a name="left inside hit"></a>  

### Left inside hit

**Example**  
Grouping over prefix `lemma` at position `0` from the left and prefix `pos` at position `1-3` from the left inside the hit for CQL query `[pos="LID"][pos="ADJ"]`.

**Request and response**  
`q=*:*&rows=0&mtas=true&mtas.group=true&mtas.group.0.field=text&mtas.group.0.query.type=cql&mtas.group.0.query.value=[pos="LID"][pos="ADJ"]&mtas.group.0.grouping.hit.insideLeft.0.prefixes=lemma&mtas.group.0.grouping.hit.insideLeft.0.position=0&mtas.group.0.grouping.hit.insideLeft.1.prefixes=pos&mtas.group.0.grouping.hit.insideLeft.1.position=1-3&mtas.group.0.number=3&wt=json&indent=true`

``` json
"mtas":{
    "group":[{
        "key":"0",
        "listTotal":72,
        "list":[{
            "mean":31.155598846589545,
            "sum":3630375,
            "n":116524,
            "group":{"hit":{
                "0":[{
                    "prefix":"lemma",
                    "value":"de"}],
                "1":[{
                    "prefix":"pos",
                    "value":"ADJ"}]}},
            "key":"| [lemma=\"de\"] [pos=\"ADJ\"] |"},
          {
            "mean":17.898333524005643,
            "sum":1877392,
            "n":104892,
            "group":{"hit":{
                "0":[{
                    "prefix":"lemma",
                    "value":"een"}],
                "1":[{
                    "prefix":"pos",
                    "value":"ADJ"}]}},
            "key":"| [lemma=\"een\"] [pos=\"ADJ\"] |"},
          {
            "mean":13.61732368967055,
            "sum":1404518,
            "n":103142,
            "group":{"hit":{
                "0":[{
                    "prefix":"lemma",
                    "value":"het"}],
                "1":[{
                    "prefix":"pos",
                    "value":"ADJ"}]}},
            "key":"| [lemma=\"het\"] [pos=\"ADJ\"] |"}]}]}
```

<a name="right inside hit"></a>  

### Right inside hit

**Example**  
Grouping over prefix `lemma` at position `0` from the right and prefix `pos` at position `1-3` from the right inside the hit for CQL query `[pos="LID"][pos="ADJ"]`.

**Request and response**  
`q=*:*&rows=0&mtas=true&mtas.group=true&mtas.group.0.field=text&mtas.group.0.query.type=cql&mtas.group.0.query.value=[pos="LID"][pos="ADJ"]&mtas.group.0.grouping.hit.insideRight.0.prefixes=lemma&mtas.group.0.grouping.hit.insideRight.0.position=1&mtas.group.0.grouping.hit.insideRight.1.prefixes=pos&mtas.group.0.grouping.hit.insideRight.1.position=1-3&mtas.group.0.number=3&wt=json&indent=true`

``` json
"mtas":{
    "group":[{
        "key":"0",
        "listTotal":72,
        "list":[{
            "mean":31.155598846589545,
            "sum":3630375,
            "n":116524,
            "group":{"hit":{
                "0":[{
                    "prefix":"lemma",
                    "value":"de"},
                  {
                    "prefix":"pos",
                    "value":"LID"}],
                "1":null}},
            "key":"| [lemma=\"de\" & pos=\"LID\"] [] |"},
          {
            "mean":17.898333524005643,
            "sum":1877392,
            "n":104892,
            "group":{"hit":{
                "0":[{
                    "prefix":"lemma",
                    "value":"een"},
                  {
                    "prefix":"pos",
                    "value":"LID"}],
                "1":null}},
            "key":"| [lemma=\"een\" & pos=\"LID\"] [] |"},
          {
            "mean":13.61732368967055,
            "sum":1404518,
            "n":103142,
            "group":{"hit":{
                "0":[{
                    "prefix":"lemma",
                    "value":"het"},
                  {
                    "prefix":"pos",
                    "value":"LID"}],
                "1":null}},
            "key":"| [lemma=\"het\" & pos=\"LID\"] [] |"}]}]}
```

<a name="left hit"></a>  

### Left hit

**Example**  
Grouping over prefixes `lemma` and `pos` on position `3` from the left  for CQL query `[pos="ADJ"]{2} followedby [][pos="LID"]`.

**Request and response**  
`q=*:*&rows=0&mtas=true&mtas.group=true&mtas.group.0.field=NLContent_mtas&mtas.group.0.query.type=cql&mtas.group.0.query.value=[pos="ADJ"]{2} followedby [][pos="LID"]&mtas.group.0.grouping.hit.left.0.prefixes=pos,lemma&mtas.group.0.grouping.hit.left.0.position=3&mtas.group.0.number=3&wt=json&indent=true`

``` json
"mtas":{
    "group":[{
        "key":"0",
        "listTotal":12,
        "list":[{
            "mean":1.791719691185204,
            "sum":63357,
            "n":35361,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "right":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"de"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"| [] [] | [] [lemma=\"de\" & pos=\"LID\"]"},
          {
            "mean":1.248066748066748,
            "sum":18399,
            "n":14742,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "right":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"het"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"| [] [] | [] [lemma=\"het\" & pos=\"LID\"]"},
          {
            "mean":1.2065838092038965,
            "sum":14368,
            "n":11908,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "right":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"een"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"| [] [] | [] [lemma=\"een\" & pos=\"LID\"]"}]}]}
```

<a name="right hit"></a>  

### Right hit

**Example**  
Grouping over prefix `pos` and `lemma` on position `3` from the right for CQL query `[pos="ADJ"]{2} precededby [pos="LID"][]`.

**Request and response**  
`q=*:*&rows=0&mtas=true&mtas.group=true&mtas.group.0.field=text&mtas.group.0.query.type=cql&mtas.group.0.query.value=[pos="ADJ"]{2} precededby [pos="LID"][]&mtas.group.0.grouping.hit.right.0.prefixes=pos,lemma&mtas.group.0.grouping.hit.right.0.position=3&mtas.group.0.number=3&wt=json&indent=true`

``` json
"mtas":{
    "group":[{
        "key":"0",
        "listTotal":20,
        "list":[{
            "mean":1.632708503124151,
            "sum":48080,
            "n":29448,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "left":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"de"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"[] [lemma=\"de\" & pos=\"LID\"] | [] [] |"},
          {
            "mean":1.4123518709740865,
            "sum":28723,
            "n":20337,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "left":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"een"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"[] [lemma=\"een\" & pos=\"LID\"] | [] [] |"},
          {
            "mean":1.255492025278363,
            "sum":16688,
            "n":13292,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "left":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"het"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"[] [lemma=\"het\" & pos=\"LID\"] | [] [] |"}]}]}
```

---

<a name="left"></a>  

### Left

**Example**  
Grouping over prefixes `lemma` and `pos` on position `1` at the left side for CQL query `[pos="ADJ"]{2} precededby [pos="LID"][]`.

**Request and response**  
`q=*:*&rows=0&mtas=true&mtas.group=true&mtas.group.0.field=NLContent_mtas&mtas.group.0.query.type=cql&mtas.group.0.query.value=[pos="ADJ"]{2} precededby [pos="LID"][]&mtas.group.0.grouping.left.0.prefixes=pos,lemma&mtas.group.0.grouping.left.0.position=1&mtas.group.0.number=3&wt=json&indent=true`

``` json
"mtas":{
    "group":[{
        "key":"0",
        "listTotal":20,
        "list":[{
            "mean":1.632708503124151,
            "sum":48080,
            "n":29448,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "left":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"de"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"[] [lemma=\"de\" & pos=\"LID\"] | [] [] |"},
          {
            "mean":1.4123518709740865,
            "sum":28723,
            "n":20337,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "left":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"een"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"[] [lemma=\"een\" & pos=\"LID\"] | [] [] |"},
          {
            "mean":1.255492025278363,
            "sum":16688,
            "n":13292,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "left":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"het"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"[] [lemma=\"het\" & pos=\"LID\"] | [] [] |"}]}]}
```

---

<a name="right"></a>  

### Right

**Example**  
Grouping over prefixes `lemma` and `pos` on position `1` at the right side for CQL query `[pos="ADJ"]{2} followedby [][pos="LID"]`.

**Request and response**  
`q=*:*&rows=0&mtas=true&mtas.group=true&mtas.group.0.field=NLContent_mtas&mtas.group.0.query.type=cql&mtas.group.0.query.value=[pos="ADJ"]{2} followedby [][pos="LID"]&mtas.group.0.grouping.right.0.prefixes=pos,lemma&mtas.group.0.grouping.right.0.position=1&mtas.group.0.number=3&wt=json&indent=true`

``` json
"mtas":{
    "group":[{
        "key":"0",
        "listTotal":12,
        "list":[{
            "mean":1.791719691185204,
            "sum":63357,
            "n":35361,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "right":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"de"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"| [] [] | [] [lemma=\"de\" & pos=\"LID\"]"},
          {
            "mean":1.248066748066748,
            "sum":18399,
            "n":14742,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "right":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"het"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"| [] [] | [] [lemma=\"het\" & pos=\"LID\"]"},
          {
            "mean":1.2065838092038965,
            "sum":14368,
            "n":11908,
            "group":{
              "hit":{
                "0":null,
                "1":null},
              "right":{
                "0":null,
                "1":[{
                    "prefix":"lemma",
                    "value":"een"},
                  {
                    "prefix":"pos",
                    "value":"LID"}]}},
            "key":"| [] [] | [] [lemma=\"een\" & pos=\"LID\"]"}]}]}
```


**Lucene**

To group results [directly in Lucene](installation_lucene.html), *ComponentGroup* together with the provided *collect* method can be used.
