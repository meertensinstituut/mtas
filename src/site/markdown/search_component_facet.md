# Facets

Mtas can produce facets on metadata for Mtas queries. To get this information, in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.facet            | true   | yes         |

Multiple facet results can be produced within the same request. To distinguish them, a unique identifier has to be provided for each of the required document results.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.facet.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.facet.\<identifier\>.field       | \<string\>   | Mtas field                      | yes         |

## Queries

One or multiple queries on the defined Mtas field have to be defined

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.facet.\<identifier\>.query.\<identifier query\>.type       | \<string\>   | query language: [cql](search_cql.html)  | yes         |
| mtas.facet.\<identifier\>.query.\<identifier query\>.value      | \<string\>   | query: [cql](search_cql.html)            | yes         |
| mtas.facet.\<identifier\>.query.\<identifier query\>.prefix     | \<string\>   | default prefix            | no         |
| mtas.facet.\<identifier\>.query.\<identifier query\>.ignore      | \<string\>   | ignore query: [cql](search_cql.html)            | no         |
| mtas.facet.\<identifier\>.query.\<identifier query\>.maximumIgnoreLength      | \<integer\>   | maximum number of succeeding occurrences to ignore            | no         |

### Variables

The query may contain one or more variables, and the value(s) of these variables have to be defined 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.facet.\<identifier\>.query.\<identifier query\>.variable.\<identifier variable\>.name      | \<string\>   | name of variable                 | yes        |
| mtas.facet.\<identifier\>.query.\<identifier query\>.variable.\<identifier variable\>.value      | \<string\>   | comma separated list of values  | yes        |

## Base

One or multiple fields to produce facets over have to be defined

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.facet.\<identifier\>.base.\<identifier base\>.field       | \<string\>   | field to produce facet over | yes         |
| mtas.facet.\<identifier\>.base.\<identifier base\>.type | \<string\>  | required [type of statistics](search_stats.html) | no         |
| mtas.facet.\<identifier\>.base.\<identifier base\>.sort.type       | \<string\>   | sort on term or [type of statistics](search_stats.html)     | no         |
| mtas.facet.\<identifier\>.base.\<identifier base\>.sort.direction       | \<string\>   | sort direction: asc or desc | no         |
| mtas.facet.\<identifier\>.base.\<identifier base\>.number       | \<double\>   | number of facets   | no         |
| mtas.facet.\<identifier\>.base.\<identifier base\>.minimum       | \<double\>   | minimum number of occurrences span(s)   | no         |
| mtas.facet.\<identifier\>.base.\<identifier base\>.maximum       | \<double\>   | maximum number of occurrences span(s)   | no         |

### Ranges

Number values can be grouped into ranges by defining a size and optionally a base for these ranges.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.facet.\<identifier\>.base.\<identifier base\>.range.size       | \<double\>   | size of the range | yes         |
| mtas.facet.\<identifier\>.base.\<identifier base\>.range.base       | \<double\>   | base for the ranges | no         |

### Functions

To compute statistics for values based on the occurrence of one or multiple spans, optionally [functions](search_functions.html) can be added. The parameters for these functions are the number of occurrences *$q0*, *$q1*, ... for each span and the number of positions *$n* in a document. Statistics on the value computed for each document in the set are added to the response.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.facet.\<identifier\>.base.\<identifier base\>.function.\<identifier function\>.key       | \<string\>   | key used in response                    | no         |
| mtas.facet.\<identifier\>.base.\<identifier base\>.function.\<identifier function\>.expression       | \<string\>   | see [functions](search_functions.html)       | yes        |
| mtas.facet.\<identifier\>.base.\<identifier base\>.function.\<identifier function\>.type      | \<string\>   | required [type of statistics](search_stats.html)                   | no         |

The key is added to the response and may be used to distinguish between multiple functions, and should therefore be unique within each specified facet base.

---

## Examples
1. [Basic](#basic) : basic facet on occurring part of speech
2. [Multiple](#multiple) : multiple facets on occurring part of speech
3. [Variable](#variable) : facets on occurring part of speech with variable
4. [Range](#range) : facet on occurring part of speech with range
5. [Function](#function) : facet on occurring part of speech with function

---

<a name="basic"></a>  

### Basic

**Example**  
Facet over year for CQL query `[pos="N"]`.

**Request and response**  
`q=*:*&mtas=true&mtas.facet=true&mtas.facet.0.field=test&mtas.facet.0.key=example+-+basic&mtas.facet.0.query.0.type=cql&mtas.facet.0.query.0.value=[pos%3D"N"]&mtas.facet.0.base.0.field=year&mtas.facet.0.base.0.sort.type=sum&mtas.facet.0.base.0.sort.direction=desc&mtas.facet.0.base.0.number=3&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "facet":[{
        "key":"example - basic",
        "listTotal":257,
        "list":[{
            "mean":380.58187772925766,
            "sum":697226,
            "n":1832,
            "key":"1997"},
          {
            "mean":389.84488636363636,
            "sum":686127,
            "n":1760,
            "key":"1999"},
          {
            "mean":415.17861482381534,
            "sum":683384,
            "n":1646,
            "key":"2002"}]}]}
```

<a name="multiple"></a>  

### Multiple

**Example**  
Facet over genre and year for CQL query `[pos="N"]`.

**Request and response**  
`q=*:*&mtas=true&mtas.facet=true&mtas.facet.0.field=test&mtas.facet.0.key=example+-+multiple&mtas.facet.0.query.0.type=cql&mtas.facet.0.query.0.value=[pos%3D"N"]&mtas.facet.0.base.0.field=genre&mtas.facet.0.base.0.sort.type=sum&mtas.facet.0.base.0.sort.direction=desc&mtas.facet.0.base.0.number=2&mtas.facet.0.base.1.field=year&mtas.facet.0.base.1.sort.type=sum&mtas.facet.0.base.1.sort.direction=desc&mtas.facet.0.base.1.number=2&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "facet":[{
        "key":"example - multiple",
        "listTotal":26,
        "list":[{
            "mean":409.7034217657067,
            "sum":65015836,
            "n":158690,
            "listTotal":257,
            "list":{
              "1997":{
                "mean":380.58187772925766,
                "sum":697226,
                "n":1832},
              "1999":{
                "mean":389.84488636363636,
                "sum":686127,
                "n":1760}},
            "key":"jaarboeken"},
          {
            "mean":409.7034217657067,
            "sum":65015836,
            "n":158690,
            "listTotal":257,
            "list":{
              "1997":{
                "mean":380.58187772925766,
                "sum":697226,
                "n":1832},
              "1999":{
                "mean":389.84488636363636,
                "sum":686127,
                "n":1760}},
            "key":"periodieken"}]}]}
```

<a name="variable"></a>  

### Variable

**Example**  
Facet over year for CQL query `[pos=$1]` with `$1` equal to `N,ADJ`.

**Request and response**  
`q=*:*&mtas=true&mtas.facet=true&mtas.facet.0.field=text&mtas.facet.0.key=example+-+variable&mtas.facet.0.query.0.type=cql&mtas.facet.0.query.0.value=[pos%3D$1]&mtas.facet.0.query.0.variable.0.name=1&mtas.facet.0.query.0.variable.0.value=N,ADJ&mtas.facet.0.base.0.field=year&mtas.facet.0.base.0.sort.type=sum&mtas.facet.0.base.0.sort.direction=desc&mtas.facet.0.base.0.number=3&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "facet":[{
        "key":"example - variable",
        "listTotal":257,
        "list":[{
            "mean":531.8187772925764,
            "sum":974292,
            "n":1832,
            "key":"1997"},
          {
            "mean":545.3232954545455,
            "sum":959769,
            "n":1760,
            "key":"1999"},
          {
            "mean":573.460510328068,
            "sum":943916,
            "n":1646,
            "key":"2002"}]}]}
```

<a name="range"></a>  

### Range

**Example**  
Facet over year with ranges of size 10 for CQL query `[pos="N"]`.

**Request and response**  
`q=*:*&mtas=true&mtas.facet=true&mtas.facet.0.field=test&mtas.facet.0.key=example+-+range&mtas.facet.0.query.0.type=cql&mtas.facet.0.query.0.value=[pos%3D"N"]&mtas.facet.0.base.0.field=year&mtas.facet.0.base.0.sort.type=sum&mtas.facet.0.base.0.sort.direction=desc&mtas.facet.0.base.0.number=3&mtas.facet.0.base.0.range.size=10&mtas.facet.0.base.0.range.base=0&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "facet":[{
        "key":"example - range",
        "listTotal":29,
        "list":[{
            "mean":369.9619179400794,
            "sum":6149507,
            "n":16622,
            "key":"1990-1999"},
          {
            "mean":559.2636835405855,
            "sum":5711760,
            "n":10213,
            "key":"1900-1909"},
          {
            "mean":482.52500238117915,
            "sum":5066030,
            "n":10499,
            "key":"1910-1919"}]}]}
```

<a name="function"></a>  

### Function

**Example**  
Facet over year for CQL query `[pos="N"]` with function.

**Request and response**  
`q=*:*&mtas=true&mtas.facet=true&mtas.facet.0.field=test&mtas.facet.0.key=example+-+basic&mtas.facet.0.query.0.type=cql&mtas.facet.0.query.0.value=[pos%3D"N"]&mtas.facet.0.base.0.field=year&mtas.facet.0.base.0.sort.type=sum&mtas.facet.0.base.0.sort.direction=desc&mtas.facet.0.base.0.number=2&mtas.facet.0.base.0.minimum=1&mtas.facet.0.base.0.function.0.key=relative&mtas.facet.0.base.0.function.0.expression=$q0/$n&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "facet":[{
        "key":"example - basic",
        "listTotal":255,
        "list":[{
            "mean":515.6997041420118,
            "sum":697226,
            "n":1352,
            "functions":{
              "relative":{
                "mean":0.17235837258586809,
                "sum":233.02851973609367,
                "n":1352}},
            "key":"1997"},
          {
            "mean":476.14642609299096,
            "sum":686127,
            "n":1441,
            "functions":{
              "relative":{
                "mean":0.17248794525621,
                "sum":248.55512911419862,
                "n":1441}},
            "key":"1999"}]}]}
```


**Lucene**

To produce facets on metadata [directly in Lucene](installation_lucene.html), *ComponentFacet* together with the provided *collect* method can be used.