# Statistics - spans

To get statistics on the occurrence of a span within a set of documents, besides the parameter to enable [statistics](search_query_stats.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.stats.spans      | true   | yes         |

Multiple statistics on the occurrence of a span can be produced within the same request. To distinguish them, a unique identifier has to be provided for  each of the required statistics. Furthermore, statistics for the occurrence of multiple spans can be produced. Spans are described by a query, and to distinguish multiple spans, also a query identifier has to be provided. 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.stats.spans.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.stats.spans.\<identifier\>.field       | \<string\>   | mtas field                      | yes         |
| mtas.stats.spans.\<identifier\>.query.\<identifier query\>.type       | \<string\>   | query language: [cql](search_cql.html)  | yes         |
| mtas.stats.spans.\<identifier\>.query.\<identifier query\>.value      | \<string\>   | query: [cql](search_cql.html)            | yes         |
| mtas.stats.spans.\<identifier\>.query.\<identifier query\>.prefix     | \<string\>   | default prefix            | no         |
| mtas.stats.spans.\<identifier\>.query.\<identifier query\>.ignore      | \<string\>   | ignore query: [cql](search_cql.html)            | no         |
| mtas.stats.spans.\<identifier\>.query.\<identifier query\>.maximumIgnoreLength      | \<integer\>   | maximum number of succeeding occurrences to ignore            | no         |
| mtas.stats.spans.\<identifier\>.type        | \<string\>   | required [type of statistics](search_stats.html) | no          |
| mtas.stats.spans.\<identifier\>.minimum     | \<double\>   | minimum number of occurrences span  | no          |
| mtas.stats.spans.\<identifier\>.maximum     | \<double\>   | maximum number of occurrences span  | no          |

The *key* is added to the response and may be used to distinguish between multiple statistics on the occurrence of spans, and should therefore be unique. The optional *minimum* and *maximum* can be used to focus only on documents satisfying a condition on the number of occurrences of the spans. When multiple queries are provided, the provided boundary will hold on the sum of occurrences of the resulting spans.

## Variables

The query may contain one or more variables, and the value(s) of these variables have to be defined 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.stats.spans.\<identifier\>.query.\<identifier query\>.variable\<identifier variable\>.name      | \<string\>   | name of variable                 | yes        |
| mtas.stats.spans.\<identifier\>.query.\<identifier query\>.variable\<identifier variable\>.value      | \<string\>   | comma separated list of values  | yes        |

## Functions

To compute statistics for values based on the occurrence of one or multiple spans, optionally [functions](search_functions.html) can be added. The parameters for these functions are the number of occurrences *$q0*, *$q1*, ... for each span and the number of positions *$n* in a document. Statistics on the value computed for each document in the set are added to the response.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.stats.spans.\<identifier\>.function.\<identifier function\>.key       | \<string\>   | key used in response                    | no         |
| mtas.stats.spans.\<identifier\>.function.\<identifier function\>.expression       | \<string\>   | see [functions](search_functions.html)       | yes        |
| mtas.stats.spans.\<identifier\>.function.\<identifier function\>.type      | \<string\>   | required [type of statistics](search_stats.html)                   | no         |

Again, the *key* is added to the response and may be used to distinguish between multiple functions, and should therefore be unique.

---

## Examples
1. [Basic](#basic) : basic statistics on the occurrence of a word.
2. [Minimum and Maximum](#minimum-and-maximum) : statistics on the occurrence of a word with restrictions on the number of occurrences.
3. [Subset](#subset) : statistics on the occurrence of a word within a subset of documents.
4. [Multiple](#multiple) : statistics on the occurrence of multiple words.
5. [Prefix](#prefix) : default prefix for query
5. [Ignore](#ignore) : query with ignore
6. [Ignore and maximumIgnoreLength](#ignore-and-maximum-ignore-length) : query with ignore and maximumIgnoreLength
6. [Functions](#functions) : statistics using functions.
7. [Multiple and Functions](#multiple-and-functions) : statistics using functions on the occurrence of multiple words.

---

<a name="basic"></a>  

### Basic

**Example**  
Total and average number of occurrences of the word "de" and the number of documents.

**CQL**  
`[t="de"]`

**Request and response**  
`q=*%3A*&mtas=true&mtas.stats=true&mtas.stats.spans=true&mtas.stats.spans.0.field=text&mtas.stats.spans.0.query.0.type=cql&mtas.stats.spans.0.query.0.value=%5Bt%3D%22de%22%5D&mtas.stats.spans.0.key=example - basic&mtas.stats.spans.0.type=n%2Csum%2Cmean&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "spans":[{
          "key":"example - basic",
          "mean":10.484745312881392,
          "sum":21648986,
          "n":2064808}]}}
```

<a name="minimum-and-maximum"></a>

### Minimum and Maximum

**Example**  
Full statistics on the number of occurrences of the word "de" for documents with a minimum of 100 occurrences, for documents with a maximum of 200 occurrences, and for documents with between 100 and 200 occurrences.

**CQL**  
`[t="de"]`

**Request and response**  
`q=*%3A*&mtas=true&mtas.stats=true&mtas.stats.spans=true&mtas.stats.spans.0.field=text&mtas.stats.spans.0.query.0.type=cql&mtas.stats.spans.0.query.0.value=[t%3D"de"]&mtas.stats.spans.0.key=example - minimum&mtas.stats.spans.0.type=all&mtas.stats.spans.0.minimum=100&mtas.stats.spans.1.field=text&mtas.stats.spans.1.query.0.type=cql&mtas.stats.spans.1.query.0.value=[t%3D"de"]&mtas.stats.spans.1.key=example - maximum&mtas.stats.spans.1.type=all&mtas.stats.spans.1.maximum=200&mtas.stats.spans.2.field=text&mtas.stats.spans.2.query.0.type=cql&mtas.stats.spans.2.query.0.value=[t%3D"de"]&mtas.stats.spans.2.key=example - minimum and maximum&mtas.stats.spans.2.type=all&mtas.stats.spans.2.minimum=100&mtas.stats.spans.2.maximum=200&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "spans":[{
          "key":"example - minimum",
          "sumsq":8.697716821E9,
          "populationvariance":419246.79216222034,
          "max":18192.0,
          "sum":4531791.0,
          "kurtosis":164.0070630212961,
          "standarddeviation":647.5106543337658,
          "n":18029,
          "quadraticmean":694.5712655602209,
          "min":100.0,
          "median":136.0,
          "variance":419270.04747574165,
          "mean":251.36119585112766,
          "geometricmean":160.51059457862394,
          "sumoflogs":91557.75154264584,
          "skewness":10.551742594336096},
        {
          "key":"example - maximum",
          "sumsq":7.37120919E8,
          "populationvariance":271.7584436494812,
          "max":200.0,
          "sum":1.9094863E7,
          "kurtosis":31.747278519903332,
          "standarddeviation":16.485101621383148,
          "n":2061622,
          "quadraticmean":18.90883830898543,
          "min":0.0,
          "median":4.0,
          "variance":271.75857546732925,
          "mean":9.26205822405864,
          "geometricmean":0.0,
          "sumoflogs":"-Infinity",
          "skewness":4.742017950067824},
        {
          "key":"example - minimum and maximum",
          "sumsq":2.73659206E8,
          "populationvariance":684.261700791398,
          "max":200.0,
          "sum":1977668.0,
          "kurtosis":-0.4734932618605048,
          "standarddeviation":26.15927758668186,
          "n":14843,
          "quadraticmean":135.78262099542508,
          "min":100.0,
          "median":127.0,
          "variance":684.3078038570759,
          "mean":133.23910260728962,
          "geometricmean":130.83058078303992,
          "sumoflogs":72343.34534205312,
          "skewness":0.7178184624731176}]}}
```

<a name="subset"></a>  

### Subset

**Example**  
Total and average number of occurrences of the word "de" and the number of documents for a subset of documents.

**CQL**  
`[t="de"]`

**Request and response**  
`q=text:koe&rows=0&mtas=true&mtas.stats=true&mtas.stats.tokens=true&mtas.stats.tokens.0.field=text&mtas.stats.tokens.0.key=example - subset&mtas.stats.tokens.0.type=sum,mean,n&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "tokens":[{
          "key":"example - subset",
          "mean":49644.49556868538,
          "sum":134437294,
          "n":2708}]}}
```

<a name="multiple"></a>  

### Multiple

**Example**  
Total and average number of occurrences of the word "de" and "het", and the number of documents.

**CQL**  
1. combined cql: `[t="de"|t="het"]`  
2. combined regexp: `[t="(de|het)"]`  
3. two queries: `[t="de"]` `[t="het"]`

**Request and response**  
`q=*%3A*&mtas=true&mtas.stats=true&mtas.stats.spans=true&mtas.stats.spans.0.field=text&mtas.stats.spans.0.query.0.type=cql&mtas.stats.spans.0.query.0.value=[t%3D"de"|t%3D"het"]&mtas.stats.spans.0.key=multiple+-+combined+cql&mtas.stats.spans.0.type=n%2Csum%2Cmean&mtas.stats.spans.1.field=text&mtas.stats.spans.1.query.0.type=cql&mtas.stats.spans.1.query.0.value=[t%3D"(de|het)"]&mtas.stats.spans.1.key=multiple+-+combined+regexp&mtas.stats.spans.1.type=n%2Csum%2Cmean&mtas.stats.spans.2.field=text&mtas.stats.spans.2.query.0.type=cql&mtas.stats.spans.2.query.0.value=[t%3D"de"]&mtas.stats.spans.2.query.1.type=cql&mtas.stats.spans.2.query.1.value=[t%3D"het"]&mtas.stats.spans.2.key=multiple+-+two+queries&mtas.stats.spans.2.type=n%2Csum%2Cmean&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "spans":[{
          "key":"multiple - combined cql",
          "mean":15.173083405333571,
          "sum":31329504,
          "n":2064808},
        {
          "key":"multiple - combined regexp",
          "mean":15.173083405333571,
          "sum":31329504,
          "n":2064808},
        {
          "key":"multiple - two queries",
          "mean":15.173083405333571,
          "sum":31329504,
          "n":2064808}]}}}
```

<a name="prefix"></a>  

### Prefix

**Example**  
Total and average number of occurrences of the word "de" followed by an adjective.

**CQL**  
`"de" [pos="ADJ"]`

**Request and response**  
`q=*%3A*&mtas=true&mtas.stats=true&mtas.stats.spans=true&mtas.stats.spans.0.field=text&mtas.stats.spans.0.query.0.type=cql&mtas.stats.spans.0.query.0.value="de" [pos%3D"ADJ"]&mtas.stats.spans.0.query.0.prefix=t_lc&mtas.stats.spans.0.key=example - prefix&mtas.stats.spans.0.type=n%2Csum%2Cmean&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "spans":[{
          "key":"example - prefix",
          "mean":2.1725308115815127,
          "sum":4485859,
          "n":2064808}]}}
```

<a name="ignore"></a>  

### Ignore

**Example**  
Total and average number of occurrences of an article followed by a noun, ignoring adjectives.

**CQL**  
`[pos="LID"][pos="N"]`

**Ignore** 
`[pos="ADJ"]`


**Request and response**  
`q=*%3A*&mtas=true&mtas.stats=true&mtas.stats.spans=true&mtas.stats.spans.0.field=text&mtas.stats.spans.0.query.0.type=cql&mtas.stats.spans.0.query.0.value=[t_lc%3D"de"]&mtas.stats.spans.0.key=functions+-+de&mtas.stats.spans.0.type=n%2Csum%2Cmean&mtas.stats.spans.0.function.0.expression=%24q0%2F%24n&mtas.stats.spans.0.function.0.key=relative+frequency&mtas.stats.spans.0.function.0.type=mean%2Cstandarddeviation%2Cdistribution(start%3D0%2Cend%3D0.1%2Cnumber%3D10)&mtas.stats.spans.0.function.1.expression=%24n&mtas.stats.spans.0.function.1.key=number+of+words&mtas.stats.spans.0.function.1.type=n%2Csum&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "spans":[{
          "key":"functions - de",
          "mean":12.34790062804871,
          "sum":25496044,
          "n":2064808,
          "functions":{
            "number of words":{
              "sum":337230767,
              "n":2064808},
            "relative frequency":{
              "distribution(start=0,end=0.1,number=10)":{
                "[0.000,0.010)":950500,
                "[0.010,0.020)":80369,
                "[0.020,0.030)":115695,
                "[0.030,0.040)":139752,
                "[0.040,0.050)":162877,
                "[0.050,0.060)":168598,
                "[0.060,0.070)":145493,
                "[0.070,0.080)":109117,
                "[0.080,0.090)":77214,
                "[0.090,0.100)":51243},
              "mean":0.030196372045937097,
              "errorList":{"division by zero":691633},
              "standarddeviation":0.03428066513492476,
              "errorNumber":691633}}}]}}
```

<a name="ignore-and-maximum-ignore-length"></a>  

### Ignore and maximumIgnoreLength

<a name="functions"></a>  

### Functions

**Example**  
Statistics for the relative frequency of the word "de" and the total number of words in documents containing this word.

**CQL**  
`[t="de"]`

**Functions**  
`$q0/$n`  
`$n`

**Request and response**  
`q=*%3A*&mtas=true&mtas.stats=true&mtas.stats.spans=true&mtas.stats.spans.0.field=text&mtas.stats.spans.0.query.0.type=cql&mtas.stats.spans.0.query.0.value=[t_lc%3D"de"]&mtas.stats.spans.0.key=functions+-+de&mtas.stats.spans.0.type=n%2Csum%2Cmean&mtas.stats.spans.0.function.0.expression=%24q0%2F%24n&mtas.stats.spans.0.function.0.key=relative+frequency&mtas.stats.spans.0.function.0.type=mean%2Cstandarddeviation%2Cdistribution(start%3D0%2Cend%3D0.1%2Cnumber%3D10)&mtas.stats.spans.0.function.1.expression=%24n&mtas.stats.spans.0.function.1.key=number+of+words&mtas.stats.spans.0.function.1.type=n%2Csum&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "spans":[{
          "key":"functions - de",
          "mean":12.34790062804871,
          "sum":25496044,
          "n":2064808,
          "functions":{
            "number of words":{
              "sum":337230767,
              "n":2064808},
            "relative frequency":{
              "distribution(start=0,end=0.1,number=10)":{
                "[0.000,0.010)":950500,
                "[0.010,0.020)":80369,
                "[0.020,0.030)":115695,
                "[0.030,0.040)":139752,
                "[0.040,0.050)":162877,
                "[0.050,0.060)":168598,
                "[0.060,0.070)":145493,
                "[0.070,0.080)":109117,
                "[0.080,0.090)":77214,
                "[0.090,0.100)":51243},
              "mean":0.030196372045937097,
              "errorList":{"division by zero":691633},
              "standarddeviation":0.03428066513492476,
              "errorNumber":691633}}}]}}
```

<a name="multiple-and-functions"></a>  

### Multiple and Functions

**Example**  
Statistics for the absolute and relative frequency of the words "de", "het" and "een", for *part of speech* type "LID" and the total number of words in documents containing this word.

**CQL**  
`[t="de"]`  
`[t="het"]`  
`[t="een"]`  
`[pos="LID"]`

**Functions**  
`$q0/$n`  
`$q1/$n`  
`$q2/$n`    
`$q3/$n`  
`$q0/$q3`  
`$q1/$q3`  
`$q2/$q3`  
`($q0+$q1+$q2)/$q3`  

**Request and response**  
`q=*%3A*&mtas=true&mtas.stats=true&mtas.stats.spans=true&mtas.stats.spans.0.field=text&mtas.stats.spans.0.query.0.type=cql&mtas.stats.spans.0.query.0.value=[t_lc%3D"de"]&mtas.stats.spans.0.query.1.type=cql&mtas.stats.spans.0.query.1.value=[t_lc%3D"het"]&mtas.stats.spans.0.query.2.type=cql&mtas.stats.spans.0.query.2.value=[t_lc%3D"een"]&mtas.stats.spans.0.query.3.type=cql&mtas.stats.spans.0.query.3.value=[pos%3D"LID"]&mtas.stats.spans.0.key=multiple+and+functions+-+de%2Bhet%2Been+and+LID&mtas.stats.spans.0.type=n&mtas.stats.spans.0.minimum=1&mtas.stats.spans.0.function.0.expression=%24q0&mtas.stats.spans.0.function.0.key=de+-+absolute&mtas.stats.spans.0.function.0.type=n%2Csum&mtas.stats.spans.0.function.1.expression=%24q1&mtas.stats.spans.0.function.1.key=het+-+absolute&mtas.stats.spans.0.function.1.type=n%2Csum&mtas.stats.spans.0.function.2.expression=%24q2&mtas.stats.spans.0.function.2.key=een+-+absolute&mtas.stats.spans.0.function.2.type=n%2Csum&mtas.stats.spans.0.function.3.expression=%24q3&mtas.stats.spans.0.function.3.key=LID+-+absolute&mtas.stats.spans.0.function.3.type=n%2Csum&mtas.stats.spans.0.function.4.expression=%24q0%2F%24n&mtas.stats.spans.0.function.4.key=de+-+relative+to+positions&mtas.stats.spans.0.function.4.type=n%2Cmean&mtas.stats.spans.0.function.5.expression=%24q1%2F%24n&mtas.stats.spans.0.function.5.key=het+-+relative+to+positions&mtas.stats.spans.0.function.5.type=n%2Cmean&mtas.stats.spans.0.function.6.expression=%24q2%2F%24n&mtas.stats.spans.0.function.6.key=een+-+relative+to+positions&mtas.stats.spans.0.function.6.type=n%2Cmean&mtas.stats.spans.0.function.7.expression=%24q3%2F%24n&mtas.stats.spans.0.function.7.key=LID+-+relative+to+positions&mtas.stats.spans.0.function.7.type=n%2Cmean&mtas.stats.spans.0.function.8.expression=%24q0%2F%24q3&mtas.stats.spans.0.function.8.key=de+-+relative+to+LID&mtas.stats.spans.0.function.8.type=n%2Cmean&mtas.stats.spans.0.function.9.expression=%24q1%2F%24q3&mtas.stats.spans.0.function.9.key=het+-+relative+to+LID&mtas.stats.spans.0.function.9.type=n%2Cmean&mtas.stats.spans.0.function.10.expression=%24q2%2F%24q3&mtas.stats.spans.0.function.10.key=een+-+relative+to+LID&mtas.stats.spans.0.function.10.type=n%2Cmean&mtas.stats.spans.0.function.11.expression=(%24q0%2B%24q1%2B%24q2)%2F%24q3&mtas.stats.spans.0.function.11.key=de%2Bhet%2Been+-+relative+to+LID&mtas.stats.spans.0.function.11.type=n%2Cmean&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "spans":[{
          "key":"multiple and functions - de+het+een and LID",
          "n":1889646,
          "functions":{
            "een - relative to LID":{
              "mean":0.26176457027078637,
              "errorList":{"division by zero":24165},
              "n":1889646,
              "errorNumber":24165},
            "LID - absolute":{
              "sum":44062088,
              "n":1889646},
            "de+het+een - relative to LID":{
              "mean":1.0864042218694616,
              "errorList":{"division by zero":24165},
              "n":1889646,
              "errorNumber":24165},
            "het - relative to LID":{
              "mean":0.27408299432800154,
              "errorList":{"division by zero":24165},
              "n":1889646,
              "errorNumber":24165},
            "een - relative to positions":{
              "mean":0.014397108127121947,
              "errorList":{"division by zero":631875},
              "n":1889646,
              "errorNumber":631875},
            "een - absolute":{
              "sum":10616743,
              "n":1889646},
            "het - relative to positions":{
              "mean":0.014874612933292992,
              "errorList":{"division by zero":631875},
              "n":1889646,
              "errorNumber":631875},
            "de - absolute":{
              "sum":25496044,
              "n":1889646},
            "het - absolute":{
              "sum":11527080,
              "n":1889646},
            "LID - relative to positions":{
              "mean":0.05786145893684233,
              "errorList":{"division by zero":631875},
              "n":1889646,
              "errorNumber":631875},
            "de - relative to LID":{
              "mean":0.5505566572707496,
              "errorList":{"division by zero":24165},
              "n":1889646,
              "errorNumber":24165},
            "de - relative to positions":{
              "mean":0.03299544495178249,
              "errorList":{"division by zero":631875},
              "n":1889646,
              "errorNumber":631875}}}]}}
```
