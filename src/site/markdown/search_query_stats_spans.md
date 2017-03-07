#Statistics - spans

To get statistics on the occurrence of a span within a set of documents in Solr requests, besides the parameter to enable [statistics](search_query_stats.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.stats.spans      | true   | yes         |

Multiple statistics on the occurrence of a span can be produced within the same request. To distinguish them, a unique identifier has to be provided for  each of the required statistics. Furthermore, statistics for the occurrence of multiple spans can be produced. Spans are described by a query, and to distinguish multiple spans, also a query identifier has to be provided. 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.stats.spans.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.stats.spans.\<identifier\>.field       | \<string\>   | Mtas field                      | yes         |
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
6. [Ignore and maximumIgnoreLength](#ignore-and-maximumignorelength) : query with ignore and maximumIgnoreLength
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
          "mean":10.488239100197209,
          "sum":21656200,
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
          "sumsq":8.697655383E9,
          "populationvariance":419224.862744871,
          "max":18192.0,
          "sum":4531747.0,
          "kurtosis":164.01633761739456,
          "standarddeviation":647.4937185426337,
          "n":18030,
          "quadraticmean":694.5495506941058,
          "min":100.0,
          "median":136.0,
          "variance":419248.1155521673,
          "mean":251.3448141985584,
          "geometricmean":160.50112302303313,
          "sumoflogs":91561.76594051626,
          "skewness":10.552060273112971},
        {
          "key":"example - maximum",
          "sumsq":7.37391079E8,
          "populationvariance":271.8217238864797,
          "max":200.0,
          "sum":1.9102393E7,
          "kurtosis":31.734626574581217,
          "standarddeviation":16.487020826545898,
          "n":2061623,
          "quadraticmean":18.91229851589547,
          "min":0.0,
          "median":4.0,
          "variance":271.82185573495815,
          "mean":9.265706193615522,
          "geometricmean":0.0,
          "sumoflogs":"-Infinity",
          "skewness":4.741031505227169},
        {
          "key":"example - minimum and maximum",
          "sumsq":2.73698488E8,
          "populationvariance":684.3248008017308,
          "max":200.0,
          "sum":1977940.0,
          "kurtosis":-0.47377181206297303,
          "standarddeviation":26.16048359466255,
          "n":14845,
          "quadraticmean":135.78321834689768,
          "min":100.0,
          "median":127.0,
          "variance":684.3709019066084,
          "mean":133.23947457056252,
          "geometricmean":130.83072059647412,
          "sumoflogs":72353.10901272473,
          "skewness":0.7177265003819447}]}}
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
          "mean":42901.60996309963,
          "sum":116263363,
          "n":2710}]}}
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
          "mean":15.178130848001365,
          "sum":31339926,
          "n":2064808},
        {
          "key":"multiple - combined regexp",
          "mean":15.178130848001365,
          "sum":31339926,
          "n":2064808},
        {
          "key":"multiple - two queries",
          "mean":15.178130848001365,
          "sum":31339926,
          "n":2064808}]}}
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
          "mean":12.352043386116287,
          "sum":25504598,
          "n":2064808,
          "functions":{
            "number of words":{
              "sum":504361094,
              "n":2064808},
            "relative frequency":{
              "distribution(start=0,end=0.1,number=10)":{
                "[0.000,0.010)":390003,
                "[0.010,0.020)":120903,
                "[0.020,0.030)":173830,
                "[0.030,0.040)":209994,
                "[0.040,0.050)":245098,
                "[0.050,0.060)":253528,
                "[0.060,0.070)":218325,
                "[0.070,0.080)":163982,
                "[0.080,0.090)":115929,
                "[0.090,0.100)":77207},
              "mean":0.04538673326024501,
              "errorList":{"division by zero":1039},
              "standarddeviation":0.03284884758453086,
              "errorNumber":1039}}}]}}
```

<a name="ignore-and-maximumignorelength"></a>  

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
          "mean":12.352043386116287,
          "sum":25504598,
          "n":2064808,
          "functions":{
            "number of words":{
              "sum":504361094,
              "n":2064808},
            "relative frequency":{
              "distribution(start=0,end=0.1,number=10)":{
                "[0.000,0.010)":390003,
                "[0.010,0.020)":120903,
                "[0.020,0.030)":173830,
                "[0.030,0.040)":209994,
                "[0.040,0.050)":245098,
                "[0.050,0.060)":253528,
                "[0.060,0.070)":218325,
                "[0.070,0.080)":163982,
                "[0.080,0.090)":115929,
                "[0.090,0.100)":77207},
              "mean":0.04538673326024501,
              "errorList":{"division by zero":1039},
              "standarddeviation":0.03284884758453086,
              "errorNumber":1039}}}]}}
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
          "n":1890377,
          "functions":{
            "een - relative to LID":{
              "mean":0.26177400695591124,
              "errorList":{"division by zero":24175},
              "n":1890377,
              "errorNumber":24175},
            "LID - absolute":{
              "sum":44077220,
              "n":1890377},
            "de+het+een - relative to LID":{
              "mean":1.0864079360130154,
              "errorList":{"division by zero":24175},
              "n":1890377,
              "errorNumber":24175},
            "het - relative to LID":{
              "mean":0.2740826070638114,
              "errorList":{"division by zero":24175},
              "n":1890377,
              "errorNumber":24175},
            "een - relative to positions":{
              "mean":0.021631171906706374,
              "n":1890377},
            "een - absolute":{
              "sum":10620744,
              "n":1890377},
            "het - relative to positions":{
              "mean":0.02235754528581941,
              "n":1890377},
            "de - absolute":{
              "sum":25504598,
              "n":1890377},
            "het - absolute":{
              "sum":11530937,
              "n":1890377},
            "LID - relative to positions":{
              "mean":0.08693980190126971,
              "n":1890377},
            "de - relative to LID":{
              "mean":0.5505513219945993,
              "errorList":{"division by zero":24175},
              "n":1890377,
              "errorNumber":24175},
            "de - relative to positions":{
              "mean":0.049574709134571515,
              "n":1890377}}}]}}
```

---

##Lucene

To use statistics on the occurrence of a span [directly in Lucene](installation_lucene.html), *ComponentSpan* together with the provided *collect* method can be used. 