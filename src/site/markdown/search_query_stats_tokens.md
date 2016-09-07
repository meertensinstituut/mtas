# Statistics - tokens

To produce statistics on the number of tokens within a set of documents, besides the parameter to enable [statistics](search_query_stats.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.stats.tokens     | true   | yes         |

Multiple statistics on tokens can be produced within the same request. 
To distinguish them, a unique identifier has to be provided for  
each of the required statistics.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.stats.tokens.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.stats.tokens.\<identifier\>.field       | \<string\>   | mtas field                      | yes         |
| mtas.stats.tokens.\<identifier\>.type        | \<string\>   | required [type of statistics](search_stats.html) | no          |
| mtas.stats.tokens.\<identifier\>.minimum     | \<double\>   | minimum number of tokens  | no          |
| mtas.stats.tokens.\<identifier\>.maximum     | \<double\>   | maximum number of tokens  | no          |

The *key* is added to the response and may be used to distinguish between multiple statistics on tokens, and should therefore be unique. The optional *minimum* and *maximum* can be used to focus only on documents satisfying a condition on the number of tokens.

---

## Examples
1. [Basic](#basic) : basic statistics on the number of tokens.
2. [Minimum and maximum](#minimum-and-maximum) : statistics on the number of tokens with restrictions on this number.
3. [Subset](#subset) : statistics on the number of tokens within a subset of documents.

---

<a name="basic"></a>

### Basic

**Example**  
Total and average number of tokens and the number of documents.

**Request and response**  
`q=*%3A*&rows=0&mtas=true&mtas.stats=true&mtas.stats.tokens=true&mtas.stats.tokens.0.field=text&mtas.stats.tokens.0.key=example - basic&mtas.stats.tokens.0.type=sum,mean,n&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "tokens":[{
          "key":"example - basic",
          "mean":2208.617131956095,
          "sum":4560370323,
          "n":2064808}]}}
```

<a name="minimum-and-maximum"></a>

### Minimum and maximum

**Example**  
Full statistics on tokens for documents with a minimum of 100 tokens, for documents with a maximum of 200 tokens, and for documents with between 100 and 200 tokens.

**Request and response**  
`q=*%3A*&rows=0&mtas=true&mtas.stats=true&mtas.stats.tokens=true&mtas.stats.tokens.0.field=text&mtas.stats.tokens.0.key=example - minimum&mtas.stats.tokens.0.type=all&mtas.stats.tokens.0.minimum=500&mtas.stats.tokens.1.field=text&mtas.stats.tokens.1.key=example - maximum&mtas.stats.tokens.1.type=all&mtas.stats.tokens.1.maximum=1000&mtas.stats.tokens.2.field=text&mtas.stats.tokens.2.key=example - minimum and maximum&mtas.stats.tokens.2.type=all&mtas.stats.tokens.2.minimum=500&mtas.stats.tokens.2.maximum=1000&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "tokens":[{
          "key":"example - minimum",
          "sumsq":6.17630129413745E14,
          "populationvariance":5.2902986678636354E8,
          "max":5626223.0,
          "sum":4.489695699E9,
          "kurtosis":11397.708386174421,
          "standarddeviation":23000.659411322642,
          "n":1133873,
          "quadraticmean":23338.98849418418,
          "min":500.0,
          "median":1788.0,
          "variance":5.290303333556648E8,
          "mean":3959.610731536747,
          "geometricmean":2073.1829663657477,
          "sumoflogs":8659207.101050543,
          "skewness":86.80974688421588},
        {
          "key":"example - maximum",
          "sumsq":1.88841931218E11,
          "populationvariance":100404.89269776225,
          "max":1000.0,
          "sum":2.84511144E8,
          "kurtosis":-0.5052359496974037,
          "standarddeviation":316.8674407373186,
          "n":1219963,
          "quadraticmean":393.4376224488639,
          "min":0.0,
          "median":0.0,
          "variance":100404.97499941812,
          "mean":233.2129285891795,
          "geometricmean":0.0,
          "sumoflogs":"-Infinity",
          "skewness":0.9900439471758536},
        {
          "key":"example - minimum and maximum",
          "sumsq":1.640713236E11,
          "populationvariance":20292.151766107923,
          "max":1000.0,
          "sum":2.1383652E8,
          "kurtosis":-1.1536799879589004,
          "standarddeviation":142.45077035455677,
          "n":289028,
          "quadraticmean":753.4360252467811,
          "min":500.0,
          "median":734.0,
          "variance":20292.221974606666,
          "mean":739.8470736399236,
          "geometricmean":725.9419502039267,
          "sumoflogs":1903963.2945452952,
          "skewness":0.08906106230519327}]}}
```

<a name="subset"></a>  

### Subset

**Example**  
Total and average number of tokens and the number of documents for a subset of documents.

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
