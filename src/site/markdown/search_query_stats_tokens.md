#Statistics - tokens

To get statistics on the number of tokens within a set of documents in Solr requests, besides the parameter to enable [statistics](search_query_stats.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.stats.tokens     | true   | yes         |

Multiple statistics on tokens can be produced within the same request. 
To distinguish them, a unique identifier has to be provided for  
each of the required statistics.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.stats.tokens.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.stats.tokens.\<identifier\>.field       | \<string\>   | Mtas field                      | yes         |
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
          "mean":1949.101406523028,
          "sum":4024520177,
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
          "sumsq":2.91825668357275E14,
          "populationvariance":2.022964435797023E8,
          "max":3320612.0,
          "sum":3.837278477E9,
          "kurtosis":9580.99014557769,
          "standarddeviation":14223.100544366072,
          "n":1390207,
          "quadraticmean":14488.452755067281,
          "min":500.0,
          "median":1359.0,
          "variance":2.0229658909514648E8,
          "mean":2760.2209433559033,
          "geometricmean":1584.8982392362057,
          "sumoflogs":1.0243428152831953E7,
          "skewness":79.47215006871889},
        {
          "key":"example - maximum",
          "sumsq":3.33432806009E11,
          "populationvariance":65815.48228216589,
          "max":1000.0,
          "sum":5.49051031E8,
          "kurtosis":-0.9495132030213522,
          "standarddeviation":256.54539199058576,
          "n":1178024,
          "quadraticmean":532.0189410229931,
          "min":0.0,
          "median":441.0,
          "variance":65815.53815160331,
          "mean":466.07796700236827,
          "geometricmean":0.0,
          "sumoflogs":"-Infinity",
          "skewness":0.2518109944817064},
        {
          "key":"example - minimum and maximum",
          "sumsq":2.70110872559E11,
          "populationvariance":20021.06838039624,
          "max":1000.0,
          "sum":3.61809331E8,
          "kurtosis":-1.0824803795579663,
          "standarddeviation":141.49596513804715,
          "n":503423,
          "quadraticmean":732.4947329880449,
          "min":500.0,
          "median":704.0,
          "variance":20021.108150347452,
          "mean":718.6984523949043,
          "geometricmean":704.889293672351,
          "sumoflogs":3301468.553637138,
          "skewness":0.2634725299866506}]}}
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
          "mean":42901.60996309963,
          "sum":116263363,
          "n":2710}]}}
```

---

##Lucene

To use statistics on the number of tokens [directly in Lucene](installation_lucene.html), *ComponentToken* together with the provided *collect* method can be used. 