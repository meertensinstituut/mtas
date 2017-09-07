#Statistics - positions

To get statistics on the number of positions within a set of documents in Solr requests, besides the parameter to enable [statistics](search_component_stats.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.stats.positions  | true   | yes         |

Multiple statistics on positions can be produced within the same request. 
To distinguish them, a unique identifier has to be provided for  
each of the required statistics.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.stats.positions.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.stats.positions.\<identifier\>.field       | \<string\>   | Mtas field                      | yes         |
| mtas.stats.positions.\<identifier\>.type        | \<string\>   | required [type of statistics](search_stats.html) | no          |
| mtas.stats.positions.\<identifier\>.minimum     | \<double\>   | minimum number of positions  | no          |
| mtas.stats.positions.\<identifier\>.maximum     | \<double\>   | maximum number of positions  | no          |

The *key* is added to the response and may be used to distinguish between multiple statistics on positions, and should therefore be unique. The optional *minimum* and *maximum* can be used to focus only on documents satisfying a condition on the number of positions.

---

## Examples
1. [Basic](#basic) : basic statistics on the number of positions.
2. [Minimum and maximum](#minimum-and-maximum) : statistics on the number of positions with restrictions on this number.
3. [Subset](#subset) : statistics on the number of positions within a subset of documents.

---

<a name="basic"></a>  

### Basic

**Example**  
Total and average number of positions and the number of documents.

**Request and response**  
`q=*%3A*&rows=0&mtas=true&mtas.stats=true&mtas.stats.positions=true&mtas.stats.positions.0.field=text&mtas.stats.positions.0.key=example - basic&mtas.stats.positions.0.type=sum,mean,n&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "positions":[{
          "key":"example - basic",
          "mean":244.26537188929916,
          "sum":504361094,
          "n":2064808}]}}
```

<a name="minimum-and-maximum"></a>

### Minimum and maximum

**Example**  
Full statistics on positions for documents with a minimum of 100 positions, for documents with a maximum of 200 positions, and for documents with between 100 and 200 positions.

**Request and response**  
`q=*%3A*&rows=0&mtas=true&mtas.stats=true&mtas.stats.positions=true&mtas.stats.positions.0.field=text&mtas.stats.positions.0.key=example - minimum&mtas.stats.positions.0.type=all&mtas.stats.positions.0.minimum=100&mtas.stats.positions.1.field=text&mtas.stats.positions.1.key=example - maximum&mtas.stats.positions.1.type=all&mtas.stats.positions.1.maximum=200&mtas.stats.positions.2.field=text&mtas.stats.positions.2.key=example - minimum and maximum&mtas.stats.positions.2.type=all&mtas.stats.positions.2.minimum=100&mtas.stats.positions.2.maximum=200&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "positions":[{
          "key":"example - minimum",
          "sumsq":4.407777345501E12,
          "populationvariance":4021377.043206717,
          "max":419252.0,
          "sum":4.53494907E8,
          "kurtosis":7589.040501278469,
          "standarddeviation":2005.3380969650148,
          "n":1047253,
          "quadraticmean":2051.5590305379797,
          "min":100.0,
          "median":232.0,
          "variance":4021380.883139267,
          "mean":433.0328077360544,
          "geometricmean":269.1549624469481,
          "sumoflogs":5859681.392265234,
          "skewness":70.39565176567714},
        {
          "key":"example - maximum",
          "sumsq":1.2589493055E10,
          "populationvariance":2516.516960673755,
          "max":200.0,
          "sum":1.14146849E8,
          "kurtosis":-0.5513713934014715,
          "standarddeviation":50.164914844725146,
          "n":1462493,
          "quadraticmean":92.78060994263417,
          "min":0.0,
          "median":68.0,
          "variance":2516.5186813785253,
          "mean":78.04950109162947,
          "geometricmean":0.0,
          "sumoflogs":"-Infinity",
          "skewness":0.6202671670124106},
        {
          "key":"example - minimum and maximum",
          "sumsq":9.370630488E9,
          "populationvariance":832.9926334704653,
          "max":200.0,
          "sum":6.3280662E7,
          "kurtosis":-1.0893405044786282,
          "standarddeviation":28.861644194831847,
          "n":444938,
          "quadraticmean":145.12246855142547,
          "min":100.0,
          "median":139.0,
          "variance":832.9945056290709,
          "mean":142.22355024745016,
          "geometricmean":139.3394542837307,
          "sumoflogs":2196620.2289446634,
          "skewness":0.31081665704505534}]}}
```

<a name="subset"></a>  

### Subset

**Example**  
Total and average number of positions and the number of documents for a subset of documents.

**Request and response**  
`q=text:koe&rows=0&mtas=true&mtas.stats=true&mtas.stats.positions=true&mtas.stats.positions.0.field=text&mtas.stats.positions.0.key=example - subset&mtas.stats.positions.0.type=sum,mean,n&wt=json&indent=true`

``` json
"mtas":{
    "stats":{
      "positions":[{
          "key":"example - subset",
          "mean":5265.321033210332,
          "sum":14269020,
          "n":2710}]}}
```

---

##Lucene

To use statistics on the number of positions [directly in Lucene](installation_lucene.html), *ComponentPosition* together with the provided *collect* method can be used. 
