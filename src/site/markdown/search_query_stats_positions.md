# Statistics - positions

To get statistics on the number of positions within a set of documents, besides the parameter to enable [statistics](search_query_stats.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.stats.positions  | true   | yes         |

Multiple statistics on positions can be produced within the same request. 
To distinguish them, a unique identifier has to be provided for  
each of the required statistics.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.stats.positions.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.stats.positions.\<identifier\>.field       | \<string\>   | mtas field                      | yes         |
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
          "mean":163.32306296759796,
          "sum":337230767,
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
          "sumsq":3.217738942941E12,
          "populationvariance":4429860.598303376,
          "max":419252.0,
          "sum":3.03356085E8,
          "kurtosis":7475.629984106579,
          "standarddeviation":2104.724912671482,
          "n":696551,
          "quadraticmean":2149.309425767974,
          "min":100.0,
          "median":232.0,
          "variance":4429866.9580199765,
          "mean":435.5116638982477,
          "geometricmean":269.08829791186866,
          "sumoflogs":3897230.408057158,
          "skewness":69.96996634727896},
        {
          "key":"example - maximum",
          "sumsq":8.387258635E9,
          "populationvariance":2952.9318413516844,
          "max":200.0,
          "sum":7.6021711E7,
          "kurtosis":0.06752103336188053,
          "standarddeviation":54.34090186412456,
          "n":1664511,
          "quadraticmean":70.98501573316724,
          "min":0.0,
          "median":25.0,
          "variance":2952.9336154064163,
          "mean":45.672098892754136,
          "geometricmean":0.0,
          "sumoflogs":"-Infinity",
          "skewness":1.055453308387049},
        {
          "key":"example - minimum and maximum",
          "sumsq":6.242915333E9,
          "populationvariance":833.0814348912809,
          "max":200.0,
          "sum":4.2147029E7,
          "kurtosis":-1.089851452153094,
          "standarddeviation":28.86319883436408,
          "n":296254,
          "quadraticmean":145.16489726459713,
          "min":100.0,
          "median":139.0,
          "variance":833.084246952036,
          "mean":142.2665314223605,
          "geometricmean":139.38264759454245,
          "sumoflogs":1462672.066002271,
          "skewness":0.30992716799338843}]}}
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
          "mean":3578.5040620384048,
          "sum":9690589,
          "n":2708}]}}
```
