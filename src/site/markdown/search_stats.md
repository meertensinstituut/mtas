#Type of statistics

Mtas can produce several type of statistics, e.g. for [positions](search_query_stats_positions.html), 
[tokens](search_query_stats_tokens.html) or [spans](search_query_stats_spans.html).

In general, statistics of type *basic* will require less resources than statistics of type *advanced*, whereas
statistics of type *advanced* will require less than these of type *full*. If multiple 
statistics are possible and required, the parameters can be seperated with a comma. 


| Parameter             | Description          | Type      |
|-----------------------|----------------------|-----------|
| n                     | number of documents  | basic     |
| sum                   | total                | basic     |
| mean                  | mean                 | basic     |
| min                   | minimum              | advanced  |
| max                   | maximum              | advanced  |
| sumsq                 | sum of squares       | advanced  |
| sumoflogs             | sum of logs          | advanced  |
| geometricmean         | geometric mean       | advanced  |
| quadraticmean         | quadratic mean       | advanced  |
| variance              | variance             | advanced  |
| populationvariance    | population variance  | advanced  |
| standarddeviation     | standard deviation   | advanced  |
| skewness              | skewness             | full      |
| median                | median               | full      |
| kurtosis              | kurtosis             | full      |
| all                   | all of the above     | full      |

**Examples**

``` console 
n,sum
mean,standarddeviation
```

When not obligatory, usually \"*n,sum,mean*\" will be the default.

## Distribution

Besides these parameters, a distribution can requested with (optional) arguments, 
optionally combined with one or more of the parameters listed above.

| Parameter      | Type | Arguments    | Description         | Obligatory | 
|----------------|------|--------------|---------------------|------------|
| distribution() | full | start        | start value         | no         |
|                |      | end          | end value           | no         |
|                |      | step         | size of intervals   | no         |
|                |      | number       | number of intervals | no         |

**Examples**

``` console 
distribution()
distribution(start=0,end=1,number=10)
mean,standarddeviation,median,distribution(start=0,end=1,number=10)
```

