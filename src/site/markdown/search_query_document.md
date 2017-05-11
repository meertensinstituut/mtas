# Document

Mtas can produce statistics on used terms for the individual listed documents. To get this information, in Solr requests, besides the parameter to enable [Mtas queries](search_query.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.document            | true   | yes         |

Multiple document results can be produced within the same request. To distinguish them, a unique identifier has to be provided for each of the required document results.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.document.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.document.\<identifier\>.field       | \<string\>   | Mtas field                      | yes         |
| mtas.document.\<identifier\>.prefix       | \<string\>   | prefix                      |yes         |
| mtas.document.\<identifier\>.number       | \<double\>   | create list with specified number of most frequent items   | no         |
| mtas.document.\<identifier\>.type        | \<string\>   | required [type of statistics](search_stats.html) | no          |
| mtas.document.\<identifier\>.regexp       | \<string\>   | regular expression condition on term                     | no         |
| mtas.document.\<identifier\>.ignoreRegexp       | \<string\>   | regular expression condition for terms that have to be ignored    | no         |

## List

A list can be provided, specifying the set of terms to consider when computing the result.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.document.\<identifier\>.list         | \<string\>   | comma separated list of values           | yes          | 
| mtas.document.\<identifier\>.listRegexp  | \<boolean\>   | list of values are to be interpreted as regular expressions           | no          |
| mtas.document.\<identifier\>.listExpand  | \<boolean\>   | expand the matches on values from list | no          |
| mtas.document.\<identifier\>.listExpandNumber  | \<boolean\>   | number of expansions of matches on values from list | no          | 

## Ignore list

Also a ignore list can be provided, specifying the set of terms not to consider when computing the result.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.document.\<identifier\>.ignoreList         | \<string\>   | comma separated list of values           | yes          | 
| mtas.document.\<identifier\>.ignoreListRegexp  | \<boolean\>   | list of values are to be interpreted as regular expressions           | no         |

---

## Examples
1. [Basic](#basic) : Statistics unique words for each document
2. [Regexp](#regexp) : Most frequent words containing only letters a-z and minimum length 5
3. [List](#list) : Statistics for a provided list of words
4. [Ignore](#ignore) : Statistics for a provided list of regular expressions, ignoring another list of regular expressions

---

<a name="basic"></a>  

### Basic

**Example**  
Statistics for set of unique tokens with prefix *t* (words) for each listed document.


**Request and response**  
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%5B%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.document=true&mtas.document.0.field=text&mtas.document.0.prefix=t&mtas.document.0.key=words&mtas.document.0.type=all&fl=*&start=0&rows=2&wt=json&indent=true`

```json
"mtas":{
    "document":[{
        "key":"words",
        "list":[{
            "documentKey":"4115a95c-011c-11e4-b0ff-51bcbd7c379f",
            "sumsq":113964.0,
            "populationvariance":126.5639231447591,
            "max":166.0,
            "sum":3336.0,
            "kurtosis":92.19837080635624,
            "standarddeviation":11.257199352433314,
            "n":789,
            "quadraticmean":12.01836364230935,
            "min":1.0,
            "median":1.0,
            "variance":126.72453726042504,
            "mean":4.228136882129286,
            "geometricmean":1.9285975498109995,
            "sumoflogs":518.209740627951,
            "skewness":8.377350653392202},
          {
            "documentKey":"4115aac4-011c-11e4-b0ff-51bcbd7c379f",
            "sumsq":25489.0,
            "populationvariance":35.695641666666134,
            "max":77.0,
            "sum":1563.0,
            "kurtosis":72.57030420433823,
            "standarddeviation":5.979568021426876,
            "n":600,
            "quadraticmean":6.517796151051877,
            "min":1.0,
            "median":1.0,
            "variance":35.75523372287092,
            "mean":2.6050000000000004,
            "geometricmean":1.5249529474773036,
            "sumoflogs":253.1781332820801,
            "skewness":7.70682353088895}]}]}
```

<a name="regexp"></a>  

### Regexp

**Example**  
Most frequent tokens containing only letters a-z and minimum length 5 with prefix *t* (words) for each listed document.

**Regexp**<br/>  
`[a-z]{5,}`

**Request and response**  
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%5B%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.document=true&mtas.document.0.field=NLContent_mtas&mtas.document.0.prefix=t&mtas.document.0.key=list+of+words&mtas.document.0.type=n%2Csum%2Cmean&mtas.document.0.regexp=%5Ba-z%5D%7B5%2C%7D&mtas.document.0.number=5&fl=%2A&start=0&rows=2&wt=json&indent=true`

```json
"mtas":{
    "document":[{
        "key":"list of words",
        "list":[{
            "documentKey":"c0c4200c-1eee-11e5-b891-f48ce0be173a",
            "list":[{
                "sum":471,
                "key":"zijne"},
              {
                "sum":317,
                "key":"eenen"},
              {
                "sum":304,
                "key":"zegde"},
              {
                "sum":249,
                "key":"hebben"},
              {
                "sum":229,
                "key":"welke"}],
            "mean":4.552402402402403,
            "sum":30319,
            "n":6660},
          {
            "documentKey":"c0c453d8-1eee-11e5-b891-f48ce0be173a",
            "list":[{
                "sum":348,
                "key":"heeft"},
              {
                "sum":243,
                "key":"hebben"},
              {
                "sum":199,
                "key":"prins"},
              {
                "sum":173,
                "key":"vader"},
              {
                "sum":161,
                "key":"komen"}],
            "mean":4.641632967456191,
            "sum":24104,
            "n":5193}]}]}
```

<a name="list"></a>  

### List

**Example**  
Statistics for a provided list of words for each listed document.

**List**<br/>
`koe,paard,schaap,geit,kip`

**Request and response**  
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%5Bt_lc%3D%5C%22koe%5C%22%7Ct_lc%3D%5C%22paard%5C%22%7Ct_lc%3D%5C%22schaap%5C%22%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.document=true&mtas.document.0.field=text&mtas.document.0.prefix=t_lc&mtas.document.0.key=list+of+words&mtas.document.0.type=n%2Csum%2Cmean&mtas.document.0.list=koe%2Cpaard%2Cschaap%2Cgeit%2Ckip&mtas.document.0.listRegexp=false&mtas.document.0.listExpand=false&mtas.document.0.number=100&fl=%2A&start=0&rows=2&wt=json&indent=true`

```json
"mtas":{
    "document":[{
        "key":"list of words",
        "list":[{
            "documentKey":"c0c46b7a-1eee-11e5-b891-f48ce0be173a",
            "list":[{
                "sum":3,
                "key":"paard"},
              {
                "sum":2,
                "key":"schaap"}],
            "mean":2.5,
            "sum":5,
            "n":2},
          {
            "documentKey":"c0c453d8-1eee-11e5-b891-f48ce0be173a",
            "list":[{
                "sum":31,
                "key":"paard"},
              {
                "sum":1,
                "key":"kip"}],
            "mean":16.0,
            "sum":32,
            "n":2}]}]}
```

<a name="ignore"></a>  

### Ignore

**Example**  
Statistics for a provided list of regular expressions, ignoring another list of regular expressions for each listed document.

**Regexp**<br/>
`[a-z]{7,}`

**Ignore**<br/>
`[a-z]{10,}`

**List**<br/>
`een.*,.*heid`

**Ignore list**<br/>
`een.*heid,ee.*nheid`

**Request and response**  
`fq=%7B%21mtas_cql+field%3D%22text%22+query%3D%22%5Bt_lc%3D%5C%22eenheid%5C%22%5D%22+++%7D&q=%2A%3A%2A&mtas=true&mtas.document=true&mtas.document.0.field=text&mtas.document.0.prefix=t_lc&mtas.document.0.key=advanced+list+of+words&mtas.document.0.type=n%2Csum%2Cmean&mtas.document.0.regexp=%5Ba-z%5D%7B7%2C%7D&mtas.document.0.list=een.%2A%2C.%2Aheid&mtas.document.0.listRegexp=true&mtas.document.0.listExpand=true&mtas.document.0.listExpandNumber=3&mtas.document.0.ignoreRegexp=%5Ba-z%5D%7B10%2C%7D&mtas.document.0.ignoreList=een.%2Aheid%2Cee.%2Anheid&mtas.document.0.ignoreListRegexp=true&mtas.document.0.number=10&fl=text_numberOfPositions%2CNLCore_NLIdentification_nederlabID%2CNLProfile_name%2CNLTitle_title&start=0&rows=2&wt=json&indent=true`

```json
"mtas":{
    "document":[{
        "key":"advanced list of words",
        "list":[{
            "documentKey":"c0c41486-1eee-11e5-b891-f48ce0be173a",
            "list":[{
                "sum":166,
                "list":{
                  "droefheid":{
                    "sum":36},
                  "godheid":{
                    "sum":22},
                  "waarheid":{
                    "sum":22}},
                "key":".*heid"},
              {
                "sum":93,
                "list":{
                  "eenigen":{
                    "sum":46},
                  "eensklaps":{
                    "sum":32},
                  "eenigste":{
                    "sum":3}},
                "key":"een.*"}],
            "mean":5.886363636363637,
            "sum":259,
            "n":44},
          {
            "documentKey":"c0c453d8-1eee-11e5-b891-f48ce0be173a",
            "list":[{
                "sum":36,
                "list":{
                  "afscheid":{
                    "sum":12},
                  "hoogheid":{
                    "sum":4},
                  "bezigheid":{
                    "sum":3}},
                "key":".*heid"},
              {
                "sum":24,
                "list":{
                  "eenvoudig":{
                    "sum":15},
                  "eenzame":{
                    "sum":3},
                  "eenmaal":{
                    "sum":2}},
                "key":"een.*"}],
            "mean":3.1578947368421053,
            "sum":60,
            "n":19}]}]}
```

---

**Lucene**

To get statistics on used terms for the listed documents [directly in Lucene](installation_lucene.html), *ComponentDocument* together with the provided *collect* method can be used.
