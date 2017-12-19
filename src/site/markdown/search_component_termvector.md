#Termvector

Mtas can produce termvectors for the set of documents satisfying the condition and/or filter. To get this information, in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.termvector       | true   | yes         |

Multiple termvector results can be produced within the same request. To distinguish them, a unique identifier has to be provided for each of the required document results.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.termvector.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.termvector.\<identifier\>.field       | \<string\>   | Mtas field                      | yes         |
| mtas.termvector.\<identifier\>.prefix       | \<string\>   | prefix                      |yes         |
| mtas.termvector.\<identifier\>.number       | \<double\>   | number of terms in list   | no         |
| mtas.termvector.\<identifier\>.start       | \<string\>   | begin list after provided term, only if sorted on term   | no         |
| mtas.termvector.\<identifier\>.type        | \<string\>   | required [type of statistics](search_stats.html) | no          |
| mtas.termvector.\<identifier\>.regexp       | \<string\>   | regular expression condition on term   | no         |
| mtas.termvector.\<identifier\>.ignoreRegexp       | \<string\>   | regular expression condition for terms that have to be ignored    | no         |
| mtas.termvector.\<identifier\>.sort.type       | \<string\>   | sort on term or [type of statistics](search_stats.html)     | no         |
| mtas.termvector.\<identifier\>.sort.direction       | \<string\>   | sort direction: asc or desc | no         |


## Full

When using distributed search, instead of applying the more efficient default algorithm where in two rounds lists of terms are collected and combined from the participating cores, also another approach can be used. Using the *full* option, the complete lists of terms (matching all requirements) is collected from the participating cores, and combined afterwards. This approach is likely to be less efficient when huge lists are involved, but necessary for example when results have to be sorted on specific statistics.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.termvector.\<identifier\>.full             | \<boolean\>  | compute full list of terms | no         |

## List

If a list of terms is provided, the termvector will be restricted to items from this list. These items may be configured to be interpreted as explicit terms or as regular expressions. 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.termvector.\<identifier\>.list       | [\<string\>,...]   | list of terms | yes |
| mtas.termvector.\<identifier\>.listRegexp | \<boolean\>  | interpret items in provided list as regular expressions | no         |

Furthermore,  a list of terms can be provided that should be ignored within the termvector. These items may also be configured to be interpreted as explicit terms or as regular expressions. 

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.termvector.\<identifier\>.ignoreList       | [\<string\>,...]   | list of terms | yes |
| mtas.termvector.\<identifier\>.ignoreListRegexp | \<boolean\>  | interpret items in provided ignoreList as regular expressions | no         |

## Distances

For each term in the termvector, the distance to a predefined `base` term can be computed. 

Two `type` of distance are available: [Levenshtein](https://en.wikipedia.org/wiki/Levenshtein_distance) and [Damerau–Levenshtein](https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance), each with configurable parameters to define the weight of the relevant operations.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.termvector.\<identifier\>.distance.\<identifier distance\>.key | \<string\>   | key used in response           | no          |
| mtas.termvector.\<identifier\>.distance.\<identifier distance\>.type    | \<string\>   | type of distance         | yes         |
| mtas.termvector.\<identifier\>.distance.\<identifier distance\>.base | \<string\>  | base term for distance | yes         |
| mtas.termvector.\<identifier\>.distance.\<identifier distance\>.maximum | \<double\>  | restrict termvector to terms with provided maximum | no         |
| mtas.termvector.\<identifier\>.distance.\<identifier distance\>.parameter.* | \<string\>  | type dependent parameters | no         |

The available type dependent additional parameters for type `levenshtein` and `damerau-levenshtein` are

| Type | Type dependent parameter         | Value        | Info                           | Default  |
|-----|-------------------------------------------------|--------------|--------------------------------|-------------|
| levenshtein, damerau-levenshtein | deletionDistance | \<double\>   | distance for a deletion | 1.0          |
| levenshtein, damerau-levenshtein | insertionDistance | \<double\>   | distance for an insertion | 1.0          |
| levenshtein, damerau-levenshtein | replaceDistance | \<double\>   | distance for a replacement | 1.0          |
| damerau-levenshtein | transpositionDistance | \<double\>   | distance for a transposition | 1.0          |

## Functions

Besides the specified statistics on hits over the documents, also statistics on the computed value of functions on the number of hits and the total number of words over the documents can be provided. In the definition of such a function, the number of hits is referred to as *$q0*, and the number of words is referred to as $n.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.termvector.\<identifier\>.function.\<identifier function\>.key | \<string\>   | key used in response           | no          |
| mtas.termvector.\<identifier\>.function.\<identifier function\>.expression    | \<string\>   | definition of function         | yes         |
| mtas.termvector.\<identifier\>.function.\<identifier function\>.type | \<string\>  | required [type of statistics](search_stats.html) | no         |


Again, the key is added to the response and may be used to distinguish between multiple functions, and should therefore be unique within each specified termvector.

---

## Examples
1. [Basic](#basic) : basic statistics on occurring part of speech
2. [Regexp](#regexp) : words of length 5 containing only characters a-z, sorted descending by number of hits
3. [Ignore](#ignore) : previous result, ignoring words ending with $-e$.
4. [List](#list) : termvector for provided list of words.
5. [Start](#start) : termvector for words containing only characters a-z sorted by term and &gt; *koe*.
6. [Distances](#distances) : termvector for words with Levenshtein distance from 1 or less sorted descending by frequency.
7. [Functions](#functions) : statistics on hits, relative frequency and total number of words in document for words containing only characters a-z.

---

<a name="basic"></a>  

### Basic

**Example**  
Total and average number of occurrences of part of speech (pos).

**Request and response**  
`q=*%3A*&mtas=true&mtas.termvector=true&mtas.termvector.0.key=example - basic&mtas.termvector.0.field=text&mtas.termvector.0.prefix=pos&mtas.termvector.0.number=3&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "termvector":[{
        "key":"example - basic",
        "list":[{
            "mean":200.22966889678833,
            "sum":25797991,
            "n":128842,
            "key":"ADJ"},
          {
            "mean":149.53835013602176,
            "sum":18689303,
            "n":124980,
            "key":"BW"},
          {
            "mean":459.93552395416265,
            "sum":59963634,
            "n":130374,
            "key":"LET"}]}]}
```

<a name="regexp"></a>  

### Regexp

**Example**  
List of words with length 5 and containing only characters a-z, sorted descending by number of hits.

**Regular expression**  
`[a-z]{5}`

**Request and response**  
`q=*%3A*&mtas=true&mtas.termvector=true&mtas.termvector.0.key=example - regexp&mtas.termvector.0.field=text&mtas.termvector.0.prefix=t_lc&mtas.termvector.0.number=5&mtas.termvector.0.type=n,sum&mtas.termvector.0.regexp=[a-z]{5}&mtas.termvector.0.sort.type=sum&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "termvector":[{
        "key":"example - regexp",
        "list":[{
            "sum":972687,
            "n":94160,
            "key":"heeft"},
          {
            "sum":645227,
            "n":84306,
            "key":"wordt"},
          {
            "sum":436038,
            "n":82453,
            "key":"onder"},
          {
            "sum":391488,
            "n":40512,
            "key":"zijne"},
          {
            "sum":314539,
            "n":62316,
            "key":"welke"}]}]}
```

<a name="ignore"></a>  

### Ignore

**Example**  
List of words with length 5 and containing only characters a-z, sorted descending by number of hits, ignoring all words ending with $-e$.

**Regular expressions**  
`[a-z]{5}`
`.*e`

**Request and response**  
`q=*%3A*&mtas=true&mtas.termvector=true&mtas.termvector.0.key=example - ignore&mtas.termvector.0.field=text&mtas.termvector.0.prefix=t_lc&mtas.termvector.0.number=5&mtas.termvector.0.type=n,sum&mtas.termvector.0.regexp=[a-z]{5}&mtas.termvector.0.ignoreRegexp=.*e&mtas.termvector.0.sort.type=sum&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "termvector":[{
        "key":"example - ignore",
        "list":[{
            "sum":972687,
            "n":94160,
            "key":"heeft"},
          {
            "sum":645227,
            "n":84306,
            "key":"wordt"},
          {
            "sum":436038,
            "n":82453,
            "key":"onder"},
          {
            "sum":304620,
            "n":60555,
            "key":"leven"},
          {
            "sum":297160,
            "n":58263,
            "key":"waren"}]}]}
```

<a name="basic"></a>  

### List

**Example**  
Termvector for provided list of words.

**List**
`koe,paard,schaap,geit,kip`

**Request and response**  
`q=*%3A*&mtas=true&mtas.termvector=true&mtas.termvector.0.key=example - list&mtas.termvector.0.field=text&mtas.termvector.0.prefix=t_lc&mtas.termvector.0.list=koe,paard,schaap,geit,kip&mtas.termvector.0.type=n,sum&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "termvector":[{
        "key":"example - list",
        "list":[{
            "sum":1128,
            "n":683,
            "key":"geit"},
          {
            "sum":1410,
            "n":864,
            "key":"kip"},
          {
            "sum":4432,
            "n":2344,
            "key":"koe"},
          {
            "sum":15478,
            "n":7436,
            "key":"paard"},
          {
            "sum":2154,
            "n":1591,
            "key":"schaap"}]}]}
```

<a name="start"></a>  

### Start

**Example**  
Termvector for words containing only characters a-z sorted by term and &gt; *koe*.

**Request and response**  
`q=*%3A*&mtas=true&mtas.termvector=true&mtas.termvector.0.key=example - start&mtas.termvector.0.field=text&mtas.termvector.0.prefix=t_lc&mtas.termvector.0.regexp=[a-z]*&mtas.termvector.0.number=5&mtas.termvector.0.start=koe&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "termvector":[{
        "key":"example - start",
        "list":[{
            "mean":2.0,
            "sum":2,
            "n":1,
            "key":"koea"},
          {
            "mean":1.0,
            "sum":1,
            "n":1,
            "key":"koeaan"},
          {
            "mean":1.0,
            "sum":2,
            "n":2,
            "key":"koeachtig"},
          {
            "mean":1.5,
            "sum":3,
            "n":2,
            "key":"koeachtige"},
          {
            "mean":1.0,
            "sum":2,
            "n":2,
            "key":"koeala"}]}]}
```

<a name="distances"></a>  

### Distances

**Example**  
List of words, sorted descending by number of hits, with at most Levenshtein distance 1 from `regering`. For each word, the Levenshtein distance from `regering` is computed, and also the Damerau-Levensthein distance from `regering` with specific weights for *deletion*, *insertion*, *replacement* and *transposition*.

**Request and response**  
`q=*%3A*&mtas=true&mtas.termvector=true&mtas.termvector.0.field=text&mtas.termvector.0.prefix=t_lc&mtas.termvector.0.key=distance&mtas.termvector.0.distance.0.type=levenshtein&mtas.termvector.0.distance.0.base=regering&mtas.termvector.0.distance.0.key=Levenshtein&mtas.termvector.0.distance.0.maximum=1&mtas.termvector.0.distance.1.type=damerau-levenshtein&mtas.termvector.0.distance.1.base=regering&mtas.termvector.0.distance.1.key=Damerau-Levenshtein&mtas.termvector.0.distance.1.parameter.deletionDistance=0.81&mtas.termvector.0.distance.1.parameter.insertionDistance=0.82&mtas.termvector.0.distance.1.parameter.replaceDistance=0.83&mtas.termvector.0.distance.1.parameter.transpositionDistance=0.84&mtas.termvector.0.number=5&mtas.termvector.0.sort.type=sum&mtas.termvector.0.sort.direction=desc&mtas.termvector.0.full=true&rows=0&wt=json`

``` json
"mtas":{
    "termvector":[{
        "key":"distance",
        "listTotal":49,
        "list":[{
            "distance":{
              "Levenshtein":0.0,
              "Damerau-Levenshtein":0.0},
            "mean":1.7536344857153994,
            "sum":134979,
            "n":76971,
            "key":"regering"},
          {
            "distance":{
              "Levenshtein":1.0,
              "Damerau-Levenshtein":0.83},
            "mean":1.6863562423749492,
            "sum":16587,
            "n":9836,
            "key":"regeling"},
          {
            "distance":{
              "Levenshtein":1.0,
              "Damerau-Levenshtein":0.81},
            "mean":1.0262390670553936,
            "sum":352,
            "n":343,
            "key":"regerings"},
          {
            "distance":{
              "Levenshtein":1.0,
              "Damerau-Levenshtein":0.83},
            "mean":1.4080459770114941,
            "sum":245,
            "n":174,
            "key":"legering"},
          {
            "distance":{
              "Levenshtein":1.0,
              "Damerau-Levenshtein":0.81},
            "mean":1.0,
            "sum":97,
            "n":97,
            "key":"regering."}]}]}
```

<a name="functions"></a>  

### Functions

**Example**  
List of words containing only characters a-z, sorted descending by number of hits, with statistics on hits, relative frequency and total number of words in document.

**Regular expression**  
`[a-z]*`

**Functions**  
`$q0/$n`
`$n`

**Request and response**  
`q=*%3A*&mtas=true&mtas.termvector=true&mtas.termvector.0.key=example - list&mtas.termvector.0.field=text&mtas.termvector.0.prefix=t_lc&mtas.termvector.0.regexp=[a-z]*&mtas.termvector.0.sort.type=sum&mtas.termvector.0.type=n,sum&mtas.termvector.0.function.0.expression=%24q0%2F%24n&mtas.termvector.0.function.0.key=relative+frequency&mtas.termvector.0.function.0.type=n%2Cmean&mtas.termvector.0.function.1.expression=%24n&mtas.termvector.0.function.1.key=total+number+of+words&mtas.termvector.0.function.1.type=n%2Csum&mtas.termvector.0.number=3&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "termvector":[{
        "key":"example - list",
        "list":[{
            "sum":15975272,
            "n":127444,
            "functions":{
              "total number of words":{
                "sum":391924648,
                "n":127444},
              "relative frequency":{
                "mean":0.040967994034336694,
                "n":127444}},
            "key":"de"},
          {
            "sum":10565895,
            "n":126197,
            "functions":{
              "total number of words":{
                "sum":391190126,
                "n":126197},
              "relative frequency":{
                "mean":0.028072930308247233,
                "n":126197}},
            "key":"van"},
          {
            "sum":8798835,
            "n":125415,
            "functions":{
              "total number of words":{
                "sum":391306760,
                "n":125415},
              "relative frequency":{
                "mean":0.02376864203286862,
                "n":125415}},
            "key":"en"}]}]}
```

**Lucene**

To use termvectors [directly in Lucene](installation_lucene.html), *ComponentTermvector* together with the provided *collect* method can be used. 


