#Termvector

Mtas can produce termvectors for the set of documents satisfying the condition and/or filter. To get this information, in Solr requests, besides the parameter to enable [Mtas queries](search_query.html), the following parameter should be provided.

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
| mtas.termvector.\<identifier\>.start       | \<double\>   | offset for list of terms   | no         |
| mtas.termvector.\<identifier\>.type        | \<string\>   | required [type of statistics](search_stats.html) | no          |
| mtas.termvector.\<identifier\>.regexp       | \<string\>   | regular expression condition on term   | no         |
| mtas.termvector.\<identifier\>.ignoreRegexp       | \<string\>   | regular expression condition for terms that have to be ignored    | no         |
| mtas.termvector.\<identifier\>.sort.type       | \<string\>   | sort on term or [type of statistics](search_stats.html)     | no         |
| mtas.termvector.\<identifier\>.sort.direction       | \<string\>   | sort direction: asc or desc | no         |


## Full

When using distributed search, instead of applying the more efficient default algorithm where in two rounds lists of terms are collected and combined from the participating cores, also another approach can be used. Using the *full* option, the complete lists of terms (matching all requirements) is collected from the participating cores, and combined afterwards. This approach is likely to be less efficient when huge lists are involved, but necessary for example when results have to be sorted on specific statistics.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.termvector.\<identifier\>.full       | \<boolean\>   | compute full list of terms | no         |




**Lucene**

To use termvectors [directly in Lucene](installation_lucene.html), *ComponentTermvector* together with the provided *collect* method can be used. 


