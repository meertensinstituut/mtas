# Prefix

Mtas can produce a list of available prefixes. To get this information, besides the parameter to enable [mtas queries](search_query.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.stats.prefix     | true   | yes         |

Information for multiple fields can be produced within the same request. To distinguish them, a unique identifier has to be provided for each of the required statistics. The list of available prefixes is independent of any restriction in the document set, and also prefixes of deleted documents can be taken into account when the core hasn't been optimized.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.stats.prefix.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.stats.prefix.\<identifier\>.field       | \<string\>   | mtas field                      | yes         |

The *key* is added to the response and may be used to distinguish between multiple lists, and should therefore be unique. The response will contain three lists: prefixes strictly used for single position tokens, prefixes (also) used for multiple position tokens and prefixes used for multiple non adjacent positions. Notice that the last list will always be a subset of the second list.

## Examples
1. [Basic](#basic) : list of available prefixes.

<a name="basic"></a>  

### Basic

**Example**  
List of avilable prefixes.

**Request and response**  
`q=*%3A*&mtas=true&mtas.prefix=true&mtas.prefix.0.field=text&mtas.prefix.0.key=example+-+basic&rows=0&wt=json&indent=true`

``` json
"mtas":{
    "prefix":[{
        "key":"example - basic",
        "singlePosition":["feat.buiging",
          "feat.conjtype",
          "feat.dial",
          "feat.genus",
          "feat.getal",
          "feat.getal-n",
          "feat.graad",
          "feat.head",
          "feat.lwtype",
          "feat.naamval",
          "feat.npagr",
          "feat.ntype",
          "feat.numtype",
          "feat.pdtype",
          "feat.persoon",
          "feat.positie",
          "feat.pvagr",
          "feat.pvtijd",
          "feat.spectype",
          "feat.status",
          "feat.vwtype",
          "feat.vztype",
          "feat.wvorm",
          "lemma",
          "morpheme",
          "pos",
          "t",
          "t_lc"],
        "multiplePosition":["div",
          "entity",
          "head",
          "p",
          "s"],
        "setPosition":["entity"]}]}
```
