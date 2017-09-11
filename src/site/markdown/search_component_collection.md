#Collection

Mtas provides a method to join query results, based on (temporary) storing lists of values, and reusing these stored lists or collections into following queries.  

To manage collections, in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be provided.

| Parameter             | Value  | Obligatory  |
|-----------------------|--------|-------------|
| mtas.collection       | true   | yes         |

Multiple actions can be performed within the same request. To distinguish them, a unique identifier has to be provided for each of the required operations. 

---

##Create

To make a new collection based on the set of unique values from one or multiple fieldnames, the `create` action can be used.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.collection.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.collection.\<identifier\>.action        | create   |           | yes         |
| mtas.collection.\<identifier\>.field      | \<string\>   | one or more comma separated fieldnames     | yes         |
| mtas.collection.\<identifier\>.id      | \<string\>   | identifier | no        |

**Example**  
Create a collection with identifier `123` for field `genre`


**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.collection=true&mtas.collection.0.key=create collection&mtas.collection.0.action=create&mtas.collection.0.field=genre&mtas.collection.0.id=123&indent=true`

```json
"mtas":{
    "collection":[{
        "key":"create collection",
        "now":1505128371721,
        "id":"123",
        "size":3,
        "version":"45079e9c-b0d6-441c-bb2a-34a46a3b52fa",
        "expiration":1505214771721}]}
```

The values will be restricted to the set occurring within the listed fields for the set of documents matching the request. The optional provided identifier should be an unique string that can be used later on in other requests to refer to this set of data. If no identifier is provided, a new identifier is generated. Sharding is fully supported, i.e. the values are collected from all participating shards, and stored on both the main core and all these shards.

---

##Check

To check availablity of a collection if an `identitfier` is available, the `check` action can be used.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.collection.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.collection.\<identifier\>.action        | check   |           | yes         |
| mtas.collection.\<identifier\>.id      | \<string\>   | identifier | yes        |

**Example**  
Check collection with identifier `123` and `456`, and update `expiration` if (still) available.


**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.collection=true&mtas.collection.0.key=check existing collection&mtas.collection.0.action=check&mtas.collection.0.id=123&mtas.collection.1.key=check non-existing collection&mtas.collection.1.action=check&mtas.collection.1.id=456&indent=true`

```json
"mtas":{
    "collection":[{
        "key":"check existing collection",
        "now":1505128431128,
        "id":"123",
        "size":3,
        "version":"45079e9c-b0d6-441c-bb2a-34a46a3b52fa",
        "expiration":1505214831128},
      {
        "key":"check non-existing collection"}]}
```

Sharding is fully supported, i.e. the collection is also checked on all participating shards, and even copied to this core if it does not (already) contain this collection.

---

##Post

To post a collection, the `post` action can be used.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.collection.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.collection.\<identifier\>.action        | post   |           | yes         |
| mtas.collection.\<identifier\>.post      | \<string\>   | array in json format | yes        |

**Example**  
Post collection `a,b,c,d,e,f` with identifier `789`.

**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.collection=true&mtas.collection.0.key=post collection&mtas.collection.0.action=post&mtas.collection.0.post=["a","b","c","d","e","f"]&mtas.collection.0.id=789&indent=true`

```json
"mtas":{
    "collection":[{
        "key":"post collection",
        "now":1505128477275,
        "id":"789",
        "size":6,
        "version":"d45a6e77-32c9-47a1-b5ae-00989dbcefd9",
        "expiration":1505214877275}]}
```

The optional provided identifier should be an unique string that can be used later on in other requests to refer to this set of data. If no identifier is provided, a new identifier is generated. Sharding is fully supported, i.e. the values are stored on both the main core and all shards.

---

##List

To list all available collections, the `list` action can be used.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.collection.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.collection.\<identifier\>.action        | list   |           | yes         |

**Example**  
List all available collections.

**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.collection=true&mtas.collection.0.key=list collections&mtas.collection.0.action=list&indent=true`

```json
"mtas":{
    "collection":[{
        "key":"list collections",
        "now":1505128551273,
        "list":[{
            "id":"123",
            "size":3,
            "version":"45079e9c-b0d6-441c-bb2a-34a46a3b52fa",
            "expiration":1505214831128},
          {
            "id":"789",
            "size":6,
            "version":"d45a6e77-32c9-47a1-b5ae-00989dbcefd9",
            "expiration":1505214877275}]}]}
```

Again, sharding is supported, and availablity on the participating cores is also displayed.

---

##Import

To list all available collections, the `list` action can be used.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.collection.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.collection.\<identifier\>.action        | import   |           | yes         |
| mtas.collection.\<identifier\>.id        | \<string\>   |   | no         |
| mtas.collection.\<identifier\>.url        | \<string\>   |  url of source solr core  | yes         |
| mtas.collection.\<identifier\>.collection        | \<string\>   | identifier of source collection    | yes         |



**Example**  
Get collection `123` from core `http://solr/core/` and store it with identifier `abc`.

**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.collection=true&mtas.collection.0.key=import collection&mtas.collection.0.action=import&mtas.collection.0.url=http://solr/core/&mtas.collection.0.collection=123&mtas.collection.0.id=abc&indent=true`

```json
"mtas":{
    "collection":[{
        "key":"import collection",
        "now":1505129081870,
        "id":"abc",
        "size":3,
        "version":"1ac0f4bd-2d8a-46d8-878f-608c1023d3ba",
        "expiration":1505215481870}]}
```

The optional provided identifier should be an unique string that can be used later on in other requests to refer to this set of data. If no identifier is provided, a new identifier is generated. Sharding is fully supported, i.e. the values are stored on both the main core and all shards.

---

##Delete

To delete a collection, the `delete` action can be used.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.collection.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.collection.\<identifier\>.action        | delete   |           | yes         |
| mtas.collection.\<identifier\>.id      | \<string\>   | identifier | yes        |

**Example**  
Delete collection with identifier `123`.

**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.collection=true&mtas.collection.0.key=delete collection&mtas.collection.0.action=delete&mtas.collection.0.id=123&indent=true`

```json
"mtas":{
    "collection":[{
        "key":"delete collection"}]}
```

Sharding is fully supported, i.e. the collection is also deleted on all participating shards.

---

##Empty

To remove all collections, the `empty` action can be used.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.collection.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.collection.\<identifier\>.action        | empty   |           | yes         |

**Example**  
Delete all collections.

**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.collection=true&mtas.collection.0.key=delete all collections&mtas.collection.0.action=empty&indent=true`

```json
"mtas":{
    "collection":[{
        "key":"delete all collections"}]}
```

Sharding is fully supported, i.e. all collections are also deleted on  participating shards.

---

##Get

To get a collection, the `get` action can be used.

| Parameter                                       | Value        | Info                           | Obligatory  |
|-------------------------------------------------|--------------|--------------------------------|-------------|
| mtas.collection.\<identifier\>.key         | \<string\>   | key used in response           | no          |
| mtas.collection.\<identifier\>.action        | get   |           | yes         |
| mtas.collection.\<identifier\>.id         | \<string\>   | identifier           | yes          |

**Example**  
Get collection `789`.

**Request and response**  
`q=*:*&rows=0&wt=json&mtas=true&mtas.collection=true&mtas.collection.0.key=get collection&mtas.collection.0.action=get&mtas.collection.0.id=789&indent=true`

```json
"mtas":{
    "collection":[{
        "key":"get collection",
        "now":1505130978997,
        "id":"789",
        "size":6,
        "version":"d45a6e77-32c9-47a1-b5ae-00989dbcefd9",
        "expiration":1505217378997,
        "values":["a",
          "b",
          "c",
          "d",
          "e",
          "f"]}]}
```

Sharding is fully supported, i.e. if found, the collection is also searched for on participating cores.



