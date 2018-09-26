#FoLiA

For indexing [FoLiA](https://proycon.github.io/folia/) resources, the *mtas.analysis.parser.MtasFoliaParser* extending the abstract *MtasXMLParser* is available; full examples of configuration files are provided on [GitHub](https://github.com/meertensinstituut/mtas/tree/master/conf/parser/nederlab/mtas).

```xml
<!-- START CONFIGURATION MTAS PARSER -->
<parser name="mtas.analysis.parser.MtasFoliaParser">
...
  <!-- START MAPPINGS -->
  <mappings>
  ...
  </mapping>
  <!-- END MAPPINGS --->
  ...
</parser>
<!-- END CONFIGURATION MTAS PARSER -->
```

The [configuration file](indexing_configuration.html#configuration) defining the [mapping](indexing_mapping.html) has some specific settings for the FoLiA parser distinguishing several types of elements within the XML-based FoLiA resource: 

* [words](indexing_formats_folia.html#word) : the basic tokenisation layer
* [wordAnnotations](indexing_formats_folia.html#wordAnnotation) : annotations occurring within a word
* [groups](indexing_formats_folia.html#group) : containing one or multiple words
* [groupAnnotations](indexing_formats_folia.html#groupAnnotation) occurring within a group 
* [relations](indexing_formats_folia.html#relation) : containing one or multiple references
* [relationAnnotations](indexing_formats_folia.html#relationAnnotation) : occuring within a relation
* [references](indexing_formats_folia.html#reference) : elements referring to (typically) words by id

Inside the *mappings* part of the configuration file, all elements are defined that may be mapped onto the index structure : *words*, *wordAnnotations*, *groups*, *groupAnnotations* and *relations*. Outside the *mappings* part the references can be defined, since a reference itself will never be mapped directly onto the index structure. 

The use and meaning of the different elements is illustrated and explained by some examples.  

<a name="word"></a>**Words**

In the parser configuration, a word can be defined by 

```xml
<mapping type="word" name="w">
</mapping>
```

This will recognize every occurring w-tag within the FoLiA-resource as a word, defining the basic tokenization to be used in the mapping. To add a token for each occurring word, we have to add a token definition, for example

```xml
<mapping type="word" name="w">
  <token type="string" offset="false" realoffset="false" parent="false">
    <pre>
      <item type="name" />
    </pre>    
  </token>
</mapping>
```

Here the *prefix* is chosen to equal the *name* of the matching tag, and no *offset*, *realOffset* or *parent* will be included. To only add tokens conditionally, and/or to include the value from for example a provided attribute, we can define for example

```xml
<mapping type="word" name="w">
  <token type="string" offset="false" realoffset="false" parent="false">
    <pre>
      <item type="name" />
    </pre>
    <post>
      <item type="attribute" name="class" />
    </post>
  </token>
  <condition>
    <item type="attribute" name="class" />
    <item type="attribute" name="class" not="true" condition="WORD" />
  </condition>
</mapping>
```

This will add tokens to the index for all w-tags with the attribute *class* set and unequal to "WORD". The resulting single position tokens will have *prefix* value "w" and *postfix* value equal to the provided *class*.

If *parent* was set to *true*, the id of the first parenting [group](indexing_formats_group.html) would have been used as *parentId* for the resulting token.


<a name="wordAnnotation"></a>**Word annotations**

All elements occurring within a [word](indexing_formats_folia.html#word) can be defined as *wordAnnotation*. 

```xml
<mapping type="wordAnnotation" name="lemma">
  <token type="string" offset="false" realoffset="false" parent="false">
    <pre>
      <item type="name" />
    </pre>
    <post>
      <item type="attribute" name="class" />
    </post>
  </token>
  <condition>
    <item type="attribute" name="class" />
  </condition>
</mapping>
```

As illustrated in the next sample, not only attributes can be used, but also the text value within a matching tag. Furthermore, also multiple tokens can be generated from the same matching element. Finally, a filter may be applied.

```xml
<mapping type="wordAnnotation" name="t">
  <token type="string" offset="false" parent="true">
    <pre>
      <item type="name" />
    </pre>
    <post>
      <item type="text" />
    </post>
  </token>
  <token type="string" offset="false" realoffset="false" parent="false">
    <pre>
      <item type="name" />
      <item type="string" value="_lc" />
    </pre>
    <post>
      <item type="text" filter="ascii,lowercase" />
    </post>
  </token>  
</mapping>
```

If *parent* is set to true, the id of the first parenting [group](indexing_formats_group.html) will be used as *parentId* for the generated token.

<a name="group"></a>**Groups**

Elements containing one or multiple [words](index_formats_folia.html#word) can be defined as *group*.

```xml
<mapping type="group" name="s">
  <token type="string" offset="false" parent="true">
    <pre>
      <item type="name" />
    </pre>
    <post>
      <item type="attribute" name="class" />
    </post>
  </token>
</mapping>
```

The id of the first parenting group is used as *parentId*.

<a name="groupAnnotation"></a>**Group annotations**

Elements within a [group](index_formats_folia.html#group) and not containing one or multiple [words](index_formats_folia.html#word) can be defined as *groupAnnotation*.

```xml
<mapping type="groupAnnotation" name="lang">
  <token type="string" offset="false" realoffset="false" parent="false">
    <pre>
      <item type="name" />
    </pre>
    <post>
      <item type="attribute" name="class" />
    </post>
  </token>
</mapping>
```

If *parent* was set to *true*, the id of the first parenting [group](indexing_formats_group.html) would have been used as *parentId* for the resulting token.

<a name="relation"></a>**Relations**

Elements containing one or multiple [references](index_formats_folia.html#reference) and not containing one or multiple [words](index_formats_folia.html#word) can be defined as *relation*.

```xml
<mapping type="relation" name="entities">
</mapping>
<mapping type="relation" name="entity">
  <token type="string" offset="false" realoffset="false" parent="false">
    <pre>
      <item type="name" />
    </pre>
    <post>
      <item type="attribute" name="class" />
    </post>
    <payload>
      <item type="attribute" name="confidence" />
    </payload>
  </token>
  <condition>
    <item type="ancestor" number="1" />
    <item type="ancestorName" condition="entities" />
  </condition>
</mapping>
```

<a name="relationAnnotation"></a>**Relation annotations**

Elements within a [relation](index_formats_folia.html#relation) can be defined as *relationAnnotation*.

```xml
<mapping type="relationAnnotation" name="lang">
  <token type="string" offset="false" realoffset="false" parent="false">
    <pre>
      <item type="name" />
    </pre>
    <post>
      <item type="attribute" name="class" />
    </post>
  </token>
  <condition>
    <item type="attribute" name="href" />
  </condition>
</mapping>
```


<a name="reference"></a>**References**

Elements may be defined as *reference* to a word, for example *wref* elements referring in the *ref* attribute to the *id* of words. 

```xml
<references>
  <reference name="wref" ref="id" />
</references>
```


