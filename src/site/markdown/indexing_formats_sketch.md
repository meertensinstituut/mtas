#Sketch Engine
For indexing [Sketch Engine](https://www.sketchengine.co.uk/word-sketch-index-format/) resources, the *mtas.analysis.parser.MtasSketchParser* extending the *MtasBasicParser* is available; full examples of configuration files are provided on [GitHub](https://github.com/meertensinstituut/mtas/tree/master/conf/parser/nederlab/mtas).

```xml
<!-- START CONFIGURATION MTAS PARSER -->
<parser name="mtas.analysis.parser.MtasSketchParser">
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

The [configuration file](indexing_configuration.html#configuration) defining the [mapping](indexing_mapping.html) has some specific settings for the Sketch parser distinguishing several types of elements within the XML-based Sketch resource: 

* [words](indexing_formats_sketch.html#word) : the basic tokenisation layer
* [wordAnnotations](indexing_formats_sketch.html#wordAnnotation) : annotations occurring within a word
* [groups](indexing_formats_sketch.html#group) : containing one or multiple words

All these elements are defined inside the *mappings* part of the configuration file. The use and meaning of the different elements is illustrated and explained by some examples. 

 
<a name="word"></a>**Words**

All rows not consisting of a start or end tag in the Sketch resource are supposed to be a set of tab-separated values. Such a row is potentially to be interpreted as *word* with each value an associated *wordAnnotation*. In the parser configuration, conditions can be put on which potential items in the Sketch resource should really be interpreted as a *word*: 

```xml
<mapping type="word">
  <condition>
    <item type="ancestorGroupName" not="true" condition="field" />
  </condition>
</mapping>
```

The example above excludes potential words that are contained within a *field* tag.

<a name="word"></a>**Word annotations**

Each value in the set of tab separated values from a word is a potential *wordAnnotation*. A mapping on such a *wordAnnotation* can be defined by referring to the position of the value in the *word* definition.

```xml
<mapping type="wordAnnotation" name="0">
  <token type="string" offset="false" parent="false">
    <pre>
      <item type="string" value="t" />
    </pre>
    <post>
      <item type="text" />
    </post>
  </token>
</mapping>  
```

The example above will add a token based on the first *wordAnntotation* value from each *word*.


<a name="group"></a>**Groups**

Rows containing start and end tags in the Sketch resource define potential groups. These groups must contain words, and mappings can be configured by referring to their name.

```xml
<mapping type="group" name="s">
  <token type="string" offset="false">
    <pre>
      <item type="name" />
    </pre>
    <post>
      <item type="attribute" name="class" />
    </post>
  </token>        
</mapping>
```

