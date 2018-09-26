#TEI

For indexing [ISO-TEI](http://www.tei-c.org/) resources, the *mtas.analysis.parser.MtasTEIParser* extending the abstract *MtasXMLParser* is available; full examples of configuration files are provided on [GitHub](https://github.com/meertensinstituut/mtas/tree/master/conf/parser/nederlab/mtas).

```xml
<!-- START CONFIGURATION MTAS PARSER -->
<parser name="mtas.analysis.parser.MtasTEIParser">
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

The syntax of the parser part in the [configuration file](indexing_configuration.html#configuration) is, besides from the *name* attribute, almost identical to the configuration of the [FoLiA-parser](indexing_formats_folia.html). An additional feature is the definition and use of *variables*, again illustrated and explained with examples.

**Variables**

From occurring elements, variable-mappings may be derived and defined. Just as *references*, these definitions are placed within a *variables*-tag outside the *mappings*-tag within the *parser* configuration section. In the example below the variable-mapping *interval* is defined from each occurring *when*-tag, defining a mapping from the *id* of the *when*-tag to value of the *interval* attribute.

```xml
<variables>
  <variable name="when" value="interval">
    <value>
      <item type="attribute" name="interval" />
    </value>
  </variable>
</variables>
```

This will define for a TEI resource containing

```xml
...
<timeline unit="s">
  <when xml:id="TLI_0"/>
  <when xml:id="TLI_1" interval="0.64" since="#TLI_0"/>
  <when xml:id="TLI_2" interval="9.7" since="#TLI_0"/>
  <when xml:id="TLI_3" interval="10.216" since="#TLI_0"/>
  <when xml:id="TLI_4" interval="13.052" since="#TLI_0"/>
  <when xml:id="TLI_5" interval="16.28" since="#TLI_0"/>
...  
```

a mapping *interval* that will map for example "TLI_3" to "10.216". Now, when defining other elements, for example a word, we can refer to this defined *variable*: 

```xml
<mapping type="word" name="anchor">
  <token type="string" offset="false" realoffset="false" parent="false">
    <pre>
      <item type="name" />
      <item type="string" value=".time" />
    </pre>
    <post>
      <item type="variableFromAttribute" name="interval" value="synch" />
    </post>
  </token>
</mapping>
```

describing the mapping for resource elements like

```xml
<anchor synch="#TLI_3"/>
```

This will define the *postfix* value from the generated token as the value in the defined mapping *interval* for the value defined by the *sync* attribute of the matching *anchor* tag. In the example above, this will generate a token with *prefix* "anchor.time" and *postfix* "10.216".

Furthermore, if for an element in the mapping a *start* and *end* is defined, for example

```xml
<mapping type="groupAnnotation" name="span" start="from" end="to">
...
</mapping>
```
 
the start and end position of the elements referenced in the defined attributes is used for position and offset of the generated tokens. So, if the source contains

```xml
...
<w xml:id="w115">hier</w>
<w xml:id="w116">sehn</w>
<w xml:id="w117">wir</w>
...
```

and

```xml
...
<span from="#w116" to="#w116">sehen</span>
...
```

the tokens generated from the groupAnnotation mapping on the *span*-tag will have the position and offset from the *word*-tag with *id* "w116".

