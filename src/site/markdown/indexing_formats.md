#Formats

To configure the mapping from resources to the index structure, several parsers are available for different formats:

* [MtasFoliaParser](indexing_formats_folia.html) : mapping [FoLiA](https://proycon.github.io/folia/) resources
* [MtasTEIParser](indexing_formats_tei.html): mapping [ISO-TEI](http://www.tei-c.org/) resources
* [MtasChatParser](indexing_formats_chat.html): mapping [CHAT transcription format](http://talkbank.org/manuals/CHAT.pdf) resources converted to [XML](http://talkbank.org/software/xsddoc/)
* [MtasSketchParser](indexing_formats_sketch.html): mapping [Sketch Engine](https://www.sketchengine.co.uk/word-sketch-index-format/) resources
* [MtasCRMParser](indexing_formats_crm.html): mapping resources with format Corpus Van Reenen-Mulder/Adelheid

For XML-based formats, these parsers often just slightly extend the abstract MtasXMLParser by defining the correct namespaces and root tags. 

The [configuration file](indexing_configuration.html#configuration) defining the [mapping](indexing_mapping.html) contains general settings and more specific settings defining and configuring the parser. 

The index part may contain general default settings to be applied in the mapping, the content of the parser part is more specific for the defined Mtas parser.

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<mtas>

  <!-- START MTAS INDEX CONFIGURATION -->
  <index>
    <!-- START GENERAL SETTINGS MTAS INDEX PROCESS -->
    <payload index="false" />
    <offset index="false" />
    <realoffset index="false" />
    <parent index="true" />
    <!-- END GENERAL SETTINGS MTAS INDEX PROCESS -->
  </index>
  <!-- END MTAS INDEX CONFIGURATION -->
  
  <!-- START CONFIGURATION MTAS PARSER -->
  <parser name="...">
  ...
    <!-- START MAPPINGS -->
    <mappings>
    ...
    </mapping>
    <!-- END MAPPINGS --->
    ...
  </parser>
  <!-- END CONFIGURATION MTAS PARSER -->
 
</mtas>  
```
