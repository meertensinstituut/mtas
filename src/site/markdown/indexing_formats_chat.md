#CHAT

For indexing [CHAT transcription format](http://talkbank.org/manuals/CHAT.pdf) resources converted to [XML](http://talkbank.org/software/xsddoc/), the *mtas.analysis.parser.MtasChatParser* extending the abstract *MtasXMLParser* is available; full examples of configuration files are provided on [GitHub](https://github.com/meertensinstituut/mtas/tree/master/conf/parser/nederlab/mtas).

```xml
<!-- START CONFIGURATION MTAS PARSER -->
<parser name="mtas.analysis.parser.MtasChatParser">
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

The syntax of the parser part in the [configuration file](indexing_configuration.html#configuration) is, besides from the *name* attribute, almost identical to the configuration of the [FoLiA-parser](indexing_formats_folia.html) and [TEI-parser](indexing_formats_tei.html). 

