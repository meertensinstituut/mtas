#CRM

For indexing CRM resources with format Corpus Van Reenen-Mulder/Adelheid, the *mtas.analysis.parser.MtasCRMParser* extending the *MtasBasicParser* is available; full examples of configuration files are provided on [GitHub](https://github.com/meertensinstituut/mtas/tree/master/conf/parser/nederlab/mtas).

```xml
<!-- START CONFIGURATION MTAS PARSER -->
<parser name="mtas.analysis.parser.MtasCRMParser">
...
  <!-- START FILTERS -->
  <filters>
  ...
  </filters>
  <!-- END FILTERS --->
  
  <!-- START MAPPINGS -->
  <mappings>
  ...
  </mappings>
  <!-- END MAPPINGS --->
  ...
</parser>
<!-- END CONFIGURATION MTAS PARSER -->
```

The [configuration file](indexing_configuration.html#configuration) defining the [mapping](indexing_mapping.html) has specific settings for the CRM parser distinguishing several types of elements within the CRM resource.



