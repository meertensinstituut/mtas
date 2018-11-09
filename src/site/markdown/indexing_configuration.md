# Configuration

On adding a resource to the index, besides choosing the correct parser for the provided type of resource, also the exact [mapping](indexing_mapping.html) of the resource to the index has to be specified. 

**Mapping**

For resources containing the tokenisation and annotations, like the xml sample below, several parsers are available.

<a name="resource"></a>

```xml
...
  <text xml:id="WR-P-E-J-0000000001.text">
      <div xml:id="WR-P-E-J-0000000001.div0.1" class="chapter">
        <head xml:id="WR-P-E-J-0000000001.head.1">
          <s xml:id="WR-P-E-J-0000000001.head.1.s.1">
            <w xml:id="WR-P-E-J-0000000001.head.1.s.1.w.1">
              <t>Stemma</t>
              <pos class="N(soort,ev,basis,onz,stan)"/>
              <lemma class="stemma"/>
            </w>
          </s>
        </head>
        <p xml:id="WR-P-E-J-0000000001.p.1" class="firstparagraph">
          <s xml:id="WR-P-E-J-0000000001.p.1.s.1">
            <w xml:id="WR-P-E-J-0000000001.p.1.s.1.w.1">
              <t>Stemma</t>
              <pos class="N(eigen,ev,basis,zijd,stan)" />
              <lemma class="Stemma" />
            </w>
            <w xml:id="WR-P-E-J-0000000001.p.1.s.1.w.2">
              <t>is</t>
              <pos class="WW(pv,tgw,ev)"/>
              <lemma class="zijn"/>
...
```

The mapping configuration not only specifies the parser, but also defines exactly how resources are mapped onto the index structure:

<a name="configuration"></a>

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
  </parser>
  <!-- END CONFIGURATION MTAS PARSER -->
</mtas>
```

Several parsers are available supporting multiple [formats](indexing_formats.html), and for each of these parsers specific mapping configurations can be set.

**Multiple configurations**

Within one index, multiple configurations can be used to map resources to the Mtas index structure. The file describing the configuration to be used is of the form

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<mtas>
  <configurations type="mtas.analysis.util.MtasTokenizerFactory">
    <configuration name="folia" file="folia.xml" />
    <configuration name="tei" file="tei.xml" />
  </configurations>
</mtas>
```

Configurations for *tokenizer* and *charFilter* are provided, depending on the type of document. With the charFilter, retrieval of documents from file or url can be configured. The type is usually derived from one of the other metadata fields in the document. In the [installation instructions for Solr](installation_solr.html) an example is included on how to set these configurations in the `schema.xml`. 

