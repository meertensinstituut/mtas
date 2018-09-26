package mtas.analysis.parser;

import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasConfiguration;

final public class MtasFoliaParser extends MtasXMLParser {
  public MtasFoliaParser(MtasConfiguration config) {
    super(config);
  }

  @Override
  protected void initParser() throws MtasConfigException {
    namespaceURI = "http://ilk.uvt.nl/folia";
    namespaceURI_id = "http://www.w3.org/XML/1998/namespace";
    rootTag = "FoLiA";
    contentTag = "text";
    allowNonContent = true;
    super.initParser();
  }
}
