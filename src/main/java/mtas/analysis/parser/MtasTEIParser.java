package mtas.analysis.parser;

import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasConfiguration;

final public class MtasTEIParser extends MtasXMLParser {
  public MtasTEIParser(MtasConfiguration config) {
    super(config);
  }

  @Override
  protected void initParser() throws MtasConfigException {
    namespaceURI = "http://www.tei-c.org/ns/1.0";
    namespaceURI_id = "http://www.w3.org/XML/1998/namespace";
    rootTag = "TEI";
    contentTag = "text";
    allowNonContent = true;
    super.initParser();
  }
}
