package mtas.analysis.parser;

import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.Configuration;

final public class MtasElanParser extends MtasXMLParser {
  public MtasElanParser(Configuration config) {
    super(config);
  }

  @Override
  protected void initParser() throws MtasConfigException {
    namespaceURI = null;
    namespaceURI_id = null;
    rootTag = "ELAN";
    contentTag = null;
    allowNonContent = true;
    super.initParser();
  }
}
