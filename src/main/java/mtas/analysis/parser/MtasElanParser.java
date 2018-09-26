package mtas.analysis.parser;

import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasConfiguration;

final public class MtasElanParser extends MtasXMLParser {
  public MtasElanParser(MtasConfiguration config) {
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
