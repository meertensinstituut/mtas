package mtas.analysis.parser;

import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.Configuration;

final public class MtasChatParser extends MtasXMLParser {
  public MtasChatParser(Configuration config) {
    super(config);
  }

  @Override
  protected void initParser() throws MtasConfigException {
    namespaceURI = "http://www.talkbank.org/ns/talkbank";
    namespaceURI_id = null;
    rootTag = "CHAT";
    super.initParser();
  }
}
