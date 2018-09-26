package mtas.analysis.parser;

import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasConfiguration;

final public class MtasChatParser extends MtasXMLParser {
  public MtasChatParser(MtasConfiguration config) {
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
