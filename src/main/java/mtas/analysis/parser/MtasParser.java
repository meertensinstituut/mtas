package mtas.analysis.parser;

import mtas.analysis.token.MtasTokenCollection;
import mtas.analysis.util.Configuration;
import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasParserException;

import java.io.Reader;

abstract public class MtasParser {
  MtasTokenCollection tokenCollection;
  protected Configuration config;
  protected Boolean autorepair = false;
  protected Boolean makeunique = false;
  static final String TOKEN_OFFSET = "offset";
  static final String TOKEN_REALOFFSET = "realoffset";
  static final String TOKEN_PARENT = "parent";

  MtasParser(Configuration config) {
    this.config = config;
  }

  protected void initParser() throws MtasConfigException {
    if (config != null) {
      // find namespaceURI
      for (int i = 0; i < config.numChildren(); i++) {
        Configuration current = config.child(i);
        if (current.getName().equals("autorepair")) {
          autorepair = current.getAttr("value").equals("true");
        }
        if (current.getName().equals("makeunique")) {
          makeunique = current.getAttr("value").equals("true");
        }
      }
    }
  }

  public abstract MtasTokenCollection createTokenCollection(Reader reader)
      throws MtasParserException, MtasConfigException;

  public abstract String printConfig();
}
