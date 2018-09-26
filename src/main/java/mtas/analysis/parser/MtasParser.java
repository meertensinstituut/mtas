package mtas.analysis.parser;

import java.io.Reader;
import mtas.analysis.token.MtasTokenCollection;
import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasConfiguration;
import mtas.analysis.util.MtasParserException;

abstract public class MtasParser {
  protected MtasTokenCollection tokenCollection;
  protected MtasConfiguration config;
  protected Boolean autorepair = false;
  protected Boolean makeunique = false;
  protected static final String TOKEN_OFFSET = "offset";
  protected static final String TOKEN_REALOFFSET = "realoffset";
  protected static final String TOKEN_PARENT = "parent";

  public MtasParser() {
  }

  public MtasParser(MtasConfiguration config) {
    this.config = config;
  }

  protected void initParser() throws MtasConfigException {
    if (config != null) {
      // find namespaceURI
      for (int i = 0; i < config.children.size(); i++) {
        MtasConfiguration current = config.children.get(i);
        if (current.name.equals("autorepair")) {
          autorepair = current.attributes.get("value").equals("true");
        }
        if (current.name.equals("makeunique")) {
          makeunique = current.attributes.get("value").equals("true");
        }
      }
    }
  }

  public abstract MtasTokenCollection createTokenCollection(Reader reader)
      throws MtasParserException, MtasConfigException;

  public abstract String printConfig();
}
