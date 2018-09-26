package mtas.solr.schema;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.PreAnalyzedField;

import java.util.Map;

public class MtasPreAnalyzedField extends PreAnalyzedField {
  private final static String FOLLOW_INDEX_ANALYZER = "followIndexAnalyzer";
  private final static String DEFAULT_CONFIGURATION = "defaultConfiguration";
  private final static String CONFIGURATION_FROM_FIELD = "configurationFromField";
  private final static String SET_NUMBER_OF_TOKENS = "setNumberOfTokens";
  private final static String SET_NUMBER_OF_POSITIONS = "setNumberOfPositions";
  private final static String SET_SIZE = "setSize";
  private final static String SET_ERROR = "setError";
  private final static String SET_PREFIX = "setPrefix";
  private final static String SET_PREFIX_NUMBERS = "setPrefixNumbers";

  public String followIndexAnalyzer = null;
  public String defaultConfiguration = null;
  public String configurationFromField = null;
  public String setNumberOfTokens = null;
  public String setNumberOfPositions = null;
  public String setSize = null;
  public String setError = null;
  public String setPrefix = null;
  public String setPrefixNumbers = null;

  @Override
  public void init(IndexSchema schema, Map<String, String> args) {
    args.put(PARSER_IMPL, MtasPreAnalyzedParser.class.getName());
    super.init(schema, args);
  }

  @Override
  protected void setArgs(IndexSchema schema, Map<String, String> args) {
    followIndexAnalyzer = args.get(FOLLOW_INDEX_ANALYZER);
    defaultConfiguration = args.get(DEFAULT_CONFIGURATION);
    configurationFromField = args.get(CONFIGURATION_FROM_FIELD);
    setNumberOfTokens = args.get(SET_NUMBER_OF_TOKENS);
    setNumberOfPositions = args.get(SET_NUMBER_OF_POSITIONS);
    setSize = args.get(SET_SIZE);
    setError = args.get(SET_ERROR);
    setPrefix = args.get(SET_PREFIX);
    setPrefixNumbers = args.get(SET_PREFIX_NUMBERS);
    if (followIndexAnalyzer == null) {
      throw new RuntimeException("No " + FOLLOW_INDEX_ANALYZER
          + " for fieldType " + this.getTypeName());
    }
    args.remove(FOLLOW_INDEX_ANALYZER);
    args.remove(DEFAULT_CONFIGURATION);
    args.remove(CONFIGURATION_FROM_FIELD);
    args.remove(SET_NUMBER_OF_TOKENS);
    args.remove(SET_NUMBER_OF_POSITIONS);
    args.remove(SET_SIZE);
    args.remove(SET_ERROR);
    args.remove(SET_PREFIX);
    args.remove(SET_PREFIX_NUMBERS);
    super.setArgs(schema, args);
  }
}
