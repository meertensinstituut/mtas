package mtas.solr.schema;

import java.util.Map;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.PreAnalyzedField;

public class MtasPreAnalyzedField extends PreAnalyzedField {

  private static String FOLLOW_INDEX_ANALYZER = "followIndexAnalyzer";
  private static String DEFAULT_CONFIGURATION = "defaultConfiguration";
  private static String CONFIGURATION_FROM_FIELD = "configurationFromField";
  private static String SET_NUMBER_OF_TOKENS = "setNumberOfTokens";
  private static String SET_NUMBER_OF_POSITIONS = "setNumberOfPositions";
  private static String SET_SIZE = "setSize";
  private static String SET_ERROR = "setError";
    
  public String followIndexAnalyzer = null;
  public String defaultConfiguration = null;
  public String configurationFromField = null;
  public String setNumberOfTokens = null;
  public String setNumberOfPositions = null;
  public String setSize = null;
  public String setError = null;
  
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
    if(followIndexAnalyzer==null) {
      throw new RuntimeException("No " + FOLLOW_INDEX_ANALYZER + " for fieldType "+this.getTypeName());      
    } 
    args.remove(FOLLOW_INDEX_ANALYZER);
    args.remove(DEFAULT_CONFIGURATION);
    args.remove(CONFIGURATION_FROM_FIELD);
    args.remove(SET_NUMBER_OF_TOKENS);
    args.remove(SET_NUMBER_OF_POSITIONS);
    args.remove(SET_ERROR);
    args.remove(SET_SIZE);
    super.setArgs(schema, args);
  }

}
