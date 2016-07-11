package mtas.parser.function.util;

import mtas.parser.function.ParseException;

public class MtasFunctionParserItem {

  private String type = null;
  private Integer id = null;
  private Long valueLong = null;
  private Double valueDouble = null;
  private MtasFunctionParserFunction parser = null;
  
  public final static String TYPE_CONSTANT_LONG = "constantLong";
  public final static String TYPE_CONSTANT_DOUBLE = "constantDouble";
  public final static String TYPE_PARSER_LONG = "parserLong";
  public final static String TYPE_PARSER_DOUBLE = "parserDouble";
  public final static String TYPE_ARGUMENT = "argument";
  public final static String TYPE_N = "n";
  
  public MtasFunctionParserItem(String t) throws ParseException {
    if(t.equals(TYPE_N)) {
      type = t;
    } else {
      throw new ParseException("unknown type "+t);
    }
  }
  
  public MtasFunctionParserItem(String t, int i) throws ParseException {
    if(t.equals(TYPE_ARGUMENT)) {
      type = t;
      id = i;
    } else {
      throw new ParseException("unknown type "+t);
    }
  }
  
  public MtasFunctionParserItem(String t, long l) throws ParseException {
    if(t.equals(TYPE_CONSTANT_LONG)) {
      type = t;
      valueLong = l;
    } else {
      throw new ParseException("unknown type "+t);
    }
  }
  
  public MtasFunctionParserItem(String t, double d) throws ParseException {
    if(t.equals(TYPE_CONSTANT_DOUBLE)) {
      type = t;
      valueDouble = d;
    } else {
      throw new ParseException("unknown type "+t);
    }      
  }
  
  public MtasFunctionParserItem(String t, MtasFunctionParserFunction p) throws ParseException {
    if(t.equals(TYPE_PARSER_LONG)) {
      type = t;
      parser = p;
    } else if(t.equals(TYPE_PARSER_DOUBLE)) {
      type = t;
      parser = p;
    } else {
      throw new ParseException("unknown type "+t);
    }
  }
  
  public String getType() {
    return type;
  }
  
  public int getId() {
    return id.intValue();
  }
  
  public long getValueLong() {
    return valueLong.longValue();
  }
  
  public double getValueDouble() {
    return valueDouble.doubleValue();
  }
  
  public MtasFunctionParserFunction getParser() {
    return parser;
  }
  
}
