package mtas.parser.function.util;

import mtas.parser.function.ParseException;

/**
 * The Class MtasFunctionParserItem.
 */
public class MtasFunctionParserItem {

  /** The type. */
  private String type = null;
  
  /** The id. */
  private Integer id = null;
  
  /** The value long. */
  private Long valueLong = null;
  
  /** The value double. */
  private Double valueDouble = null;
  
  /** The parser. */
  private MtasFunctionParserFunction parser = null;
  
  /** The Constant TYPE_CONSTANT_LONG. */
  public final static String TYPE_CONSTANT_LONG = "constantLong";
  
  /** The Constant TYPE_CONSTANT_DOUBLE. */
  public final static String TYPE_CONSTANT_DOUBLE = "constantDouble";
  
  /** The Constant TYPE_PARSER_LONG. */
  public final static String TYPE_PARSER_LONG = "parserLong";
  
  /** The Constant TYPE_PARSER_DOUBLE. */
  public final static String TYPE_PARSER_DOUBLE = "parserDouble";
  
  /** The Constant TYPE_ARGUMENT. */
  public final static String TYPE_ARGUMENT = "argument";
  
  /** The Constant TYPE_N. */
  public final static String TYPE_N = "n";
  
  /**
   * Instantiates a new mtas function parser item.
   *
   * @param t the t
   * @throws ParseException the parse exception
   */
  public MtasFunctionParserItem(String t) throws ParseException {
    if(t.equals(TYPE_N)) {
      type = t;
    } else {
      throw new ParseException("unknown type "+t);
    }
  }
  
  /**
   * Instantiates a new mtas function parser item.
   *
   * @param t the t
   * @param i the i
   * @throws ParseException the parse exception
   */
  public MtasFunctionParserItem(String t, int i) throws ParseException {
    if(t.equals(TYPE_ARGUMENT)) {
      type = t;
      id = i;
    } else {
      throw new ParseException("unknown type "+t);
    }
  }
  
  /**
   * Instantiates a new mtas function parser item.
   *
   * @param t the t
   * @param l the l
   * @throws ParseException the parse exception
   */
  public MtasFunctionParserItem(String t, long l) throws ParseException {
    if(t.equals(TYPE_CONSTANT_LONG)) {
      type = t;
      valueLong = l;
    } else {
      throw new ParseException("unknown type "+t);
    }
  }
  
  /**
   * Instantiates a new mtas function parser item.
   *
   * @param t the t
   * @param d the d
   * @throws ParseException the parse exception
   */
  public MtasFunctionParserItem(String t, double d) throws ParseException {
    if(t.equals(TYPE_CONSTANT_DOUBLE)) {
      type = t;
      valueDouble = d;
    } else {
      throw new ParseException("unknown type "+t);
    }      
  }
  
  /**
   * Instantiates a new mtas function parser item.
   *
   * @param t the t
   * @param p the p
   * @throws ParseException the parse exception
   */
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
  
  /**
   * Gets the type.
   *
   * @return the type
   */
  public String getType() {
    return type;
  }
  
  /**
   * Gets the id.
   *
   * @return the id
   */
  public int getId() {
    return id.intValue();
  }
  
  /**
   * Gets the value long.
   *
   * @return the value long
   */
  public long getValueLong() {
    return valueLong.longValue();
  }
  
  /**
   * Gets the value double.
   *
   * @return the value double
   */
  public double getValueDouble() {
    return valueDouble.doubleValue();
  }
  
  /**
   * Gets the parser.
   *
   * @return the parser
   */
  public MtasFunctionParserFunction getParser() {
    return parser;
  }
  
}
