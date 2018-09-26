package mtas.parser.function.util;

import mtas.parser.function.ParseException;

public class MtasFunctionParserItem {
  private String type = null;
  private Integer id = null;
  private Long valueLong = null;
  private Double valueDouble = null;
  private Integer degree = null;
  private MtasFunctionParserFunction parser = null;

  public static final String TYPE_CONSTANT_LONG = "constantLong";
  public static final String TYPE_CONSTANT_DOUBLE = "constantDouble";
  public static final String TYPE_PARSER_LONG = "parserLong";
  public static final String TYPE_PARSER_DOUBLE = "parserDouble";
  public static final String TYPE_ARGUMENT = "argument";
  public static final String TYPE_N = "n";

  public MtasFunctionParserItem(String t) throws ParseException {
    if (t.equals(TYPE_N)) {
      type = t;
      degree = 0;
    } else {
      throw new ParseException("unknown type " + t);
    }
  }

  public MtasFunctionParserItem(String t, int i) throws ParseException {
    if (t.equals(TYPE_ARGUMENT)) {
      type = t;
      id = i;
      degree = 1;
    } else {
      throw new ParseException("unknown type " + t);
    }
  }

  public MtasFunctionParserItem(String t, long l) throws ParseException {
    if (t.equals(TYPE_CONSTANT_LONG)) {
      type = t;
      valueLong = l;
      degree = 0;
    } else {
      throw new ParseException("unknown type " + t);
    }
  }

  public MtasFunctionParserItem(String t, double d) throws ParseException {
    if (t.equals(TYPE_CONSTANT_DOUBLE)) {
      type = t;
      valueDouble = d;
      degree = 0;
    } else {
      throw new ParseException("unknown type " + t);
    }
  }

  public MtasFunctionParserItem(String t, MtasFunctionParserFunction p)
      throws ParseException {
    if (t.equals(TYPE_PARSER_LONG)) {
      type = t;
      parser = p;
      degree = parser.degree;
    } else if (t.equals(TYPE_PARSER_DOUBLE)) {
      type = t;
      parser = p;
      degree = parser.degree;
    } else {
      throw new ParseException("unknown type " + t);
    }
  }

  public String getType() {
    return type;
  }

  public int getId() {
    return id.intValue();
  }

  public Integer getDegree() {
    return degree;
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
