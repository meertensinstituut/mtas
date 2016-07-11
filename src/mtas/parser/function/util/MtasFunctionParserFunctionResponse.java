package mtas.parser.function.util;

abstract public class MtasFunctionParserFunctionResponse {

  boolean defined;
  double valueDouble;
  int valueInt;
  
  protected MtasFunctionParserFunctionResponse(boolean s) {
    defined = s;
  }
  
  @Override
  abstract public boolean equals(Object obj);
  
}
