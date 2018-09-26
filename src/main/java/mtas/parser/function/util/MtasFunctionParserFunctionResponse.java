package mtas.parser.function.util;

abstract public class MtasFunctionParserFunctionResponse {
  boolean defined;

  protected MtasFunctionParserFunctionResponse(boolean s) {
    defined = s;
  }

  @Override
  abstract public boolean equals(Object obj);
}
