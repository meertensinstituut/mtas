package mtas.parser.cql.util;

import mtas.parser.cql.ParseException;

public class MtasCQLParserDefaultPrefixCondition
    extends MtasCQLParserWordCondition {

  public MtasCQLParserDefaultPrefixCondition(String field, String prefix,
      String value) throws ParseException {
    super(field, TYPE_AND);
    if (prefix == null) {
      throw new ParseException("no default prefix defined");
    } else {
      addPositiveQuery(new MtasCQLParserWordQuery(field,prefix,value));      
    }
  }

}
