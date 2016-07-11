package mtas.parser.cql.util;

import org.apache.lucene.search.spans.SpanQuery;

public class MtasCQLParserGroupCondition {

  private SpanQuery condition;
  private String field;

  public MtasCQLParserGroupCondition(String field, SpanQuery condition) {
    this.field = field;
    this.condition = condition;
  }

  public String field() {
    return field;
  }

  public SpanQuery getQuery() {
    return condition;    
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) {
      return false;
    } else if (object instanceof MtasCQLParserGroupCondition) {
      MtasCQLParserGroupCondition condition = (MtasCQLParserGroupCondition) object;
      // basic checks
      if (!field.equals(condition.field)) {
        return false;
      } else {
        if (!this.condition.equals(condition)) {
          return false;
        } 
        return true;
      }
    } else {
      return false;
    }
  }
}
