package mtas.parser.cql.util;

import mtas.search.spans.util.MtasSpanQuery;

public class MtasCQLParserGroupCondition {
  private MtasSpanQuery condition;
  private String field;

  public MtasCQLParserGroupCondition(String field, MtasSpanQuery condition) {
    this.field = field;
    this.condition = condition;
  }

  public String field() {
    return field;
  }

  public MtasSpanQuery getQuery() {
    return condition;
  }

  @Override
  public boolean equals(Object object) {
    if (object != null && object instanceof MtasCQLParserGroupCondition) {
      MtasCQLParserGroupCondition groupCondition = (MtasCQLParserGroupCondition) object;
      return field.equals(groupCondition.field)
          && condition.equals(groupCondition.condition);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 5) ^ field.hashCode();
    h = (h * 7) ^ condition.hashCode();
    return h;
  }
}
