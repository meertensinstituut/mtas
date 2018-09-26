package mtas.parser.cql.util;

import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanEndQuery;
import mtas.search.spans.MtasSpanStartQuery;
import mtas.search.spans.util.MtasSpanQuery;

public class MtasCQLParserGroupFullCondition extends MtasCQLParserBasicSentencePartCondition {
  public static final String GROUP_FULL = "full";
  public static final String GROUP_START = "start";
  public static final String GROUP_END = "end";

  private MtasCQLParserGroupCondition groupCondition;
  private String type;

  public MtasCQLParserGroupFullCondition(MtasCQLParserGroupCondition condition,
      String type) {
    minimumOccurence = 1;
    maximumOccurence = 1;
    optional = false;
    not = false;
    groupCondition = condition;
    if (type.equals(GROUP_START)) {
      this.type = GROUP_START;
    } else if (type.equals(GROUP_END)) {
      this.type = GROUP_END;
    } else {
      this.type = GROUP_FULL;
    }
  }

  public MtasCQLParserGroupCondition getCondition() {
    return groupCondition;
  }

  @Override
  public int getMinimumOccurence() {
    return minimumOccurence;
  }

  @Override
  public int getMaximumOccurence() {
    return maximumOccurence;
  }

  @Override
  public void setOccurence(int min, int max) throws ParseException {
    if ((min < 0) || (min > max) || (max < 1)) {
      throw new ParseException("Illegal number {" + min + "," + max + "}");
    }
    if (min == 0) {
      optional = true;
    }
    minimumOccurence = Math.max(1, min);
    maximumOccurence = max;
  }

  @Override
  public boolean isOptional() {
    return optional;
  }

  @Override
  public void setOptional(boolean status) {
    optional = status;
  }

  @Override
  public MtasSpanQuery getQuery() throws ParseException {
    if (type.equals(MtasCQLParserGroupFullCondition.GROUP_START)) {
      return new MtasSpanStartQuery(groupCondition.getQuery());
    } else if (type.equals(MtasCQLParserGroupFullCondition.GROUP_END)) {
      return new MtasSpanEndQuery(groupCondition.getQuery());
    } else {
      return groupCondition.getQuery();
    }
  }

  @Override
  public boolean equals(Object object) {
    if (object == null)
      return false;
    if (object instanceof MtasCQLParserGroupFullCondition) {
      MtasCQLParserGroupFullCondition word = (MtasCQLParserGroupFullCondition) object;
      return groupCondition.equals(word.groupCondition)
          && type.equals(word.type);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 5) ^ groupCondition.hashCode();
    h = (h * 7) ^ type.hashCode();
    return h;
  }
}
