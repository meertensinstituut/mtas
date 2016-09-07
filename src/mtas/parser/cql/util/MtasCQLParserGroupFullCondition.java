package mtas.parser.cql.util;

import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanEndQuery;
import mtas.search.spans.MtasSpanStartQuery;

import org.apache.lucene.search.spans.SpanQuery;

/**
 * The Class MtasCQLParserGroupFullCondition.
 */
public class MtasCQLParserGroupFullCondition
    extends MtasCQLParserBasicSentencePartCondition {

  /** The group full. */
  public static String GROUP_FULL = "full";

  /** The group start. */
  public static String GROUP_START = "start";

  /** The group end. */
  public static String GROUP_END = "end";

  /** The group condition. */
  private MtasCQLParserGroupCondition groupCondition;

  /** The type. */
  private String type;

  /**
   * Instantiates a new mtas cql parser group full condition.
   *
   * @param condition
   *          the condition
   * @param type
   *          the type
   */
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

  /**
   * Gets the condition.
   *
   * @return the condition
   */
  public MtasCQLParserGroupCondition getCondition() {
    return groupCondition;
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.parser.cql.util.MtasCQLParserBasicSentencePartCondition#
   * getMinimumOccurence()
   */
  @Override
  public int getMinimumOccurence() {
    return minimumOccurence;
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.parser.cql.util.MtasCQLParserBasicSentencePartCondition#
   * getMaximumOccurence()
   */
  @Override
  public int getMaximumOccurence() {
    return maximumOccurence;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.parser.cql.util.MtasCQLParserBasicSentencePartCondition#setOccurence(
   * int, int)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.parser.cql.util.MtasCQLParserBasicSentencePartCondition#isOptional()
   */
  @Override
  public boolean isOptional() {
    return optional;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.parser.cql.util.MtasCQLParserBasicSentencePartCondition#setOptional(
   * boolean)
   */
  @Override
  public void setOptional(boolean status) {
    optional = status;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.parser.cql.util.MtasCQLParserBasicSentencePartCondition#getQuery()
   */
  @Override
  public SpanQuery getQuery() throws ParseException {
    if (type.equals(MtasCQLParserGroupFullCondition.GROUP_START)) {
      return new MtasSpanStartQuery(groupCondition.getQuery());
    } else if (type.equals(MtasCQLParserGroupFullCondition.GROUP_END)) {
      return new MtasSpanEndQuery(groupCondition.getQuery());
    } else {
      return groupCondition.getQuery();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object object) {
    if (object == null)
      return false;
    if (object instanceof MtasCQLParserGroupFullCondition) {
      MtasCQLParserGroupFullCondition word = (MtasCQLParserGroupFullCondition) object;
      if (!groupCondition.equals(word.groupCondition))
        return false;
      return true;
    } else {
      return false;
    }
  }

}
