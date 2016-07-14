package mtas.parser.cql.util;

import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanMatchAllQuery;
import mtas.search.spans.MtasSpanAndQuery;
import mtas.search.spans.MtasSpanOrQuery;

import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;

/**
 * The Class MtasCQLParserWordFullCondition.
 */
public class MtasCQLParserWordFullCondition extends MtasCQLParserBasicSentencePartCondition {

  /** The word condition. */
  private MtasCQLParserWordCondition wordCondition;  

  /**
   * Instantiates a new mtas cql parser word full condition.
   *
   * @param condition the condition
   */
  public MtasCQLParserWordFullCondition(MtasCQLParserWordCondition condition) {
    minimumOccurence = 1;
    maximumOccurence = 1;    
    optional = false;
    condition.simplify();
    if (condition.not()) {
      not = true;
      condition.swapNot();
    } else {
      not = false;
    }
    wordCondition = condition;
  }

  /**
   * Gets the condition.
   *
   * @return the condition
   */
  public MtasCQLParserWordCondition getCondition() {
    return wordCondition;
  }

  

  /**
   * Checks if is empty.
   *
   * @return true, if is empty
   */
  public boolean isEmpty() {
    return wordCondition.isEmpty();
  }
  
  /* (non-Javadoc)
   * @see mtas.parser.cql.util.MtasCQLParserBasicSentencePartCondition#getQuery()
   */
  @Override
  public SpanQuery getQuery() throws ParseException {
    SpanQuery q = null;
    // match any word (try to avoid...)
    if (wordCondition.isEmpty()) {
      q = new MtasSpanMatchAllQuery(wordCondition.field());
      // only positive queries
    } else if (wordCondition.isSimplePositive()) {
      if (wordCondition.isSingle()) {
        q = wordCondition.getPositiveQuery(0);
      } else {
        if (wordCondition.type().equals(MtasCQLParserWordCondition.TYPE_AND)) {
          q = new MtasSpanAndQuery(wordCondition.getPositiveQuery().toArray(
              new SpanQuery[wordCondition.getPositiveQuery().size()]));
        } else if (wordCondition.type().equals(
            MtasCQLParserWordCondition.TYPE_OR)) {
          q = new MtasSpanOrQuery(wordCondition.getPositiveQuery().toArray(
              new SpanQuery[wordCondition.getPositiveQuery().size()]));
        } else {
          throw new ParseException("unknown type " + wordCondition.type());
        }
      }
      // only negative queries
    } else if (wordCondition.isSimpleNegative()) {
      throw new ParseException("shouldn't be simple negative");
      // both positive and negative queries
    } else {
      if (wordCondition.type().equals(MtasCQLParserWordCondition.TYPE_AND)) {
        SpanQuery qPositive, qNegative;
        if (wordCondition.getPositiveQuery().size() == 1) {
          qPositive = wordCondition.getPositiveQuery(0);
        } else {
          qPositive = new MtasSpanAndQuery(wordCondition.getPositiveQuery()
              .toArray(new SpanQuery[wordCondition.getPositiveQuery().size()]));
        }
        if (wordCondition.getNegativeQuery().size() == 1) {
          qNegative = wordCondition.getNegativeQuery(0);
        } else {
          qNegative = new MtasSpanOrQuery(wordCondition.getNegativeQuery().toArray(
              new SpanQuery[wordCondition.getNegativeQuery().size()]));
        }
        q = new SpanNotQuery(qPositive, qNegative);
      } else if (wordCondition.type()
          .equals(MtasCQLParserWordCondition.TYPE_OR)) {
        SpanQuery qPositive, qNegative;
        if (wordCondition.getPositiveQuery().size() == 1) {
          qPositive = wordCondition.getPositiveQuery(0);
        } else {
          qPositive = new MtasSpanOrQuery(wordCondition.getPositiveQuery().toArray(
              new SpanQuery[wordCondition.getPositiveQuery().size()]));
        }
        if (wordCondition.getNegativeQuery().size() == 1) {
          qNegative = wordCondition.getNegativeQuery(0);
        } else {
          qNegative = new MtasSpanAndQuery(wordCondition.getNegativeQuery()
              .toArray(new SpanQuery[wordCondition.getNegativeQuery().size()]));
        }
        q = new SpanNotQuery(qPositive, qNegative);
      } else {
        throw new ParseException("unknown type " + wordCondition.type());
      }
    }
    if(not) {
      SpanQuery qPositive = new MtasSpanMatchAllQuery(wordCondition.field());
      q = new SpanNotQuery(qPositive, q);
    }
    return q;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object object) {
    if(object==null) 
      return false;
    if(object instanceof MtasCQLParserWordFullCondition) {
      MtasCQLParserWordFullCondition word = (MtasCQLParserWordFullCondition) object;      
      if(!wordCondition.equals(word.wordCondition))
        return false;
      if(not!=word.not)
        return false;
      return true;    
    } else {
      return false;
    }
  }

}
