package mtas.parser.cql.util;

import java.util.ArrayList;
import java.util.List;

import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanRecurrenceQuery;
import mtas.search.spans.MtasSpanSequenceItem;
import mtas.search.spans.MtasSpanSequenceQuery;

import org.apache.lucene.search.spans.SpanQuery;

/**
 * The Class MtasCQLParserBasicSentenceCondition.
 */
public class MtasCQLParserBasicSentenceCondition {

  /** The part list. */
  private List<MtasCQLParserBasicSentencePartCondition> partList;

  /** The maximum occurence. */
  private int minimumOccurence, maximumOccurence;

  /** The optional parts. */
  private boolean simplified, optional, optionalParts;

  /**
   * Instantiates a new mtas cql parser basic sentence condition.
   */
  public MtasCQLParserBasicSentenceCondition() {
    partList = new ArrayList<MtasCQLParserBasicSentencePartCondition>();
    minimumOccurence = 1;
    maximumOccurence = 1;
    optional = false;
    simplified = false;
  }

  /**
   * Adds the word.
   *
   * @param w
   *          the w
   * @throws ParseException
   *           the parse exception
   */
  public void addWord(MtasCQLParserWordFullCondition w) throws ParseException {
    assert w.getCondition()
        .not() == false : "condition word should be positive in sentence definition";
    if (!simplified) {
      partList.add(w);
    } else {
      throw new ParseException("already simplified");
    }
  }

  /**
   * Adds the group.
   *
   * @param w
   *          the w
   * @throws ParseException
   *           the parse exception
   */
  public void addGroup(MtasCQLParserGroupFullCondition w)
      throws ParseException {
    if (!simplified) {
      partList.add(w);
    } else {
      throw new ParseException("already simplified");
    }
  }

  /**
   * Adds the basic sentence.
   *
   * @param s
   *          the s
   * @throws ParseException
   *           the parse exception
   */
  public void addBasicSentence(MtasCQLParserBasicSentenceCondition s)
      throws ParseException {
    if (!simplified) {
      List<MtasCQLParserBasicSentencePartCondition> newWordList = s
          .getPartList();
      partList.addAll(newWordList);
    } else {
      throw new ParseException("already simplified");
    }
  }

  /**
   * Gets the minimum occurence.
   *
   * @return the minimum occurence
   */
  public int getMinimumOccurence() {
    return minimumOccurence;
  }

  /**
   * Gets the maximum occurence.
   *
   * @return the maximum occurence
   */
  public int getMaximumOccurence() {
    return maximumOccurence;
  }

  /**
   * Sets the occurence.
   *
   * @param min
   *          the min
   * @param max
   *          the max
   * @throws ParseException
   *           the parse exception
   */
  public void setOccurence(int min, int max) throws ParseException {
    if (!simplified) {
      if ((min < 0) || (min > max) || (max < 1)) {
        throw new ParseException("Illegal number {" + min + "," + max + "}");
      }
      if (min == 0) {
        optional = true;
      }
      minimumOccurence = Math.max(1, min);
      maximumOccurence = max;
    } else {
      throw new ParseException("already simplified");
    }
  }

  /**
   * Checks if is optional.
   *
   * @return true, if is optional
   */
  public boolean isOptional() {
    return optional;
  }

  /**
   * Checks for optional parts.
   *
   * @return true, if successful
   * @throws ParseException
   *           the parse exception
   */
  public boolean hasOptionalParts() throws ParseException {
    if (simplified) {
      return optionalParts;
    } else {
      throw new ParseException("can't be called when not simplified");
    }
  }

  /**
   * Sets the optional.
   *
   * @param status
   *          the new optional
   * @throws ParseException
   *           the parse exception
   */
  public void setOptional(boolean status) throws ParseException {
    optional = status;
  }

  /**
   * Simplify.
   *
   * @throws ParseException
   *           the parse exception
   */
  public void simplify() throws ParseException {
    if (!simplified) {
      simplified = true;
      optionalParts = true;
      List<MtasCQLParserBasicSentencePartCondition> newPartList;
      MtasCQLParserBasicSentencePartCondition lastPart = null;
      newPartList = new ArrayList<MtasCQLParserBasicSentencePartCondition>();
      // try and merge equal word conditions
      for (MtasCQLParserBasicSentencePartCondition part : partList) {
        if ((lastPart == null) || !lastPart.equals(part)) {
          lastPart = part;
          newPartList.add(part);
          if (!part.isOptional()) {
            optionalParts = false;
          }
        } else {
          int newMinimumOccurence, newMaximumOccurence;
          if (!lastPart.isOptional() && !part.isOptional()) {
            newMinimumOccurence = lastPart.getMinimumOccurence()
                + part.getMinimumOccurence();
            newMaximumOccurence = lastPart.getMaximumOccurence()
                + part.getMaximumOccurence();
            lastPart.setOccurence(newMinimumOccurence, newMaximumOccurence);
          } else if (!lastPart.isOptional() && part.isOptional()) {
            if (part.getMinimumOccurence() == 1) {
              newMinimumOccurence = lastPart.getMinimumOccurence()
                  + part.getMinimumOccurence() - 1;
              newMaximumOccurence = lastPart.getMaximumOccurence()
                  + part.getMaximumOccurence();
              lastPart.setOccurence(newMinimumOccurence, newMaximumOccurence);
              lastPart.setOptional(false);
              optionalParts = false;
            } else {
              lastPart = part;
              newPartList.add(part);
              if (!part.isOptional()) {
                optionalParts = false;
              }
            }
          } else if (lastPart.isOptional() && !part.isOptional()) {
            if (lastPart.getMinimumOccurence() == 1) {
              newMinimumOccurence = lastPart.getMinimumOccurence()
                  + part.getMinimumOccurence() - 1;
              newMaximumOccurence = lastPart.getMaximumOccurence()
                  + part.getMaximumOccurence();
              lastPart.setOccurence(newMinimumOccurence, newMaximumOccurence);
              lastPart.setOptional(false);
              optionalParts = false;
            } else {
              lastPart = part;
              newPartList.add(part);
              if (!part.isOptional()) {
                optionalParts = false;
              }
            }
          } else {
            if ((lastPart.getMinimumOccurence() == 1)
                && (part.getMinimumOccurence() == 1)) {
              newMinimumOccurence = lastPart.getMinimumOccurence()
                  + part.getMinimumOccurence() - 1;
              newMaximumOccurence = lastPart.getMaximumOccurence()
                  + part.getMaximumOccurence();
              lastPart.setOccurence(newMinimumOccurence, newMaximumOccurence);
              lastPart.setOptional(true);
            } else {
              lastPart = part;
              newPartList.add(part);
              if (!part.isOptional()) {
                optionalParts = false;
              }
            }
          }
        }
      }
      partList = newPartList;
      if (optionalParts) {
        optional = true;
      }
    }
  }

  /**
   * Gets the part list.
   *
   * @return the part list
   */
  public List<MtasCQLParserBasicSentencePartCondition> getPartList() {
    return partList;
  }

  /**
   * Gets the query.
   *
   * @return the query
   * @throws ParseException
   *           the parse exception
   */
  public SpanQuery getQuery() throws ParseException {
    simplify();
    MtasSpanSequenceItem currentQuery = null;
    List<MtasSpanSequenceItem> currentQueryList = null;
    for (MtasCQLParserBasicSentencePartCondition part : partList) {
      // start list
      if (currentQuery != null) {
        currentQueryList = new ArrayList<MtasSpanSequenceItem>();
        currentQueryList.add(currentQuery);
        currentQuery = null;
      }
      if (part.getMaximumOccurence() > 1) {
        SpanQuery q = new MtasSpanRecurrenceQuery(part.getQuery(),
            part.getMinimumOccurence(), part.getMaximumOccurence());
        currentQuery = new MtasSpanSequenceItem(q, part.isOptional());
      } else {
        currentQuery = new MtasSpanSequenceItem(part.getQuery(),
            part.isOptional());
      }
      // add to list, if it exists
      if (currentQueryList != null) {
        currentQueryList.add(currentQuery);
        currentQuery = null;
      }
    }
    if (currentQueryList != null) {
      return new MtasSpanSequenceQuery(currentQueryList);
    } else if (currentQuery.isOptional()) {
      currentQueryList = new ArrayList<MtasSpanSequenceItem>();
      currentQueryList.add(currentQuery);
      currentQuery = null;
      return new MtasSpanSequenceQuery(currentQueryList);
    } else {
      return currentQuery.getQuery();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    String text = "BASIC SENTENCE";
    if (optional) {
      text += " OPTIONAL";
    }
    text += "\n";
    if (simplified) {
      try {
        text += "- Query: " + getQuery().toString(getQuery().getField());
      } catch (ParseException e) {
        text += "- Query: " + e.getMessage();
      }
    } else {
      for (MtasCQLParserBasicSentencePartCondition word : partList) {
        text += word.toString("  - ", "   ");
      }
    }
    return text;
  }

}
