package mtas.parser.cql.util;

import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanRecurrenceQuery;
import mtas.search.spans.MtasSpanSequenceItem;
import mtas.search.spans.MtasSpanSequenceQuery;
import mtas.search.spans.util.MtasSpanQuery;

import java.util.ArrayList;
import java.util.List;

public class MtasCQLParserBasicSentenceCondition {
  private List<MtasCQLParserBasicSentencePartCondition> partList;
  private int minimumOccurence;
  private int maximumOccurence;
  private boolean simplified;
  private boolean optional;
  private MtasSpanQuery ignoreClause;
  private Integer maximumIgnoreLength;

  public MtasCQLParserBasicSentenceCondition(MtasSpanQuery ignore,
      Integer maximumIgnoreLength) {
    partList = new ArrayList<MtasCQLParserBasicSentencePartCondition>();
    minimumOccurence = 1;
    maximumOccurence = 1;
    optional = false;
    simplified = false;
    this.ignoreClause = ignore;
    this.maximumIgnoreLength = maximumIgnoreLength;
  }

  public void addWord(MtasCQLParserWordFullCondition w) throws ParseException {
    assert w.getCondition()
        .not() == false : "condition word should be positive in sentence definition";
    if (!simplified) {
      partList.add(w);
    } else {
      throw new ParseException("already simplified");
    }
  }

  public void addGroup(MtasCQLParserGroupFullCondition g)
      throws ParseException {
    if (!simplified) {
      partList.add(g);
    } else {
      throw new ParseException("already simplified");
    }
  }

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

  public int getMinimumOccurence() {
    return minimumOccurence;
  }

  public int getMaximumOccurence() {
    return maximumOccurence;
  }

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

  public boolean isOptional() {
    return optional;
  }

  public void setOptional(boolean status) throws ParseException {
    optional = status;
  }

  public void simplify() throws ParseException {
    if (!simplified) {
      simplified = true;
      boolean optionalParts = true;
      List<MtasCQLParserBasicSentencePartCondition> newPartList;
      MtasCQLParserBasicSentencePartCondition lastPart = null;
      newPartList = new ArrayList<MtasCQLParserBasicSentencePartCondition>();
      // try and merge equal basicSentencePart (word/group) conditions
      for (MtasCQLParserBasicSentencePartCondition part : partList) {
        if ((lastPart == null) || !lastPart.equals(part)) {
          lastPart = part;
          newPartList.add(part);
          if (!part.isOptional()) {
            optionalParts = false;
          }
        } else {
          int newMinimumOccurence;
          int newMaximumOccurence;
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
              optionalParts = false;
            }
          } else {
            if ((lastPart.getMinimumOccurence() == 1)
                && (part.getMinimumOccurence() == 1)) {
              newMinimumOccurence = 1;
              newMaximumOccurence = lastPart.getMaximumOccurence()
                  + part.getMaximumOccurence();
              lastPart.setOccurence(newMinimumOccurence, newMaximumOccurence);
              lastPart.setOptional(true);
            } else {
              lastPart = part;
              newPartList.add(part);
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

  private List<MtasCQLParserBasicSentencePartCondition> getPartList() {
    return partList;
  }

  public MtasSpanQuery getQuery() throws ParseException {
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
        MtasSpanQuery q = new MtasSpanRecurrenceQuery(part.getQuery(),
            part.getMinimumOccurence(), part.getMaximumOccurence(),
            ignoreClause, maximumIgnoreLength);
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
      return new MtasSpanSequenceQuery(currentQueryList, ignoreClause,
          maximumIgnoreLength);
    } else if (currentQuery.isOptional()) {
      currentQueryList = new ArrayList<MtasSpanSequenceItem>();
      currentQueryList.add(currentQuery);
      currentQuery = null;
      return new MtasSpanSequenceQuery(currentQueryList, ignoreClause,
          maximumIgnoreLength);
    } else {
      return currentQuery.getQuery();
    }
  }

  @Override
  public String toString() {
    StringBuilder text = new StringBuilder("BASIC SENTENCE");
    if (optional) {
      text.append(" OPTIONAL");
    }
    text.append("\n");
    if (simplified) {
      try {
        text.append("- Query: " + getQuery().toString(getQuery().getField()));
      } catch (ParseException e) {
        text.append("- Query: " + e.getMessage());
      }
    } else {
      for (MtasCQLParserBasicSentencePartCondition word : partList) {
        text.append(word.toString("  - ", "   "));
      }
    }
    return text.toString();
  }
}
