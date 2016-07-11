package mtas.parser.cql.util;

import java.util.ArrayList;
import java.util.List;

import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanOrQuery;
import mtas.search.spans.MtasSpanRecurrenceQuery;
import mtas.search.spans.MtasSpanSequenceItem;
import mtas.search.spans.MtasSpanSequenceQuery;

import org.apache.lucene.search.spans.SpanQuery;

public class MtasCQLParserSentenceCondition {

  // parent list: multiple items for OR
  // child list: sequence
  private List<List<MtasCQLParserSentenceCondition>> sequenceList;
  private List<MtasCQLParserSentenceCondition> sentenceSequence = null;
  private MtasCQLParserSentenceCondition sentenceCurrent = null;
  private MtasCQLParserBasicSentenceCondition basicSentence = null;
  private int minimumOccurence, maximumOccurence;
  private boolean basic, simplified, optional, optionalParts;

  public MtasCQLParserSentenceCondition(MtasCQLParserBasicSentenceCondition s)
      throws ParseException {
    sequenceList = new ArrayList<List<MtasCQLParserSentenceCondition>>();
    basicSentence = s;
    minimumOccurence = 1;
    maximumOccurence = 1;
    simplified = false;
    basic = true;
    optional = false;
  }

  public MtasCQLParserSentenceCondition(MtasCQLParserSentenceCondition sp)
      throws ParseException {
    sequenceList = new ArrayList<List<MtasCQLParserSentenceCondition>>();
    addSentenceToEndLatestSequence(sp);
    minimumOccurence = 1;
    maximumOccurence = 1;
    simplified = false;
    basic = false;
    optional = false;
  }

  public void addBasicSentenceToEndLatestSequence(
      MtasCQLParserBasicSentenceCondition s) throws ParseException {
    if (!simplified) {
      if (isBasic()) {
        if (basicSentence == null) {
          basicSentence = s;
        } else {
          basicSentence.addBasicSentence(s);
        }
      } else {
        sentenceCurrent = new MtasCQLParserSentenceCondition(s);
        sentenceSequence.add(sentenceCurrent);
      }
    } else {
      throw new ParseException("already simplified");
    }
  }

  public void addBasicSentenceAsOption(MtasCQLParserBasicSentenceCondition s)
      throws ParseException {
    if (!simplified) {
      if (isBasic()) {
        if (basicSentence == null) {
          basicSentence = s;
        } else {
          // add previous basic sentence as first option
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = new MtasCQLParserSentenceCondition(basicSentence);
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          basicSentence = null;
          // create new option for current basic sentence
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = new MtasCQLParserSentenceCondition(s);
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          // not basic anymore
          basic = false;
        }
      } else {
        sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
        sentenceCurrent = new MtasCQLParserSentenceCondition(s);
        sentenceSequence.add(sentenceCurrent);
        sequenceList.add(sentenceSequence);
      }
    } else {
      throw new ParseException("already simplified");
    }
  }

  public void addSentenceToStartFirstSequence(MtasCQLParserSentenceCondition s)
      throws ParseException {
    if (!simplified) {
      if (isBasic()) {
        if (basicSentence == null) {
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = s;
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          // not basic anymore
          basic = false;
        } else {
          // add sentence as first item in new sequence
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          // add sentence to first option
          sentenceCurrent = s;
          sentenceSequence.add(sentenceCurrent);
          // add basic sentence as second item
          sentenceCurrent = new MtasCQLParserSentenceCondition(basicSentence);
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          basicSentence = null;
          // not simple anymore
          basic = false;
        }
      } else {
        sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
        sentenceSequence.add(s);
        sentenceSequence.addAll(sequenceList.get(0));
        sequenceList.set(0, sentenceSequence);
        sentenceSequence = sequenceList.get((sequenceList.size() - 1));
        sentenceCurrent = sentenceSequence.get((sentenceSequence.size() - 1));
      }
    } else {
      throw new ParseException("already simplified");
    }
  }

  public void addSentenceToEndLatestSequence(MtasCQLParserSentenceCondition s)
      throws ParseException {
    if (!simplified) {
      if (isBasic()) {
        if (basicSentence == null) {
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = s;
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          // not simple anymore
          basic = false;
        } else {
          // add previous basic sentence as first option
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = new MtasCQLParserSentenceCondition(basicSentence);
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          basicSentence = null;
          // add sentence to first option
          sentenceCurrent = s;
          sentenceSequence.add(sentenceCurrent);
          // not simple anymore
          basic = false;
        }
      } else {
        sentenceCurrent = s;
        if(sentenceSequence==null) {
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();                  
          sequenceList.add(sentenceSequence);
        }  
        sentenceSequence.add(sentenceCurrent);
      }
    } else {
      throw new ParseException("already simplified");
    }
  }

  public void addSentenceAsFirstOption(MtasCQLParserSentenceCondition s)
      throws ParseException {
    if (!simplified) {
      if (isBasic()) {
        if (basicSentence == null) {
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = s;
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          // not simple anymore
          basic = false;
        } else {
          // add sentence as first option
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = s;
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          // add previous basic sentence as new option
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = new MtasCQLParserSentenceCondition(basicSentence);
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          basicSentence = null;
          // not simple anymore
          basic = false;
        }
      } else {
        sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
        sentenceCurrent = s;
        sentenceSequence.add(sentenceCurrent);
        List<List<MtasCQLParserSentenceCondition>> newsequenceList = new ArrayList<List<MtasCQLParserSentenceCondition>>();
        newsequenceList.add(sentenceSequence);
        newsequenceList.addAll(sequenceList);
        sequenceList = newsequenceList;
      }
    } else {
      throw new ParseException("already simplified");
    }
  }

  public void addSentenceAsOption(MtasCQLParserSentenceCondition s)
      throws ParseException {
    if (!simplified) {
      if (isBasic()) {
        if (basicSentence == null) {
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = s;
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          // not simple anymore
          basic = false;
        } else {
          // add previous basic sentence as first option
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = new MtasCQLParserSentenceCondition(basicSentence);
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          basicSentence = null;
          // add sentence as new option
          sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
          sentenceCurrent = s;
          sentenceSequence.add(sentenceCurrent);
          sequenceList.add(sentenceSequence);
          // not simple anymore
          basic = false;
        }
      } else {
        sentenceSequence = new ArrayList<MtasCQLParserSentenceCondition>();
        sentenceCurrent = s;
        sentenceSequence.add(sentenceCurrent);
        sequenceList.add(sentenceSequence);
      }
    } else {
      throw new ParseException("already simplified");
    }
  }

  public boolean isBasic() {
    return basic;
  }

  public boolean isSingle() {
    return basic ? true : ((sequenceList.size() > 1) ? false : true);
  }

  public void simplify() throws ParseException {
    if (!simplified) {
      if (!isBasic()) {
        for (List<MtasCQLParserSentenceCondition> sequence : sequenceList) {
          simplifySequence(sequence);
        }
      }
      //flatten      
      if(sequenceList.size()>1) {
        List<List<MtasCQLParserSentenceCondition>> newSequenceList = new ArrayList<List<MtasCQLParserSentenceCondition>>();
        for (List<MtasCQLParserSentenceCondition> sequence : sequenceList) {
          if(sequence.size()==1) {
            MtasCQLParserSentenceCondition subSentence = sequence.get(0);
            if(subSentence.isBasic()) {
              newSequenceList.add(sequence);
            } else {
              newSequenceList.addAll(subSentence.getsequenceList());
            }
          }
        }
        sequenceList = newSequenceList;
      }
      simplified = true;
    }
  }

  private void simplifySequence(List<MtasCQLParserSentenceCondition> sequence)
      throws ParseException {
    List<MtasCQLParserSentenceCondition> newSequence = new ArrayList<MtasCQLParserSentenceCondition>();
    MtasCQLParserSentenceCondition lastSentence = null;
    for (MtasCQLParserSentenceCondition sentence : sequence) {
      sentence.simplify();
      if (lastSentence == null) {
        lastSentence = sentence;
      } else if (lastSentence.isBasic() && sentence.isBasic()) {
        // if no recurrence or equal queries
        // opt1 optPart1 opt2 optPart2 merge opt optPart
        // ..-.....-.....-........-.....+.....-.....-
        // ..+.....-.....-........-.....-............
        // ..+.....+.....-........-.....+.....-.....-
        // ..-.....-.....+........-.....-............
        // ..+.....-.....+........-.....-............
        // ..+.....+.....+........-.....+.....+.....-
        // ..-.....-.....+........+.....+.....-.....-
        // ..+.....-.....+........+.....+.....+.....-
        // ..+.....+.....+........+.....+.....+.....+
        if ((((lastSentence.getMaximumOccurence() == 1) && (sentence
            .getMaximumOccurence() == 1))
            || lastSentence.getQuery().equals(sentence.getQuery())
            && ((!lastSentence.isOptional() && !sentence.isOptional()))
            || lastSentence.hasOptionalParts() || sentence.hasOptionalParts())) {
          // create new basic sentence
          MtasCQLParserBasicSentenceCondition newBasicSentence = new MtasCQLParserBasicSentenceCondition();
          newBasicSentence.addBasicSentence(lastSentence.basicSentence);
          newBasicSentence.addBasicSentence(sentence.basicSentence);
          // make optional
          if (lastSentence.isOptional() && sentence.isOptional()) {
            newBasicSentence.setOptional(true);
          }
          lastSentence = new MtasCQLParserSentenceCondition(newBasicSentence);
          lastSentence.simplify();
        } else {
          newSequence.add(lastSentence);
          lastSentence = sentence;
        }
      } else if (lastSentence.isBasic()) {
        if (sentence.isSingle()
            && (!sentence.isOptional() || sentence.hasOptionalParts())
            && ((sentence.getMaximumOccurence() == 1) && (lastSentence
                .getMaximumOccurence() == 1))) {
          for (MtasCQLParserSentenceCondition subSentence : sentence
              .getsequenceList().get(0)) {
            newSequence.add(lastSentence);
            lastSentence = subSentence;
          }
        } else {
          newSequence.add(lastSentence);
          lastSentence = sentence;
        }
      } else if (sentence.isBasic()) {
        if (lastSentence.isSingle()
            && (!lastSentence.isOptional() || lastSentence.hasOptionalParts())&& ((sentence.getMaximumOccurence() == 1) && (lastSentence
                .getMaximumOccurence() == 1))) {
          lastSentence.addBasicSentenceToEndLatestSequence(sentence.getBasicSentence());
        } else {
          newSequence.add(lastSentence);
          lastSentence = sentence;
        }
      } else {
        newSequence.add(lastSentence);
        lastSentence = sentence;
      }
    }
    if (lastSentence != null) {
      newSequence.add(lastSentence);
    }
    sequence.clear();
    sequence.addAll(newSequence);
  }

  public List<List<MtasCQLParserSentenceCondition>> getsequenceList() {
    return sequenceList;
  }

  public MtasCQLParserBasicSentenceCondition getBasicSentence() {
    return basicSentence;
  }

  public int getMinimumOccurence() {
    return minimumOccurence;
  }

  public int getMaximumOccurence() {
    return maximumOccurence;
  }

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

  public boolean isOptional() {
    return optional;
  }

  public boolean hasOptionalParts() throws ParseException {
    if (simplified) {
      return optionalParts;
    } else {
      throw new ParseException("can't be called when not simplified");
    }
  }

  public void setOptional(boolean status) {
    optional = status;
  }

  private SpanQuery createQuery(
      List<MtasCQLParserSentenceCondition> sentenceSequence)
      throws ParseException {
    if (sentenceSequence.size() == 1) {
      if(maximumOccurence>1) {
        return new MtasSpanRecurrenceQuery(sentenceSequence.get(0).getQuery(), minimumOccurence, maximumOccurence);
      } else {
        return sentenceSequence.get(0).getQuery();
      }
    } else {
      List<MtasSpanSequenceItem> clauses = new ArrayList<MtasSpanSequenceItem>();
      for (MtasCQLParserSentenceCondition sentence : sentenceSequence) {
        clauses.add(new MtasSpanSequenceItem(sentence.getQuery(),
            sentence.optional));
      }
      if(maximumOccurence>1) {
        return new MtasSpanRecurrenceQuery(new MtasSpanSequenceQuery(clauses), minimumOccurence, maximumOccurence);
      } else {
        return new MtasSpanSequenceQuery(clauses);
      }
    }
  }

  public SpanQuery getQuery() throws ParseException {
    simplify();
    if (isBasic()) {
      if (basicSentence == null) {
        throw new ParseException("no condition");
      } else if (basicSentence.isOptional()) {
        List<MtasSpanSequenceItem> clauses = new ArrayList<MtasSpanSequenceItem>();
        clauses.add(new MtasSpanSequenceItem(basicSentence.getQuery(),
            basicSentence.isOptional()));
        return new MtasSpanSequenceQuery(clauses);
      } else {
        return basicSentence.getQuery();
      }
    } else if (sequenceList.isEmpty()) {
      throw new ParseException("no condition");
    } else if (isSingle()) {
      return createQuery(sequenceList.get(0));
    } else {
      List<SpanQuery> clauses = new ArrayList<SpanQuery>();
      for (List<MtasCQLParserSentenceCondition> sentenceSequence : sequenceList) {
        clauses.add(createQuery(sentenceSequence));
      }
      return new MtasSpanOrQuery(clauses.toArray(new SpanQuery[clauses.size()]));
    }
  }

  @Override
  public String toString() {
    return toString("", "");
  }

  public String toString(String firstIndent, String indent) {
    String text = "";
    if (isBasic()) {
      try {
        text += firstIndent + "BASIC SENTENCE" + (optional ? " OPTIONAL" : "")
            + ": " + basicSentence.getQuery()
            + (basicSentence.isOptional() ? " OPTIONAL" : "") + "\n";
      } catch (ParseException e) {
        text += firstIndent + "BASIC SENTENCE" + (optional ? " OPTIONAL" : "")
            + ": " + e.getMessage() + "\n";
      }
    } else {
      text += firstIndent + "SENTENCE" + (optional ? " OPTIONAL" : "") + "\n";
      if (simplified) {
        try {
          text += indent + "- Query: " + getQuery().toString(getQuery().getField());
        } catch (ParseException e) {
          text += indent + "- Query: " + e.getMessage();
        }
        text += "\n";
      } else {
        for (List<MtasCQLParserSentenceCondition> sentenceSequence : sequenceList) {
          text += indent + "- Sequence :\n";
          for (MtasCQLParserSentenceCondition sentence : sentenceSequence) {
            text += sentence.toString(indent + "  - ", indent + "    ");
          }
        }
        text += "\n";
      }
    }
    return text;
  }

}
