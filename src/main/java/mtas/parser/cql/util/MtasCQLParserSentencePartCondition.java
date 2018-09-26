package mtas.parser.cql.util;

import mtas.parser.cql.ParseException;
import mtas.search.spans.util.MtasSpanQuery;

public class MtasCQLParserSentencePartCondition {
  private MtasCQLParserSentenceCondition firstSentence = null;
  private MtasCQLParserBasicSentenceCondition firstBasicSentence = null;
  private int firstMinimumOccurence;
  private int firstMaximumOccurence;
  private boolean firstOptional;

  MtasCQLParserSentencePartCondition secondSentencePart = null;

  private boolean orOperator = false;
  private MtasCQLParserSentenceCondition fullCondition = null;
  private MtasSpanQuery ignoreClause;
  private Integer maximumIgnoreLength;

  public MtasCQLParserSentencePartCondition(
      MtasCQLParserBasicSentenceCondition bs, MtasSpanQuery ignore,
      Integer maximumIgnoreLength) {
    firstMinimumOccurence = 1;
    firstMaximumOccurence = 1;
    firstOptional = false;
    firstBasicSentence = bs;
    this.ignoreClause = ignore;
    this.maximumIgnoreLength = maximumIgnoreLength;
  }

  public MtasCQLParserSentencePartCondition(MtasCQLParserSentenceCondition s,
      MtasSpanQuery ignore, Integer maximumIgnoreLength) {
    firstMinimumOccurence = 1;
    firstMaximumOccurence = 1;
    firstOptional = false;
    firstSentence = s;
    this.ignoreClause = ignore;
    this.maximumIgnoreLength = maximumIgnoreLength;
  }

  public void setFirstOccurence(int min, int max) throws ParseException {
    if (fullCondition == null) {
      if ((min < 0) || (min > max) || (max < 1)) {
        throw new ParseException("Illegal number {" + min + "," + max + "}");
      }
      if (min == 0) {
        firstOptional = true;
      }
      firstMinimumOccurence = Math.max(1, min);
      firstMaximumOccurence = max;
    } else {
      throw new ParseException("fullCondition already generated");
    }
  }

  public void setFirstOptional(boolean status) throws ParseException {
    if (fullCondition == null) {
      firstOptional = status;
    } else {
      throw new ParseException("fullCondition already generated");
    }
  }

  public void setOr(boolean status) throws ParseException {
    if (fullCondition == null) {
      orOperator = status;
    } else {
      throw new ParseException("fullCondition already generated");
    }
  }

  public void setSecondPart(MtasCQLParserSentencePartCondition sp)
      throws ParseException {
    if (fullCondition == null) {
      secondSentencePart = sp;
    } else {
      throw new ParseException("fullCondition already generated");
    }
  }

  public MtasCQLParserSentenceCondition createFullSentence()
      throws ParseException {
    if (fullCondition == null) {
      if (secondSentencePart == null) {
        if (firstBasicSentence != null) {
          fullCondition = new MtasCQLParserSentenceCondition(firstBasicSentence,
              ignoreClause, maximumIgnoreLength);

        } else {
          fullCondition = firstSentence;
        }
        fullCondition.setOccurence(firstMinimumOccurence,
            firstMaximumOccurence);
        if (firstOptional) {
          fullCondition.setOptional(firstOptional);
        }
        return fullCondition;
      } else {
        if (!orOperator) {
          if (firstBasicSentence != null) {
            firstBasicSentence.setOccurence(firstMinimumOccurence,
                firstMaximumOccurence);
            firstBasicSentence.setOptional(firstOptional);
            fullCondition = new MtasCQLParserSentenceCondition(
                firstBasicSentence, ignoreClause, maximumIgnoreLength);
          } else {
            firstSentence.setOccurence(firstMinimumOccurence,
                firstMaximumOccurence);
            firstSentence.setOptional(firstOptional);
            fullCondition = new MtasCQLParserSentenceCondition(firstSentence,
                ignoreClause, maximumIgnoreLength);
          }
          fullCondition.addSentenceToEndLatestSequence(
              secondSentencePart.createFullSentence());
        } else {
          MtasCQLParserSentenceCondition sentence = secondSentencePart
              .createFullSentence();
          if (firstBasicSentence != null) {
            sentence.addSentenceAsFirstOption(
                new MtasCQLParserSentenceCondition(firstBasicSentence,
                    ignoreClause, maximumIgnoreLength));
          } else {
            sentence.addSentenceAsFirstOption(firstSentence);
          }
          fullCondition = sentence;
        }
        return fullCondition;
      }
    } else {
      return fullCondition;
    }
  }
}
