package mtas.parser.cql.util;

import mtas.parser.cql.ParseException;

/**
 * The Class MtasCQLParserSentencePartCondition.
 */
public class MtasCQLParserSentencePartCondition {

  /** The first sentence. */
  private MtasCQLParserSentenceCondition firstSentence = null;

  /** The first basic sentence. */
  private MtasCQLParserBasicSentenceCondition firstBasicSentence = null;

  /** The first maximum occurence. */
  private int firstMinimumOccurence, firstMaximumOccurence;

  /** The first optional. */
  private boolean firstOptional;

  /** The second sentence part. */
  MtasCQLParserSentencePartCondition secondSentencePart = null;

  /** The or operator. */
  private boolean orOperator = false;

  /** The full condition. */
  private MtasCQLParserSentenceCondition fullCondition = null;

  /**
   * Instantiates a new mtas cql parser sentence part condition.
   *
   * @param bs
   *          the bs
   */
  public MtasCQLParserSentencePartCondition(
      MtasCQLParserBasicSentenceCondition bs) {
    firstMinimumOccurence = 1;
    firstMaximumOccurence = 1;
    firstOptional = false;
    firstBasicSentence = bs;
  }

  /**
   * Instantiates a new mtas cql parser sentence part condition.
   *
   * @param s
   *          the s
   */
  public MtasCQLParserSentencePartCondition(MtasCQLParserSentenceCondition s) {
    firstMinimumOccurence = 1;
    firstMaximumOccurence = 1;
    firstOptional = false;
    firstSentence = s;
  }

  /**
   * Gets the first minimum occurence.
   *
   * @return the first minimum occurence
   */
  public int getFirstMinimumOccurence() {
    return firstMinimumOccurence;
  }

  /**
   * Gets the first maximum occurence.
   *
   * @return the first maximum occurence
   */
  public int getFirstMaximumOccurence() {
    return firstMaximumOccurence;
  }

  /**
   * Sets the first occurence.
   *
   * @param min
   *          the min
   * @param max
   *          the max
   * @throws ParseException
   *           the parse exception
   */
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

  /**
   * Checks if is first optional.
   *
   * @return true, if is first optional
   */
  public boolean isFirstOptional() {
    return firstOptional;
  }

  /**
   * Sets the first optional.
   *
   * @param status
   *          the new first optional
   * @throws ParseException
   *           the parse exception
   */
  public void setFirstOptional(boolean status) throws ParseException {
    if (fullCondition == null) {
      firstOptional = status;
    } else {
      throw new ParseException("fullCondition already generated");
    }
  }

  /**
   * Sets the or.
   *
   * @param status
   *          the new or
   * @throws ParseException
   *           the parse exception
   */
  public void setOr(boolean status) throws ParseException {
    if (fullCondition == null) {
      orOperator = status;
    } else {
      throw new ParseException("fullCondition already generated");
    }
  }

  /**
   * Sets the second part.
   *
   * @param sp
   *          the new second part
   * @throws ParseException
   *           the parse exception
   */
  public void setSecondPart(MtasCQLParserSentencePartCondition sp)
      throws ParseException {
    if (fullCondition == null) {
      secondSentencePart = sp;
    } else {
      throw new ParseException("fullCondition already generated");
    }
  }

  /**
   * Creates the full sentence.
   *
   * @return the mtas cql parser sentence condition
   * @throws ParseException
   *           the parse exception
   */
  public MtasCQLParserSentenceCondition createFullSentence()
      throws ParseException {
    if (fullCondition == null) {
      if (secondSentencePart == null) {
        if (firstBasicSentence != null) {
          fullCondition = new MtasCQLParserSentenceCondition(
              firstBasicSentence);
          fullCondition.setOccurence(firstMinimumOccurence,
              firstMaximumOccurence);
          return fullCondition;
        } else {
          fullCondition = firstSentence;
          fullCondition.setOccurence(firstMinimumOccurence,
              firstMaximumOccurence);
          return fullCondition;
        }
      } else {
        if (!orOperator) {
          if (firstBasicSentence != null) {
            firstBasicSentence.setOccurence(firstMinimumOccurence,
                firstMaximumOccurence);
            fullCondition = new MtasCQLParserSentenceCondition(
                firstBasicSentence);
          } else {
            firstSentence.setOccurence(firstMinimumOccurence,
                firstMaximumOccurence);
            fullCondition = new MtasCQLParserSentenceCondition(firstSentence);
          }
          fullCondition.addSentenceToEndLatestSequence(
              secondSentencePart.createFullSentence());
        } else {
          MtasCQLParserSentenceCondition sentence = secondSentencePart
              .createFullSentence();
          if (firstBasicSentence != null) {
            sentence.addSentenceAsFirstOption(
                new MtasCQLParserSentenceCondition(firstBasicSentence));
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
