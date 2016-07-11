package mtas.parser.cql.util;


import mtas.parser.cql.ParseException;


public class MtasCQLParserSentencePartCondition {

  private MtasCQLParserSentenceCondition firstSentence = null;
  private MtasCQLParserBasicSentenceCondition firstBasicSentence = null;
  private int firstMinimumOccurence, firstMaximumOccurence;
  private boolean firstOptional;
  MtasCQLParserSentencePartCondition secondSentencePart = null;
  private boolean orOperator = false;
  private MtasCQLParserSentenceCondition fullCondition = null;
  
  public MtasCQLParserSentencePartCondition(MtasCQLParserBasicSentenceCondition bs) {
    firstMinimumOccurence = 1;
    firstMaximumOccurence = 1;
    firstOptional = false;
    firstBasicSentence = bs;
  }
  
  public MtasCQLParserSentencePartCondition(MtasCQLParserSentenceCondition s) {
    firstMinimumOccurence = 1;
    firstMaximumOccurence = 1;
    firstOptional = false;
    firstSentence = s;
  }
  
  public int getFirstMinimumOccurence() {
    return firstMinimumOccurence;
  }

  public int getFirstMaximumOccurence() {
    return firstMaximumOccurence;
  }

  public void setFirstOccurence(int min, int max) throws ParseException {
    if(fullCondition==null) {
      if ((min < 0) || (min > max) || (max < 1)) {    
      throw new ParseException("Illegal number {" + min + "," + max + "}");
      }
      if(min==0) {
        firstOptional = true;
      }
      firstMinimumOccurence = Math.max(1,min);
      firstMaximumOccurence = max;    
    } else {
      throw new ParseException("fullCondition already generated");
    }
  }
  
  public boolean isFirstOptional() {
    return firstOptional;
  }
  
  public void setFirstOptional(boolean status) throws ParseException {
    if(fullCondition==null) {
      firstOptional = status;
    } else {
      throw new ParseException("fullCondition already generated");
    }
  }
  
  public void setOr(boolean status) throws ParseException {
    if(fullCondition==null) {
      orOperator = status;
    } else {
      throw new ParseException("fullCondition already generated");
    }  
  }
  
  public void setSecondPart(MtasCQLParserSentencePartCondition sp) throws ParseException {
    if(fullCondition==null) {
      secondSentencePart = sp;   
    } else {
      throw new ParseException("fullCondition already generated");
    }  
  }
  
  public MtasCQLParserSentenceCondition createFullSentence() throws ParseException {    
    if(fullCondition==null) {
      if(secondSentencePart == null) {
        if(firstBasicSentence!=null) {
          fullCondition = new MtasCQLParserSentenceCondition(firstBasicSentence);
          fullCondition.setOccurence(firstMinimumOccurence, firstMaximumOccurence);
          return fullCondition;
        } else {
          fullCondition = firstSentence;
          fullCondition.setOccurence(firstMinimumOccurence, firstMaximumOccurence);
          return fullCondition;
        }
      } else {
        if(!orOperator) {
          if(firstBasicSentence!=null) {
            firstBasicSentence.setOccurence(firstMinimumOccurence, firstMaximumOccurence);
            fullCondition = new MtasCQLParserSentenceCondition(firstBasicSentence);
          } else {
            firstSentence.setOccurence(firstMinimumOccurence, firstMaximumOccurence);
            fullCondition = new MtasCQLParserSentenceCondition(firstSentence);
          }
          fullCondition.addSentenceToEndLatestSequence(secondSentencePart.createFullSentence());          
        } else {
          MtasCQLParserSentenceCondition sentence = secondSentencePart.createFullSentence();
          if(firstBasicSentence!=null) {
            sentence.addSentenceAsFirstOption(new MtasCQLParserSentenceCondition(firstBasicSentence));
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
