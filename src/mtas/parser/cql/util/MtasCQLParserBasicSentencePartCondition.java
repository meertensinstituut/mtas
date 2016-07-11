package mtas.parser.cql.util;

import org.apache.lucene.search.spans.SpanQuery;

import mtas.parser.cql.ParseException;

public abstract class MtasCQLParserBasicSentencePartCondition {
  
  protected int minimumOccurence, maximumOccurence;
  protected boolean optional, not;
  
  public abstract SpanQuery getQuery() throws ParseException;

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
    if(min==0) {
      optional = true;
    }
    minimumOccurence = Math.max(1,min);
    maximumOccurence = max;    
  }
  
  public boolean isOptional() {
    return optional;
  }
  
  public void setOptional(boolean status) {
    optional = status;
  }
  
  @Override
  public String toString() {
    return toString("", "");
  }

  public String toString(String firstIndent, String indent) {
    String text = "";
    text += firstIndent + "PART";
    if(optional) {
      text += " OPTIONAL";
    }
    if ((minimumOccurence > 1) || (minimumOccurence != maximumOccurence)) {
      if (minimumOccurence != maximumOccurence) {
        text += " {" + minimumOccurence + "," + maximumOccurence + "}";
      } else {
        text += " {" + minimumOccurence + "}";
      }
    }
    try {
      text += "\n" + indent + "- Query: " + getQuery().toString(getQuery().getField());
    } catch (ParseException e) {
      text += "\n" + indent + "- Query: " + e.getMessage();
    }
    text += "\n";
    return text;
  }
  
}
