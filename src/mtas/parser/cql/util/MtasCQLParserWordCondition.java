package mtas.parser.cql.util;

import java.util.ArrayList;
import java.util.List;

import mtas.search.spans.MtasSpanAndQuery;
import mtas.search.spans.MtasSpanOrQuery;

import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;

public class MtasCQLParserWordCondition {

  public static String TYPE_AND = "and";
  public static String TYPE_OR = "or";

  private List<SpanQuery> positiveQueryList, negativeQueryList;
  private List<MtasCQLParserWordCondition> conditionList;

  private boolean simplified;
  private boolean not;
  private String type;
  private String field;

  public MtasCQLParserWordCondition(String field, String type) {
    this.field = field;
    this.type = type;
    not = false;
    simplified = true;
    positiveQueryList = new ArrayList<SpanQuery>();
    negativeQueryList = new ArrayList<SpanQuery>();
    conditionList = new ArrayList<MtasCQLParserWordCondition>();
  }

  public String type() {
    return type;
  }

  public String field() {
    return field;
  }

  public void swapNot() {
    not = not ? false : true;
    simplified = false;
  }

  public boolean not() {
    return not;
  }

  public void addPositiveQuery(SpanQuery q) {
    positiveQueryList.add(q);
  }

  public void addNegativeQuery(SpanQuery q) {
    negativeQueryList.add(q);
  }

  public List<SpanQuery> getPositiveQuery() {
    return positiveQueryList;
  }

  public SpanQuery getPositiveQuery(int index) {
    if ((index >= 0) && (index < positiveQueryList.size())) {
      return positiveQueryList.get(index);
    } else {
      return null;
    }
  }

  public List<SpanQuery> getNegativeQuery() {
    return negativeQueryList;
  }

  public SpanQuery getNegativeQuery(int index) {
    if ((index >= 0) && (index < negativeQueryList.size())) {
      return negativeQueryList.get(index);
    } else {
      return null;
    }
  }

  public void addCondition(MtasCQLParserWordCondition c) {
    conditionList.add(c);
    simplified = false;
  }

  public boolean isSingle() {
    // assume simplified
    if ((positiveQueryList.size() == 1) && (negativeQueryList.size() == 0)) {
      return true;
    } else if ((positiveQueryList.size() == 0)
        && (negativeQueryList.size() == 1)) {
      return true;
    }
    return false;
  }

  public boolean isSimplePositive() {
    // assume simplified
    if ((positiveQueryList.size() > 0) && (negativeQueryList.size() == 0)) {
      return true;
    }
    return false;
  }

  public boolean isSimpleNegative() {
    // assume simplified
    if ((negativeQueryList.size() > 0) && (positiveQueryList.size() == 0)) {
      return true;
    }
    return false;
  }

  public boolean isEmpty() {
    if ((positiveQueryList.size() == 0) && (negativeQueryList.size() == 0)
        && (conditionList.size() == 0)) {
      return true;
    }
    return false;
  }

  public void swapType() {
    if (type.equals(TYPE_AND)) {
      type = TYPE_OR;
    } else if (type.equals(TYPE_OR)) {
      type = TYPE_AND;
    } else {
      throw new Error("unknown type");
    }
    swapNot();
    List<SpanQuery> queryList = positiveQueryList;
    positiveQueryList = negativeQueryList;
    negativeQueryList = queryList;
    for (MtasCQLParserWordCondition c : conditionList) {
      c.swapNot();
    }
    simplified = false;
  }

  public Boolean simplified() {
    return simplified;
  }

  public void simplify() {
    if (!simplified) {
      if (conditionList.size() > 0) {
        for (MtasCQLParserWordCondition c : conditionList) {
          c.simplify();
          // A & B & ( C & !D )
          if (c.type().equals(type) && !c.not()) {
            positiveQueryList.addAll(c.positiveQueryList);
            negativeQueryList.addAll(c.negativeQueryList);
            // A & B & !( C | !D )
          } else if (!c.type().equals(type) && c.not()) {
            positiveQueryList.addAll(c.negativeQueryList);
            negativeQueryList.addAll(c.positiveQueryList);
            // A & B & ( C )
          } else if (c.isSingle() && !c.not()) {
            positiveQueryList.addAll(c.positiveQueryList);
            negativeQueryList.addAll(c.negativeQueryList);
            // A & B & !( C )
          } else if (c.isSingle() && c.not()) {
            positiveQueryList.addAll(c.negativeQueryList);
            negativeQueryList.addAll(c.positiveQueryList);
          } else if (c.isSimplePositive()) {
            // A | B | ( C & D )
            if (c.type().equals(TYPE_AND)) {
              SpanQuery q = new MtasSpanAndQuery(
                  c.positiveQueryList.toArray(new SpanQuery[c.positiveQueryList
                      .size()]));
              if (c.not()) {
                negativeQueryList.add(q);
              } else {
                positiveQueryList.add(q);
              }
              // A & B & ( C | D )
            } else {
              SpanQuery q = new MtasSpanOrQuery(
                  c.positiveQueryList.toArray(new SpanQuery[c.positiveQueryList
                      .size()]));
              if (c.not()) {
                negativeQueryList.add(q);
              } else {
                positiveQueryList.add(q);
              }
            }
          } else if (c.isSimpleNegative()) {
            // A | B | ( !C | !D )
            if (c.type().equals(TYPE_OR)) {
              SpanQuery q = new MtasSpanAndQuery(
                  c.negativeQueryList.toArray(new SpanQuery[c.negativeQueryList
                      .size()]));
              if (c.not()) {
                positiveQueryList.add(q);
              } else {
                negativeQueryList.add(q);
              }
              // A | B | ( !C & !D )
            } else {
              SpanQuery q = new MtasSpanOrQuery(
                  c.negativeQueryList.toArray(new SpanQuery[c.negativeQueryList
                      .size()]));
              if (c.not()) {
                positiveQueryList.add(q);
              } else {
                negativeQueryList.add(q);
              }
            }
          } else {
            // swap if necessary
            if (this.isSimplePositive() && c.not()) {
              c.swapType();
            } else if (this.isSimpleNegative() && !c.not()) {
              c.swapType();
            }
            // A | B | ( C & !D )
            if (c.type().equals(TYPE_AND)) {
              SpanQuery positiveQuery = new MtasSpanAndQuery(
                  c.positiveQueryList.toArray(new SpanQuery[c.positiveQueryList
                      .size()]));
              SpanQuery negativeQuery = new MtasSpanAndQuery(
                  c.negativeQueryList.toArray(new SpanQuery[c.negativeQueryList
                      .size()]));
              SpanQuery q = new SpanNotQuery(positiveQuery, negativeQuery);
              if (c.not()) {
                negativeQueryList.add(q);
              } else {
                positiveQueryList.add(q);
              }
              // A & B & ( C | !D )
            } else {
              SpanQuery positiveQuery = new MtasSpanOrQuery(
                  c.positiveQueryList.toArray(new SpanQuery[c.positiveQueryList
                      .size()]));
              SpanQuery negativeQuery = new MtasSpanOrQuery(
                  c.negativeQueryList.toArray(new SpanQuery[c.negativeQueryList
                      .size()]));
              SpanQuery q = new SpanNotQuery(positiveQuery, negativeQuery);
              if (c.not()) {
                negativeQueryList.add(q);
              } else {
                positiveQueryList.add(q);
              }
            }
          }
        }
        conditionList.clear();
      }
      if (isSimpleNegative()) {
        swapType();
      }
      simplified = true;
    }
  }

  @Override
  public String toString() {
    return toString("", "");
  }

  public String toString(String firstIndent, String indent) {
    String text = "";
    if (isEmpty()) {
      text += firstIndent + "Type: any word";
      text += (not ? " (not)\n" : "\n");
    } else {
      text += firstIndent + "Type: " + type;
      text += (not ? " (not)\n" : "\n");
      if (positiveQueryList.size() > 0) {
        for (SpanQuery q : positiveQueryList) {
          text += indent + "List Positive Subqueries: " + q.toString(field)
              + "\n";
        }
      }
      if (negativeQueryList.size() > 0) {
        for (SpanQuery q : negativeQueryList) {
          text += indent + "List Negative Queries: " + q.toString(field) + "\n";
        }
      }
      if (conditionList.size() > 0) {
        text += indent + "List Conditions\n";
        for (MtasCQLParserWordCondition c : conditionList) {
          text += c.toString(indent + "- ", indent + "  ") + "\n";
        }
      }
    }
    return text;
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) {
      return false;
    } else if (object instanceof MtasCQLParserWordCondition) {
      MtasCQLParserWordCondition condition = (MtasCQLParserWordCondition) object;
      // basic checks
      if (!field.equals(condition.field) || not ^ condition.not
          || !type.equals(condition.type) || isSingle() ^ condition.isSingle()
          || isSimplePositive() ^ condition.isSimplePositive()
          || isSimpleNegative() ^ condition.isSimpleNegative() || isEmpty()
          ^ condition.isEmpty()) {
        return false;
      } else if (isEmpty()) {
        return true;
      } else {
        if (!positiveQueryList.equals(condition.positiveQueryList)) {
          return false;
        } else {
          for(int i=0; i<positiveQueryList.size(); i++) {             
            if(positiveQueryList.get(i) instanceof MtasCQLParserWordQuery) {              
              if(!(condition.positiveQueryList.get(i) instanceof MtasCQLParserWordQuery)) {
                return false;
              } else if(!((MtasCQLParserWordQuery) positiveQueryList.get(i)).equals(condition.positiveQueryList.get(i))) {
                return false;
              } 
            } 
          }
        }
        if (!negativeQueryList.equals(condition.negativeQueryList)) {
          return false;
        } else {
          for(int i=0; i<negativeQueryList.size(); i++) {             
            if(negativeQueryList.get(i) instanceof MtasCQLParserWordQuery) {              
              if(!(condition.negativeQueryList.get(i) instanceof MtasCQLParserWordQuery)) {
                return false;
              } else if(!((MtasCQLParserWordQuery) negativeQueryList.get(i)).equals(condition.negativeQueryList.get(i))) {
                return false;
              } 
            } 
          }
        }
        return true;
      }
    } else {
      return false;
    }
  }
}
