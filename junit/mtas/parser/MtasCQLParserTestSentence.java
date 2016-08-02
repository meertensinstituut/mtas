package mtas.parser;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWithinQuery;

import mtas.parser.cql.MtasCQLParser;
import mtas.parser.cql.ParseException;
import mtas.parser.cql.util.MtasCQLParserGroupQuery;
import mtas.parser.cql.util.MtasCQLParserWordQuery;
import mtas.search.spans.MtasSpanMatchAllQuery;
import mtas.search.spans.MtasSpanOrQuery;
import mtas.search.spans.MtasSpanRecurrenceQuery;
import mtas.search.spans.MtasSpanSequenceItem;
import mtas.search.spans.MtasSpanSequenceQuery;

public class MtasCQLParserTestSentence {

  @org.junit.Test
  public void test() {
    basicTests();
  }
  
  private void testCQLParse(String field, String defaultPrefix, String cql, SpanQuery q) {    
    MtasCQLParser p = new MtasCQLParser(new BufferedReader(new StringReader(cql)));   
    try {
      System.out.print("CQL parsing:\t"+cql);
      assertEquals(p.parse(field, defaultPrefix) ,q);
      System.out.print("\n");
    } catch (ParseException e) {
      System.out.println("Error CQL parsing:\t"+cql);
      e.printStackTrace();
    }
  }
  
  private void testCQLEquivalent(String field, String defaultPrefix, String cql1, String cql2) {    
    MtasCQLParser p1 = new MtasCQLParser(new BufferedReader(new StringReader(cql1)));   
    MtasCQLParser p2 = new MtasCQLParser(new BufferedReader(new StringReader(cql2)));
    try {
      System.out.print("CQL equivalent:\t"+cql1+" and "+cql2);
      assertEquals(p1.parse(field, defaultPrefix) ,p2.parse(field, defaultPrefix));
      System.out.print("\n");
    } catch (ParseException e) {
      System.out.println("Error CQL equivalent:\t"+cql1+" and "+cql2);
      e.printStackTrace();
    }
  }
  
  private void basicTests() {
    basicTest1();
    basicTest2();
    basicTest3();
    basicTest4();
    basicTest5();
    basicTest6();
    basicTest7();
    basicTest8();
    basicTest9();
    basicTest10();
    basicTest11();
    basicTest12();
    basicTest13();
    basicTest14();
    basicTest15();
    basicTest16();
    basicTest17();
    basicTest18();
    basicTest19();
  }
  
  private void basicTest1() {
    String field = "testveld";
    String cql = "[pos=\"LID\"] [lemma=\"koe\"]";    
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","koe");
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q2, false));
    SpanQuery q = new MtasSpanSequenceQuery(items);
    testCQLParse(field, null, cql, q);    
  }
  
  private void basicTest2() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\"] [] []? [] [lemma=\"koe\"]";
    String cql2 = "[pos=\"LID\"] []{2,3} [lemma=\"koe\"]";
    testCQLEquivalent(field, null, cql1, cql2);    
  }
  
  private void basicTest3() {
    String field = "testveld";
    String cql = "[pos=\"LID\"] | [lemma=\"koe\"]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q = new MtasSpanOrQuery(q1,q2);
    testCQLParse(field, null, cql, q);       
  }
  
  private void basicTest4() {
    String field = "testveld";
    String cql = "[pos=\"LID\"] | ([lemma=\"de\"] [lemma=\"koe\"])";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","de");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"lemma","koe");
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(q2, false));
    items.add(new MtasSpanSequenceItem(q3, false));
    SpanQuery q4 = new MtasSpanSequenceQuery(items);
    SpanQuery q = new MtasSpanOrQuery(q1,q4);
    testCQLParse(field, null, cql, q);       
  }
  
  private void basicTest5() {
    String field = "testveld";
    String cql1 = "([pos=\"LID\"]([pos=\"ADJ\"][lemma=\"koe\"]))";
    String cql2 = "[pos=\"LID\"][pos=\"ADJ\"][lemma=\"koe\"]";
    testCQLEquivalent(field, null, cql1, cql2);    
  }
  
  private void basicTest6() {
    String field = "testveld";
    String cql1 = "([pos=\"LID\"]|[lemma=\"de\"][lemma=\"koe\"])|([pos=\"ADJ\"]|([lemma=\"het\"]([lemma=\"paard\"])))";
    String cql2 = "[pos=\"LID\"]|[lemma=\"de\"][lemma=\"koe\"]|[pos=\"ADJ\"]|[lemma=\"het\"][lemma=\"paard\"]";
    testCQLEquivalent(field, null, cql1, cql2);  
  }
  
  private void basicTest7() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\"] []{0,1} []{3,5} []{2,4}";
    String cql2 = "[pos=\"LID\"] []{5,10}";
    testCQLEquivalent(field, null, cql1, cql2);    
  }
  
  private void basicTest8() {
    String field = "testveld";
    String cql = "[lemma=\"koe\"]([pos=\"N\"]|[pos=\"ADJ\"])";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"pos","N");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"pos","ADJ");
    SpanQuery q4 = new MtasSpanOrQuery(q2,q3);
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q4, false));
    SpanQuery q = new MtasSpanSequenceQuery(items);
    testCQLParse(field, null, cql, q);
  }
  
  private void basicTest9() {
    String field = "testveld";
    String cql = "[lemma=\"koe\"]([pos=\"N\"]|[pos=\"ADJ\"]){2,3}[lemma=\"paard\"]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"pos","N");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"pos","ADJ");
    SpanQuery q4 = new MtasCQLParserWordQuery(field,"lemma","paard");
    SpanQuery q5 = new MtasSpanOrQuery(new MtasSpanRecurrenceQuery(q2,2,3),new MtasSpanRecurrenceQuery(q3,2,3));
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q5, false));
    items.add(new MtasSpanSequenceItem(q4, false));
    SpanQuery q = new MtasSpanSequenceQuery(items);
    testCQLParse(field, null, cql, q);  
  }
  
  private void basicTest10() {
    String field = "testveld";
    String cql = "[pos=\"LID\"]? [pos=\"ADJ\"]{1,3} [lemma=\"koe\"]";    
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"pos","ADJ");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"lemma","koe");
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(q1, true));
    items.add(new MtasSpanSequenceItem(new MtasSpanRecurrenceQuery(q2,1,3), false));
    items.add(new MtasSpanSequenceItem(q3, false));
    SpanQuery q = new MtasSpanSequenceQuery(items);
    testCQLParse(field, null, cql, q);    
  }

  private void basicTest11() {
    String field = "testveld";
    String cql = "<sentence/> containing [lemma=\"koe\"]"; 
    SpanQuery q1 = new MtasCQLParserGroupQuery(field,"sentence");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q = new SpanContainingQuery(q1, q2);
    testCQLParse(field, null, cql, q);    
  }
  
  private void basicTest12() {
    String field = "testveld";
    String cql = "[lemma=\"koe\"] within <sentence/>"; 
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q2 = new MtasCQLParserGroupQuery(field,"sentence");
    SpanQuery q = new SpanWithinQuery(q2, q1);
    testCQLParse(field, null, cql, q);    
  }
  
  private void basicTest13() {
    String field = "testveld";
    String cql = "[lemma=\"koe\"]([t=\"de\"] within <sentence/>)"; 
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"t","de");
    SpanQuery q3 = new MtasCQLParserGroupQuery(field,"sentence");
    SpanQuery q4 = new SpanWithinQuery(q3, q2);
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q4, false));
    SpanQuery q = new MtasSpanSequenceQuery(items);
    testCQLParse(field, null, cql, q);    
  }
  
  private void basicTest14() {
    String field = "testveld";
    String cql = "([t=\"de\"] within <sentence/>)[lemma=\"koe\"]"; 
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"t","de");
    SpanQuery q2 = new MtasCQLParserGroupQuery(field,"sentence");
    SpanQuery q3 = new SpanWithinQuery(q2, q1);
    SpanQuery q4 = new MtasCQLParserWordQuery(field,"lemma","koe");
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(q3, false));
    items.add(new MtasSpanSequenceItem(q4, false));
    SpanQuery q = new MtasSpanSequenceQuery(items);
    testCQLParse(field, null, cql, q);    
  }
  
  private void basicTest15() {
    String field = "testveld";
    String cql = "[lemma=\"koe\"](<sentence/> containing [t=\"de\"]) within <sentence/>[lemma=\"paard\"]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q2 = new MtasCQLParserGroupQuery(field,"sentence");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"t","de");
    SpanQuery q4 = new SpanContainingQuery(q2, q3);
    SpanQuery q5 = new MtasCQLParserGroupQuery(field,"sentence");
    SpanQuery q6 = new MtasCQLParserWordQuery(field,"lemma","paard");    
    List<MtasSpanSequenceItem> items1 = new ArrayList<MtasSpanSequenceItem>();
    items1.add(new MtasSpanSequenceItem(q5, false));
    items1.add(new MtasSpanSequenceItem(q6, false));
    SpanQuery q7 = new MtasSpanSequenceQuery(items1);
    SpanQuery q8 = new SpanWithinQuery(q7, q4);
    List<MtasSpanSequenceItem> items2 = new ArrayList<MtasSpanSequenceItem>();
    items2.add(new MtasSpanSequenceItem(q1, false));
    items2.add(new MtasSpanSequenceItem(q8, false));
    SpanQuery q = new MtasSpanSequenceQuery(items2);    
    testCQLParse(field, null, cql, q);    
  }
  
  private void basicTest16() {
    String field = "testveld";
    String cql = "(<entity=\"loc\"/> within (<s/> containing [t_lc=\"amsterdam\"])) !containing ([t_lc=\"amsterdam\"])"; 
    SpanQuery q1 = new MtasCQLParserGroupQuery(field,"entity","loc");
    SpanQuery q2 = new MtasCQLParserGroupQuery(field,"s");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"t_lc","amsterdam");
    SpanQuery q4 = new SpanContainingQuery(q2, q3);
    SpanQuery q5 = new SpanWithinQuery(q4, q1);
    SpanQuery q = new SpanNotQuery(q5,new SpanContainingQuery(q5, q3));
    testCQLParse(field, null, cql, q);    
  }
  
  private void basicTest17() {
    String field = "testveld";
    String cql = "[]<entity=\"loc\"/>{1,2}[]"; 
    SpanQuery q1 = new MtasCQLParserGroupQuery(field,"entity","loc");
    SpanQuery q2 = new MtasSpanRecurrenceQuery(q1,1,2);
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(new MtasSpanMatchAllQuery(field), false));
    items.add(new MtasSpanSequenceItem(q2, false));
    items.add(new MtasSpanSequenceItem(new MtasSpanMatchAllQuery(field), false));
    SpanQuery q = new MtasSpanSequenceQuery(items);
    testCQLParse(field, null, cql, q);    
  }
  
  private void basicTest18() {
    String field = "testveld";
    String cql = "\"de\" [pos=\"N\"]"; 
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"t_lc","de");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"pos","N");
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q2, false));
    SpanQuery q = new MtasSpanSequenceQuery(items);
    testCQLParse(field, "t_lc", cql, q);    
  }
  
  private void basicTest19() {
    String field = "testveld";
    String cql = "([]<entity=\"loc\"/>{1,2}[]){3,4}"; 
    SpanQuery q1 = new MtasCQLParserGroupQuery(field,"entity","loc");
    SpanQuery q2 = new MtasSpanRecurrenceQuery(q1,1,2);
    List<MtasSpanSequenceItem> items = new ArrayList<MtasSpanSequenceItem>();
    items.add(new MtasSpanSequenceItem(new MtasSpanMatchAllQuery(field), false));
    items.add(new MtasSpanSequenceItem(q2, false));
    items.add(new MtasSpanSequenceItem(new MtasSpanMatchAllQuery(field), false));
    SpanQuery q3 = new MtasSpanSequenceQuery(items);
    SpanQuery q = new MtasSpanRecurrenceQuery(q3,3,4); 
    testCQLParse(field, null, cql, q);    
  }
  
}
