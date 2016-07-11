package mtas.parser;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.StringReader;

import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;

import mtas.parser.cql.MtasCQLParser;
import mtas.parser.cql.ParseException;
import mtas.parser.cql.util.MtasCQLParserWordPositionQuery;
import mtas.parser.cql.util.MtasCQLParserWordQuery;
import mtas.search.spans.MtasSpanAndQuery;
import mtas.search.spans.MtasSpanOrQuery;

public class MtasCQLParserTestWord {

  @org.junit.Test
  public void test() {
    basicTests();
    basicNotTests();
  }
  
  private void testCQLParse(String field, String cql, SpanQuery q) {    
    MtasCQLParser p = new MtasCQLParser(new BufferedReader(new StringReader(cql)));
    try {
      assertEquals(p.parse(field) ,q);
      System.out.println("Tested CQL parsing:\t"+cql);
    } catch (ParseException e) {
      System.out.println("Error CQL parsing:\t"+cql);
      e.printStackTrace();
    }
  }
  
  private void testCQLEquivalent(String field, String cql1, String cql2) {    
    MtasCQLParser p1 = new MtasCQLParser(new BufferedReader(new StringReader(cql1)));   
    MtasCQLParser p2 = new MtasCQLParser(new BufferedReader(new StringReader(cql2)));   
    try {
      assertEquals(p1.parse(field) ,p2.parse(field));
      System.out.println("Tested CQL equivalent:\t"+cql1+" and "+cql2);
    } catch (ParseException e) {
      System.out.println("Error CQL equivalent:\t"+cql1+" and "+cql2);
      e.printStackTrace();
    }
  }
  
  private void basicNotTests() {
    basicNotTest1();
    basicNotTest2();
    basicNotTest3();
    basicNotTest4();
    basicNotTest5();
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
  }
  
  private void basicNotTest1() {
    String field = "testveld";
    String cql = "[pos=\"LID\" & !lemma=\"de\"]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","de");
    SpanQuery q = new SpanNotQuery(q1,q2);
    testCQLParse(field, cql, q);    
  }
  
  private void basicNotTest2() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\" & (!lemma=\"de\")]";
    String cql2 = "[pos=\"LID\" & !(lemma=\"de\")]";
    testCQLEquivalent(field, cql1, cql2);    
  }
  
  private void basicNotTest3() {
    String field = "testveld";
    String cql = "[pos=\"LID\" & !(lemma=\"de\" | lemma=\"een\")]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","de");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"lemma","een");
    SpanQuery q4 = new MtasSpanOrQuery(new SpanQuery[]{q2,q3});
    SpanQuery q = new SpanNotQuery(q1,q4);
    testCQLParse(field, cql, q);    
  }
  
  private void basicNotTest4() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\" & !(lemma=\"de\" | lemma=\"een\")]";
    String cql2 = "[pos=\"LID\" & (!lemma=\"de\" & !lemma=\"een\")]";
    testCQLEquivalent(field, cql1, cql2);    
  }
  
  private void basicNotTest5() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\" & !(lemma=\"de\" | lemma=\"een\")]";
    String cql2 = "[pos=\"LID\" & !lemma=\"de\" & !lemma=\"een\"]";
    testCQLEquivalent(field, cql1, cql2);      
  }
  
  private void basicTest1() {
    String field = "testveld";
    String cql = "[lemma=\"koe\"]";
    SpanQuery q = new MtasCQLParserWordQuery(field, "lemma", "koe");
    testCQLParse(field, cql, q);    
  }
  
  private void basicTest2() {
    String field = "testveld";
    String cql = "[lemma=\"koe\" & pos=\"N\"]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"pos","N");
    SpanQuery q = new MtasSpanAndQuery(new SpanQuery[]{q1,q2});
    testCQLParse(field, cql, q);    
  }
  
  private void basicTest3() {
    String field = "testveld";
    String cql = "[lemma=\"koe\" | lemma=\"paard\"]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","paard");
    SpanQuery q = new MtasSpanOrQuery(new SpanQuery[]{q1,q2});
    testCQLParse(field, cql, q);    
  }
  
  private void basicTest4() {
    String field = "testveld";
    String cql1 = "[lemma=\"koe\" | lemma=\"paard\"]";
    String cql2 = "[(lemma=\"koe\" | lemma=\"paard\")]";
    testCQLEquivalent(field, cql1, cql2);    
  }
  
  private void basicTest5() {
    String field = "testveld";
    String cql = "[(lemma=\"koe\" | lemma=\"paard\") & pos=\"N\"]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","paard");
    SpanQuery q3 = new MtasSpanOrQuery(new SpanQuery[]{q1,q2});
    SpanQuery q4 = new MtasCQLParserWordQuery(field,"pos","N");
    SpanQuery q = new MtasSpanAndQuery(new SpanQuery[]{q3,q4});
    testCQLParse(field, cql, q);    
  }
  
  private void basicTest6() {
    String field = "testveld";
    String cql = "[pos=\"N\" & (lemma=\"koe\" | lemma=\"paard\")]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"pos","N");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"lemma","paard");
    SpanQuery q4 = new MtasSpanOrQuery(new SpanQuery[]{q2,q3});
    SpanQuery q = new MtasSpanAndQuery(new SpanQuery[]{q1,q4});
    testCQLParse(field, cql, q);    
  }
  
  private void basicTest7() {
    String field = "testveld";
    String cql = "[pos=\"LID\" | (lemma=\"koe\" & pos=\"N\")]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"pos","N");
    SpanQuery q4 = new MtasSpanAndQuery(new SpanQuery[]{q2,q3});
    SpanQuery q = new MtasSpanOrQuery(new SpanQuery[]{q1,q4});
    testCQLParse(field, cql, q);    
  }
  
  private void basicTest8() {
    String field = "testveld";
    String cql = "[(lemma=\"de\" & pos=\"LID\") | (lemma=\"koe\" & pos=\"N\")]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","de");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q4 = new MtasCQLParserWordQuery(field,"pos","N");
    SpanQuery q5 = new MtasSpanAndQuery(new SpanQuery[]{q1,q2});
    SpanQuery q6 = new MtasSpanAndQuery(new SpanQuery[]{q3,q4});
    SpanQuery q = new MtasSpanOrQuery(new SpanQuery[]{q5,q6});
    testCQLParse(field, cql, q);    
  }
  
  private void basicTest9() {
    String field = "testveld";
    String cql = "[((lemma=\"de\"|lemma=\"het\") & pos=\"LID\") | (lemma=\"koe\" & pos=\"N\")]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","de");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","het");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q4 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q5 = new MtasCQLParserWordQuery(field,"pos","N");
    SpanQuery q6 = new MtasSpanOrQuery(new SpanQuery[]{q1,q2});    
    SpanQuery q7 = new MtasSpanAndQuery(new SpanQuery[]{q6,q3});
    SpanQuery q8 = new MtasSpanAndQuery(new SpanQuery[]{q4,q5});
    SpanQuery q = new MtasSpanOrQuery(new SpanQuery[]{q7,q8});
    testCQLParse(field, cql, q);    
  }
  
  private void basicTest10() {
    String field = "testveld";
    String cql = "[((lemma=\"de\"|lemma=\"het\") & pos=\"LID\") | ((lemma=\"koe\"|lemma=\"paard\") & pos=\"N\")]";
    SpanQuery q1 = new MtasCQLParserWordQuery(field,"lemma","de");
    SpanQuery q2 = new MtasCQLParserWordQuery(field,"lemma","het");
    SpanQuery q3 = new MtasCQLParserWordQuery(field,"pos","LID");
    SpanQuery q4 = new MtasCQLParserWordQuery(field,"lemma","koe");
    SpanQuery q5 = new MtasCQLParserWordQuery(field,"lemma","paard");
    SpanQuery q6 = new MtasCQLParserWordQuery(field,"pos","N");
    SpanQuery q7 = new MtasSpanOrQuery(new SpanQuery[]{q1,q2});    
    SpanQuery q8 = new MtasSpanAndQuery(new SpanQuery[]{q7,q3});
    SpanQuery q9 = new MtasSpanOrQuery(new SpanQuery[]{q4,q5});    
    SpanQuery q10 = new MtasSpanAndQuery(new SpanQuery[]{q9,q6});
    SpanQuery q = new MtasSpanOrQuery(new SpanQuery[]{q8,q10});
    testCQLParse(field, cql, q);    
  }
  
  private void basicTest11() {
    String field = "testveld";
    String cql1 = "[#300]";
    SpanQuery q1 = new MtasCQLParserWordPositionQuery(field, 300);
    testCQLParse(field, cql1, q1); 
    String cql2 = "[#100-110]";
    SpanQuery q2 = new MtasCQLParserWordPositionQuery(field, 100, 110);
    testCQLParse(field, cql2, q2);
    String cql3 = "[#100-105 | #110]";
    SpanQuery q3a = new MtasCQLParserWordPositionQuery(field, 100, 105);
    SpanQuery q3b = new MtasCQLParserWordPositionQuery(field, 110);
    SpanQuery q3 = new MtasSpanOrQuery(q3a, q3b);
    testCQLParse(field, cql3, q3);
  }    

}
