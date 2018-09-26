package mtas.parser;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.StringReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mtas.parser.cql.MtasCQLParser;
import mtas.parser.cql.ParseException;
import mtas.parser.cql.util.MtasCQLParserWordPositionQuery;
import mtas.parser.cql.util.MtasCQLParserWordQuery;
import mtas.search.spans.MtasSpanAndQuery;
import mtas.search.spans.MtasSpanNotQuery;
import mtas.search.spans.MtasSpanOrQuery;
import mtas.search.spans.util.MtasSpanQuery;
import mtas.search.spans.util.MtasSpanUniquePositionQuery;

/**
 * The Class MtasCQLParserTestWord.
 */
public class MtasCQLParserTestWord {

  private static Log log = LogFactory.getLog(MtasCQLParserTestWord.class);

  /**
   * Test CQL parse.
   *
   * @param field the field
   * @param defaultPrefix the default prefix
   * @param cql the cql
   * @param q the q
   */
  private void testCQLParse(String field, String defaultPrefix, String cql,
      MtasSpanQuery q) {
    MtasCQLParser p = new MtasCQLParser(
        new BufferedReader(new StringReader(cql)));
    try {
      assertEquals(p.parse(field, defaultPrefix, null, null, null), q);
      // System.out.println("Tested CQL parsing:\t"+cql);
    } catch (ParseException e) {
      // System.out.println("Error CQL parsing:\t"+cql);
      log.error(e);
    }
  }

  /**
   * Test CQL equivalent.
   *
   * @param field the field
   * @param defaultPrefix the default prefix
   * @param cql1 the cql 1
   * @param cql2 the cql 2
   */
  private void testCQLEquivalent(String field, String defaultPrefix,
      String cql1, String cql2) {
    MtasCQLParser p1 = new MtasCQLParser(
        new BufferedReader(new StringReader(cql1)));
    MtasCQLParser p2 = new MtasCQLParser(
        new BufferedReader(new StringReader(cql2)));
    try {
      assertEquals(p1.parse(field, defaultPrefix, null, null, null),
          p2.parse(field, defaultPrefix, null, null, null));
      // System.out.println("Tested CQL equivalent:\t"+cql1+" and "+cql2);
    } catch (ParseException e) {
      // System.out.println("Error CQL equivalent:\t"+cql1+" and "+cql2);
      log.error(e);
    }
  }

  /**
   * Basic not test CQL 1.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicNotTestCQL1() throws ParseException {
    String field = "testveld";
    String cql = "[pos=\"LID\" & !lemma=\"de\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "de", null,
        null);
    MtasSpanQuery q = new MtasSpanNotQuery(q1, q2);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic not test CQL 2.
   */
  @org.junit.Test
  public void basicNotTestCQL2() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\" & (!lemma=\"de\")]";
    String cql2 = "[pos=\"LID\" & !(lemma=\"de\")]";
    testCQLEquivalent(field, null, cql1, cql2);
  }

  /**
   * Basic not test CQL 3.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicNotTestCQL3() throws ParseException {
    String field = "testveld";
    String cql = "[pos=\"LID\" & !(lemma=\"de\" | lemma=\"een\")]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "de", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "lemma", "een", null,
        null);
    MtasSpanQuery q4 = new MtasSpanOrQuery(q2, q3);
    MtasSpanQuery q = new MtasSpanNotQuery(q1, q4);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic not test CQL 4.
   */
  @org.junit.Test
  public void basicNotTestCQL4() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\" & !(lemma=\"de\" | lemma=\"een\")]";
    String cql2 = "[pos=\"LID\" & (!lemma=\"de\" & !lemma=\"een\")]";
    testCQLEquivalent(field, null, cql1, cql2);
  }

  /**
   * Basic not test CQL 5.
   */
  @org.junit.Test
  public void basicNotTestCQL5() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\" & !(lemma=\"de\" | lemma=\"een\")]";
    String cql2 = "[pos=\"LID\" & !lemma=\"de\" & !lemma=\"een\"]";
    testCQLEquivalent(field, null, cql1, cql2);
  }

  /**
   * Basic test CQL 1.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL1() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"]";
    MtasSpanQuery q = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 2.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL2() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\" & pos=\"N\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q = new MtasSpanAndQuery(q1, q2);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 3.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL3() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\" | lemma=\"paard\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "paard", null,
        null);
    MtasSpanQuery q = new MtasSpanOrQuery(q1, q2);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 4.
   */
  @org.junit.Test
  public void basicTestCQL4() {
    String field = "testveld";
    String cql1 = "[lemma=\"koe\" | lemma=\"paard\"]";
    String cql2 = "[(lemma=\"koe\" | lemma=\"paard\")]";
    testCQLEquivalent(field, null, cql1, cql2);
  }

  /**
   * Basic test CQL 5.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL5() throws ParseException {
    String field = "testveld";
    String cql = "[(lemma=\"koe\" | lemma=\"paard\") & pos=\"N\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "paard", null,
        null);
    MtasSpanQuery q3 = new MtasSpanOrQuery(q1, q2);
    MtasSpanQuery q4 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q = new MtasSpanAndQuery(q3, q4);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 6.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL6() throws ParseException {
    String field = "testveld";
    String cql = "[pos=\"N\" & (lemma=\"koe\" | lemma=\"paard\")]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "lemma", "paard", null,
        null);
    MtasSpanQuery q4 = new MtasSpanOrQuery(q2, q3);
    MtasSpanQuery q = new MtasSpanAndQuery(q1, q4);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 7.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL7() throws ParseException {
    String field = "testveld";
    String cql = "[pos=\"LID\" | (lemma=\"koe\" & pos=\"N\")]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q4 = new MtasSpanAndQuery(q2, q3);
    MtasSpanQuery q = new MtasSpanOrQuery(q1, q4);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 8.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL8() throws ParseException {
    String field = "testveld";
    String cql = "[(lemma=\"de\" & pos=\"LID\") | (lemma=\"koe\" & pos=\"N\")]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "de", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q4 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q5 = new MtasSpanAndQuery(q1, q2);
    MtasSpanQuery q6 = new MtasSpanAndQuery(q3, q4);
    MtasSpanQuery q = new MtasSpanOrQuery(q5, q6);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 9.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL9() throws ParseException {
    String field = "testveld";
    String cql = "[((lemma=\"de\"|lemma=\"het\") & pos=\"LID\") | (lemma=\"koe\" & pos=\"N\")]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "de", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "het", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q4 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q5 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q6 = new MtasSpanOrQuery(q1, q2);
    MtasSpanQuery q7 = new MtasSpanAndQuery(q6, q3);
    MtasSpanQuery q8 = new MtasSpanAndQuery(q4, q5);
    MtasSpanQuery q = new MtasSpanOrQuery(q7, q8);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 10.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL10() throws ParseException {
    String field = "testveld";
    String cql = "[((lemma=\"de\"|lemma=\"het\") & pos=\"LID\") | ((lemma=\"koe\"|lemma=\"paard\") & pos=\"N\")]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "de", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "het", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q4 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q5 = new MtasCQLParserWordQuery(field, "lemma", "paard", null,
        null);
    MtasSpanQuery q6 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q7 = new MtasSpanOrQuery(q1, q2);
    MtasSpanQuery q8 = new MtasSpanAndQuery(q7, q3);
    MtasSpanQuery q9 = new MtasSpanOrQuery(q4, q5);
    MtasSpanQuery q10 = new MtasSpanAndQuery(q9, q6);
    MtasSpanQuery q = new MtasSpanOrQuery(q8, q10);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 11.
   */
  @org.junit.Test
  public void basicTestCQL11() {
    String field = "testveld";
    String cql1 = "[#300]";
    MtasSpanQuery q1 = new MtasCQLParserWordPositionQuery(field, 300);
    testCQLParse(field, null, cql1, new MtasSpanUniquePositionQuery(q1));
    String cql2 = "[#100-110]";
    MtasSpanQuery q2 = new MtasCQLParserWordPositionQuery(field, 100, 110);
    testCQLParse(field, null, cql2, new MtasSpanUniquePositionQuery(q2));
    String cql3 = "[#100-105 | #110]";
    MtasSpanQuery q3a = new MtasCQLParserWordPositionQuery(field, 100, 105);
    MtasSpanQuery q3b = new MtasCQLParserWordPositionQuery(field, 110);
    MtasSpanQuery q3 = new MtasSpanOrQuery(q3a, q3b);
    testCQLParse(field, null, cql3, new MtasSpanUniquePositionQuery(q3));
  }

  /**
   * Basic test CQL 12.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL12() throws ParseException {
    String field = "testveld";
    String cql = "[(t_lc=\"de\"|t_lc=\"het\"|t_lc=\"paard\")]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "t_lc", "de", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "t_lc", "het", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "t_lc", "paard", null,
        null);
    MtasSpanQuery q = new MtasSpanOrQuery(q1, q2, q3);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  /**
   * Basic test CQL 13.
   *
   * @throws ParseException the parse exception
   */
  @org.junit.Test
  public void basicTestCQL13() throws ParseException {
    String field = "testveld";
    String cql = "\"de\"";
    MtasSpanQuery q = new MtasCQLParserWordQuery(field, "t_lc", "de", null,
        null);
    testCQLParse(field, "t_lc", cql, new MtasSpanUniquePositionQuery(q));
  }
}
