package mtas.parser;

import mtas.parser.cql.MtasCQLParser;
import mtas.parser.cql.ParseException;
import mtas.parser.cql.util.MtasCQLParserGroupQuery;
import mtas.parser.cql.util.MtasCQLParserWordQuery;
import mtas.search.spans.MtasSpanContainingQuery;
import mtas.search.spans.MtasSpanFollowedByQuery;
import mtas.search.spans.MtasSpanFullyAlignedWithQuery;
import mtas.search.spans.MtasSpanIntersectingQuery;
import mtas.search.spans.MtasSpanMatchAllQuery;
import mtas.search.spans.MtasSpanNotQuery;
import mtas.search.spans.MtasSpanOrQuery;
import mtas.search.spans.MtasSpanPrecededByQuery;
import mtas.search.spans.MtasSpanRecurrenceQuery;
import mtas.search.spans.MtasSpanSequenceItem;
import mtas.search.spans.MtasSpanSequenceQuery;
import mtas.search.spans.MtasSpanWithinQuery;
import mtas.search.spans.util.MtasSpanQuery;
import mtas.search.spans.util.MtasSpanUniquePositionQuery;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MtasCQLParserTestSentence {
  private static Log log = LogFactory.getLog(MtasCQLParserTestSentence.class);

  private void testCQLParse(String field, String defaultPrefix, String cql,
      MtasSpanQuery q) {
    MtasCQLParser p = new MtasCQLParser(
        new BufferedReader(new StringReader(cql)));
    try {
      // System.out.print("CQL parsing:\t"+cql);
      assertEquals(p.parse(field, defaultPrefix, null, null, null), q);
      // System.out.print("\n");
    } catch (ParseException e) {
      // System.out.println("Error CQL parsing:\t"+cql);
      log.error(e);
    }
  }

  private void testCQLEquivalent(String field, String defaultPrefix,
      String cql1, String cql2) {
    MtasCQLParser p1 = new MtasCQLParser(
        new BufferedReader(new StringReader(cql1)));
    MtasCQLParser p2 = new MtasCQLParser(
        new BufferedReader(new StringReader(cql2)));
    try {
      // System.out.println("CQL equivalent:\t"+cql1+" and "+cql2);
      assertEquals(p1.parse(field, defaultPrefix, null, null, null),
          p2.parse(field, defaultPrefix, null, null, null));
    } catch (ParseException e) {
      // System.out.println("Error CQL equivalent:\t"+cql1+" and "+cql2);
      log.error(e);
    }
  }

  @org.junit.Test
  public void basicTestCQL1() throws ParseException {
    String field = "testveld";
    String cql = "[pos=\"LID\"] [lemma=\"koe\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q2, false));
    MtasSpanQuery q = new MtasSpanSequenceQuery(items, null, null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL2() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\"] [] []? [] [lemma=\"koe\"]";
    String cql2 = "[pos=\"LID\"] []{2,3} [lemma=\"koe\"]";
    testCQLEquivalent(field, null, cql1, cql2);
  }

  @org.junit.Test
  public void basicTestCQL3() throws ParseException {
    String field = "testveld";
    String cql = "[pos=\"LID\"] | [lemma=\"koe\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q = new MtasSpanOrQuery(q1, q2);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL4() throws ParseException {
    String field = "testveld";
    String cql = "[pos=\"LID\"] | ([lemma=\"de\"] [lemma=\"koe\"])";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "de", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items.add(new MtasSpanSequenceItem(q2, false));
    items.add(new MtasSpanSequenceItem(q3, false));
    MtasSpanQuery q4 = new MtasSpanSequenceQuery(items, null, null);
    MtasSpanQuery q = new MtasSpanOrQuery(q1, q4);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL5() {
    String field = "testveld";
    String cql1 = "([pos=\"LID\"]([pos=\"ADJ\"][lemma=\"koe\"]))";
    String cql2 = "[pos=\"LID\"][pos=\"ADJ\"][lemma=\"koe\"]";
    testCQLEquivalent(field, null, cql1, cql2);
  }

  @org.junit.Test
  public void basicTestCQL6() {
    String field = "testveld";
    String cql1 = "([pos=\"LID\"]|[lemma=\"de\"][lemma=\"koe\"])|([pos=\"ADJ\"]|([lemma=\"het\"]([lemma=\"paard\"])))";
    String cql2 = "[pos=\"LID\"]|[lemma=\"de\"][lemma=\"koe\"]|[pos=\"ADJ\"]|[lemma=\"het\"][lemma=\"paard\"]";
    testCQLEquivalent(field, null, cql1, cql2);
  }

  @org.junit.Test
  public void basicTestCQL7() {
    String field = "testveld";
    String cql1 = "[pos=\"LID\"] []{0,1} []{3,5} []{2,4}";
    String cql2 = "[pos=\"LID\"] []{5,10}";
    testCQLEquivalent(field, null, cql1, cql2);
  }

  @org.junit.Test
  public void basicTestCQL8() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"]([pos=\"N\"]|[pos=\"ADJ\"])";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "pos", "ADJ", null,
        null);
    MtasSpanQuery q4 = new MtasSpanOrQuery(q2, q3);
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q4, false));
    MtasSpanQuery q = new MtasSpanSequenceQuery(items, null, null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL9() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"]([pos=\"N\"]|[pos=\"ADJ\"]){2,3}[lemma=\"paard\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "pos", "ADJ", null,
        null);
    MtasSpanQuery q4 = new MtasCQLParserWordQuery(field, "lemma", "paard", null,
        null);
    MtasSpanQuery q5 = new MtasSpanOrQuery(
        new MtasSpanRecurrenceQuery(q2, 2, 3, null, null),
        new MtasSpanRecurrenceQuery(q3, 2, 3, null, null));
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q5, false));
    items.add(new MtasSpanSequenceItem(q4, false));
    MtasSpanQuery q = new MtasSpanSequenceQuery(items, null, null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL10() throws ParseException {
    String field = "testveld";
    String cql = "[pos=\"LID\"]? [pos=\"ADJ\"]{1,3} [lemma=\"koe\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "pos", "LID", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "pos", "ADJ", null,
        null);
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items.add(new MtasSpanSequenceItem(q1, true));
    items.add(new MtasSpanSequenceItem(
        new MtasSpanRecurrenceQuery(q2, 1, 3, null, null), false));
    items.add(new MtasSpanSequenceItem(q3, false));
    MtasSpanQuery q = new MtasSpanSequenceQuery(items, null, null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL11() throws ParseException {
    String field = "testveld";
    String cql = "<sentence/> containing [lemma=\"koe\"]";
    MtasSpanQuery q1 = new MtasCQLParserGroupQuery(field, "sentence");
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q = new MtasSpanContainingQuery(q1, q2);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL12() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"] within <sentence/>";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserGroupQuery(field, "sentence");
    MtasSpanQuery q = new MtasSpanWithinQuery(q2, q1);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL13() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"]([t=\"de\"] within <sentence/>)";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "t", "de", null, null);
    MtasSpanQuery q3 = new MtasCQLParserGroupQuery(field, "sentence");
    MtasSpanQuery q4 = new MtasSpanWithinQuery(q3, q2);
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q4, false));
    MtasSpanQuery q = new MtasSpanSequenceQuery(items, null, null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL14() throws ParseException {
    String field = "testveld";
    String cql = "([t=\"de\"] within <sentence/>)[lemma=\"koe\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "t", "de", null, null);
    MtasSpanQuery q2 = new MtasCQLParserGroupQuery(field, "sentence");
    MtasSpanQuery q3 = new MtasSpanWithinQuery(q2, q1);
    MtasSpanQuery q4 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items.add(new MtasSpanSequenceItem(q3, false));
    items.add(new MtasSpanSequenceItem(q4, false));
    MtasSpanQuery q = new MtasSpanSequenceQuery(items, null, null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL15() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"](<sentence/> containing [t=\"de\"]) within <sentence/>[lemma=\"paard\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserGroupQuery(field, "sentence");
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "t", "de", null, null);
    MtasSpanQuery q4 = new MtasSpanContainingQuery(q2, q3);
    MtasSpanQuery q5 = new MtasCQLParserGroupQuery(field, "sentence");
    MtasSpanQuery q6 = new MtasCQLParserWordQuery(field, "lemma", "paard", null,
        null);
    List<MtasSpanSequenceItem> items1 = new ArrayList<>();
    items1.add(new MtasSpanSequenceItem(q5, false));
    items1.add(new MtasSpanSequenceItem(q6, false));
    MtasSpanQuery q7 = new MtasSpanSequenceQuery(items1, null, null);
    MtasSpanQuery q8 = new MtasSpanWithinQuery(q7, q4);
    List<MtasSpanSequenceItem> items2 = new ArrayList<>();
    items2.add(new MtasSpanSequenceItem(q1, false));
    items2.add(new MtasSpanSequenceItem(q8, false));
    MtasSpanQuery q = new MtasSpanSequenceQuery(items2, null, null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL16() throws ParseException {
    String field = "testveld";
    String cql = "(<entity=\"loc\"/> within (<s/> containing [t_lc=\"amsterdam\"])) !containing ([t_lc=\"amsterdam\"])";
    MtasSpanQuery q1 = new MtasCQLParserGroupQuery(field, "entity", "loc");
    MtasSpanQuery q2 = new MtasCQLParserGroupQuery(field, "s");
    MtasSpanQuery q3 = new MtasCQLParserWordQuery(field, "t_lc", "amsterdam",
        null, null);
    MtasSpanQuery q4 = new MtasSpanContainingQuery(q2, q3);
    MtasSpanQuery q5 = new MtasSpanWithinQuery(q4, q1);
    MtasSpanQuery q = new MtasSpanNotQuery(q5,
        new MtasSpanContainingQuery(q5, q3));
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL17() {
    String field = "testveld";
    String cql = "[]<entity=\"loc\"/>{1,2}[]";
    MtasSpanQuery q1 = new MtasCQLParserGroupQuery(field, "entity", "loc");
    MtasSpanQuery q2 = new MtasSpanRecurrenceQuery(q1, 1, 2, null, null);
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items
        .add(new MtasSpanSequenceItem(new MtasSpanMatchAllQuery(field), false));
    items.add(new MtasSpanSequenceItem(q2, false));
    items
        .add(new MtasSpanSequenceItem(new MtasSpanMatchAllQuery(field), false));
    MtasSpanQuery q = new MtasSpanSequenceQuery(items, null, null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL18() throws ParseException {
    String field = "testveld";
    String cql = "\"de\" [pos=\"N\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "t_lc", "de", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items.add(new MtasSpanSequenceItem(q1, false));
    items.add(new MtasSpanSequenceItem(q2, false));
    MtasSpanQuery q = new MtasSpanSequenceQuery(items, null, null);
    testCQLParse(field, "t_lc", cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL19() {
    String field = "testveld";
    String cql = "([]<entity=\"loc\"/>{1,2}[]){3,4}";
    MtasSpanQuery q1 = new MtasCQLParserGroupQuery(field, "entity", "loc");
    MtasSpanQuery q2 = new MtasSpanRecurrenceQuery(q1, 1, 2, null, null);
    List<MtasSpanSequenceItem> items = new ArrayList<>();
    items
        .add(new MtasSpanSequenceItem(new MtasSpanMatchAllQuery(field), false));
    items.add(new MtasSpanSequenceItem(q2, false));
    items
        .add(new MtasSpanSequenceItem(new MtasSpanMatchAllQuery(field), false));
    MtasSpanQuery q3 = new MtasSpanSequenceQuery(items, null, null);
    MtasSpanQuery q = new MtasSpanRecurrenceQuery(q3, 3, 4, null, null);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL20() {
    String field = "testveld";
    String cql1 = "[pos=\"N\"]?[pos=\"ADJ\"]";
    String cql2 = "([pos=\"N\"])?[pos=\"ADJ\"]";
    testCQLEquivalent(field, null, cql1, cql2);
    String cql3 = "[pos=\"N\"][pos=\"ADJ\"]?";
    String cql4 = "[pos=\"N\"]([pos=\"ADJ\"])?";
    testCQLEquivalent(field, null, cql3, cql4);
    String cql5 = "[pos=\"N\"][pos=\"ADJ\"]?[pos=\"N\"]";
    String cql6 = "[pos=\"N\"]([pos=\"ADJ\"])?[pos=\"N\"]";
    testCQLEquivalent(field, null, cql5, cql6);
    String cql7 = "[pos=\"N\"]?[pos=\"ADJ\"]?[pos=\"N\"]";
    String cql8 = "([pos=\"N\"])?([pos=\"ADJ\"])?[pos=\"N\"]";
    testCQLEquivalent(field, null, cql7, cql8);
    String cql9 = "[pos=\"N\"][pos=\"ADJ\"]?[pos=\"N\"]?";
    String cql10 = "[pos=\"N\"]([pos=\"ADJ\"])?([pos=\"N\"])?";
    testCQLEquivalent(field, null, cql9, cql10);
  }

  @org.junit.Test
  public void basicTestCQL21() {
    String field = "testveld";
    String cql1 = "(<s/>(<s/> containing [t_lc=\"rembrandt\"])</s>)";
    String cql2 = "<s/>(<s/> containing [t_lc=\"rembrandt\"])</s>";
    testCQLEquivalent(field, null, cql1, cql2);
  }

  @org.junit.Test
  public void basicTestCQL22() throws ParseException {
    String field = "testveld";
    String cql = "([lemma=\"koe\"] within <sentence/>) | [t=\"paard\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserGroupQuery(field, "sentence");
    MtasSpanQuery q3 = new MtasSpanWithinQuery(q2, q1);
    MtasSpanQuery q4 = new MtasCQLParserWordQuery(field, "t", "paard", null,
        null);
    MtasSpanQuery q = new MtasSpanOrQuery(q3, q4);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL23() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"] intersecting <sentence/>";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserGroupQuery(field, "sentence");
    MtasSpanQuery q = new MtasSpanIntersectingQuery(q1, q2);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL24() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"] fullyalignedwith [pos=\"N\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q = new MtasSpanFullyAlignedWithQuery(q1, q2);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL25() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"] followedby [pos=\"N\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q = new MtasSpanFollowedByQuery(q1, q2);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }

  @org.junit.Test
  public void basicTestCQL26() throws ParseException {
    String field = "testveld";
    String cql = "[lemma=\"koe\"] precededby [pos=\"N\"]";
    MtasSpanQuery q1 = new MtasCQLParserWordQuery(field, "lemma", "koe", null,
        null);
    MtasSpanQuery q2 = new MtasCQLParserWordQuery(field, "pos", "N", null,
        null);
    MtasSpanQuery q = new MtasSpanPrecededByQuery(q1, q2);
    testCQLParse(field, null, cql, new MtasSpanUniquePositionQuery(q));
  }
}