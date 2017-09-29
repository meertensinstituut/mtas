package mtas.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import mtas.analysis.token.MtasToken;
import mtas.codec.util.CodecInfo;
import mtas.codec.util.CodecUtil;
import mtas.codec.util.collector.MtasDataItem;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentGroup;
import mtas.codec.util.CodecComponent.ComponentPosition;
import mtas.codec.util.CodecComponent.ComponentSpan;
import mtas.codec.util.CodecComponent.ComponentTermVector;
import mtas.codec.util.CodecComponent.ComponentToken;
import mtas.codec.util.CodecComponent.GroupHit;
import mtas.codec.util.CodecComponent.SubComponentFunction;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;
import mtas.parser.cql.MtasCQLParser;
import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanRegexpQuery;
import mtas.search.spans.util.MtasDisabledTwoPhaseIteratorSpanQuery;
import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSearchTestConsistency.
 */
public class MtasSearchTestConsistency {

  /** The log. */
  private static Log log = LogFactory.getLog(MtasSearchTestConsistency.class);

  /** The Constant FIELD_ID. */
  private static final String FIELD_ID = "id";

  /** The Constant FIELD_TITLE. */
  private static final String FIELD_TITLE = "title";

  /** The Constant FIELD_CONTENT. */
  private static final String FIELD_CONTENT = "content";

  /** The directory. */
  private static Directory directory;

  /** The files. */
  private static HashMap<String, String> files;

  /** The docs. */
  private static ArrayList<Integer> docs;

  /**
   * Initialize.
   */
  @org.junit.BeforeClass
  public static void initialize() {
    try {
      Path path = Paths.get("junit").resolve("data");
      // directory = FSDirectory.open(Paths.get("testindexMtas"));
      directory = new RAMDirectory();
      files = new HashMap<>();
      files.put("Een onaangenaam mens in de Haarlemmerhout",
          path.resolve("resources").resolve("beets1.xml.gz").toAbsolutePath()
              .toString());
      files.put("Een oude kennis", path.resolve("resources")
          .resolve("beets2.xml.gz").toAbsolutePath().toString());
      files.put("Varen en Rijden", path.resolve("resources")
          .resolve("beets3.xml.gz").toAbsolutePath().toString());
      createIndex(
          path.resolve("conf").resolve("folia.xml").toAbsolutePath().toString(),
          files);
      docs = getLiveDocs(DirectoryReader.open(directory));
    } catch (IOException e) {
      log.error(e);
    }
  }

  /**
   * Basic search number of words.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchNumberOfWords() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("[]"),
        Arrays.asList("[][]", "[#0]"));
    indexReader.close();
  }

  /**
   * Basic search start sentence 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchStartSentence1() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("<s/>"),
        Arrays.asList("<s>[]"));
    indexReader.close();
  }

  /**
   * Basic search start sentence 2.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchStartSentence2() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT,
        Arrays.asList("[]</s><s>[]", "[#0]"), Arrays.asList("<s>[]"));
    indexReader.close();
  }

  /**
   * Basic search intersecting 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchIntersecting1() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("<s/>"),
        Arrays.asList("<s/> intersecting [pos=\"ADJ\"]",
            "<s/> !intersecting [pos=\"ADJ\"]"));
    indexReader.close();
  }

  /**
   * Basic search intersecting 2.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchIntersecting2() throws IOException {
    String cql = "([pos=\"N\"][]) intersecting ([pos=\"N\"][])";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql,
        null, null, null, true);
    assertFalse(
        "Intersecting: " + cql + " has no hits (" + queryResult1.hits + ")",
        queryResult1.hits == 0);
    assertEquals("Intersecting: - twoPhaseIterator", queryResult1.hits,
        queryResult1disabled.hits);
  }

  /**
   * Basic search ignore.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchIgnore() throws IOException {
    int ignoreNumber = 10;
    String cql1 = "[pos=\"LID\"][pos=\"ADJ\"]{0," + ignoreNumber
        + "}[pos=\"N\"]";
    String cql2 = "[pos=\"LID\"][pos=\"N\"]";
    String cql2ignore = "[pos=\"ADJ\"]";
    // get total number of nouns
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    MtasSpanQuery ignore;
    try {
      ignore = createQuery(FIELD_CONTENT, cql2ignore, null, null, false);
    } catch (ParseException e) {
      throw new IOException("Parse Exception", e);
    }
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, ignore,
        ignoreNumber, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        ignore, ignoreNumber, null, true);
    assertEquals("Article followed by Noun ignoring Adjectives",
        queryResult1.hits, queryResult2.hits);
    assertEquals(
        "Article followed by Noun ignoring Adjectives - disabled twoPhaseIterator",
        queryResult1disabled.hits, queryResult2disabled.hits);
    assertEquals("Ignore: twoPhaseIterator", queryResult1.hits,
        queryResult1disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search sequence.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchSequence() throws IOException {
    String cql1 = "[pos=\"N\"][]{2,3}[pos=\"LID\"]";
    String cql2 = "[][pos=\"N\"][]{2,3}[pos=\"LID\"][]";
    String cql3 = "[]{0,3}[pos=\"N\"][]{2,3}[pos=\"LID\"][]{0,2}";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null, false);
    QueryResult queryResult3disabled = doQuery(indexReader, FIELD_CONTENT, cql3,
        null, null, null, true);
    assertTrue(
        "Sequences: not #" + cql1 + " (" + queryResult1.hits + ") >= #" + cql2
            + " (" + queryResult2.hits + ")",
        queryResult1.hits >= queryResult2.hits);
    assertTrue(
        "Sequences: not #" + cql1 + " (" + queryResult1.hits + ") >= #" + cql2
            + " (" + queryResult2.hits + ") - disabled twoPhaseIterator",
        queryResult1disabled.hits >= queryResult2disabled.hits);
    assertTrue(
        "Sequences: not #" + cql1 + " (" + queryResult1.hits + ") >= #" + cql3
            + " (" + queryResult3.hits + ")",
        queryResult1.hits <= queryResult3.hits);
    assertTrue(
        "Sequences: not #" + cql1 + " (" + queryResult1.hits + ") >= #" + cql3
            + " (" + queryResult3.hits + ") - disabled twoPhaseIterator",
        queryResult1disabled.hits <= queryResult3disabled.hits);
    assertEquals("Sequences: twoPhaseIterator 1", queryResult1.hits,
        queryResult1disabled.hits);
    assertEquals("Sequences: twoPhaseIterator 2", queryResult2.hits,
        queryResult2disabled.hits);
    assertEquals("Sequences: twoPhaseIterator 3", queryResult3.hits,
        queryResult3disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search within 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchWithin1() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("[]"),
        Arrays.asList("[] within <s/>"));
    indexReader.close();
  }

  /**
   * Basic search within 2.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchWithin2() throws IOException {
    String cql1 = "[pos=\"N\"][][pos=\"LID\"]";
    String cql2 = "[pos=\"N\"][pos=\"N\"][pos=\"LID\"]";
    String cql3 = "[pos=\"N\"] within [pos=\"N\"][][pos=\"LID\"]";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null, false);
    QueryResult queryResult3disabled = doQuery(indexReader, FIELD_CONTENT, cql3,
        null, null, null, true);
    assertFalse("Within: " + cql3 + " has no hits (" + queryResult3.hits + ")",
        queryResult3.hits == 0);
    assertEquals("Within: " + cql3, queryResult3.hits,
        (long) queryResult1.hits + queryResult2.hits);
    assertEquals("Within: " + cql3 + " - disabled twoPhaseIterator",
        queryResult3.hits, (long) queryResult1.hits + queryResult2.hits);
    assertEquals("Within: twoPhaseIterator 1", queryResult1.hits,
        queryResult1disabled.hits);
    assertEquals("Within: twoPhaseIterator 2", queryResult2.hits,
        queryResult2disabled.hits);
    assertEquals("Within: twoPhaseIterator 3", queryResult3.hits,
        queryResult3disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search within 3.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchWithin3() throws IOException {
    String cql1 = "[pos=\"N\"][][pos=\"LID\"][]{2}";
    String cql2 = "[pos=\"N\"][pos=\"N\"][pos=\"LID\"][]{2}";
    String cql3 = "[pos=\"N\"] within []{0,2}[pos=\"N\"][][pos=\"LID\"][]{2,3}";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null, false);
    QueryResult queryResult3disabled = doQuery(indexReader, FIELD_CONTENT, cql3,
        null, null, null, true);
    assertFalse("Within: " + cql3 + " has no hits (" + queryResult3.hits + ")",
        queryResult3.hits == 0);
    assertTrue(
        "Within: " + cql3 + " not " + queryResult3.hits + " >= "
            + queryResult1.hits + " + " + queryResult2.hits,
        queryResult3.hits >= queryResult1.hits + queryResult2.hits);
    assertTrue(
        "Within: " + cql3 + " not " + queryResult3.hits + " >= "
            + queryResult1.hits + " + " + queryResult2.hits
            + " - disabled twoPhaseIterator",
        queryResult3.hits >= queryResult1.hits + queryResult2.hits);
    assertEquals("Within: twoPhaseIterator 1", queryResult1.hits,
        queryResult1disabled.hits);
    assertEquals("Within: twoPhaseIterator 2", queryResult2.hits,
        queryResult2disabled.hits);
    assertEquals("Within: twoPhaseIterator 3", queryResult3.hits,
        queryResult3disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search within 4.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchWithin4() throws IOException {
    String cql = "((<s>) !within ([]{0,5}[pos=\"N\"])) within ([]{0,5}[pos=\"N\"])";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult = doQuery(indexReader, FIELD_CONTENT, cql, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql,
        null, null, null, true);
    assertTrue("Within: " + cql + " has hits (" + queryResult.hits + ")",
        queryResult.hits == 0);
    assertEquals("Within: - twoPhaseIterator", queryResult.hits,
        queryResult1disabled.hits);
  }

  /**
   * Basic search within 5.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchWithin5() throws IOException {
    String cql = "([pos=\"N\"][]) within ([pos=\"N\"][])";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql,
        null, null, null, true);
    assertFalse("Within: " + cql + " has no hits (" + queryResult1.hits + ")",
        queryResult1.hits == 0);
    assertEquals("Within: - twoPhaseIterator", queryResult1.hits,
        queryResult1disabled.hits);
  }

  /**
   * Basic search containing 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchContaining1() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("<s/>"),
        Arrays.asList("<s/> containing [pos=\"ADJ\"]",
            "<s/> !containing [pos=\"ADJ\"]"));
    indexReader.close();
  }

  /**
   * Basic search containing 2.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchContaining2() throws IOException {
    String cql1 = "[pos=\"N\"][][pos=\"LID\"]";
    String cql2 = "[pos=\"N\"][pos=\"N\"][pos=\"LID\"]";
    String cql3 = "[pos=\"N\"][][pos=\"LID\"] containing [pos=\"N\"][pos=\"LID\"]";
    String cql4 = "[pos=\"N\"][][pos=\"LID\"] !containing [pos=\"N\"][pos=\"LID\"]";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null, false);
    QueryResult queryResult3disabled = doQuery(indexReader, FIELD_CONTENT, cql3,
        null, null, null, true);
    QueryResult queryResult4 = doQuery(indexReader, FIELD_CONTENT, cql4, null,
        null, null, false);
    QueryResult queryResult4disabled = doQuery(indexReader, FIELD_CONTENT, cql4,
        null, null, null, true);
    assertEquals("Containing: " + cql3, queryResult3.hits, queryResult2.hits);
    assertEquals("Containing: " + cql3 + " disabled twoPhaseIterator",
        queryResult3disabled.hits, queryResult2disabled.hits);
    assertEquals("Containing: " + cql4, queryResult4.hits,
        (long) queryResult1.hits - queryResult2.hits);
    assertEquals("Containing: " + cql4 + " disabled twoPhaseIterator",
        queryResult4disabled.hits,
        (long) queryResult1disabled.hits - queryResult2disabled.hits);
    assertEquals("Containing: twoPhaseIterator 1", queryResult1.hits,
        queryResult1disabled.hits);
    assertEquals("Containing: twoPhaseIterator 2", queryResult2.hits,
        queryResult2disabled.hits);
    assertEquals("Containing: twoPhaseIterator 3", queryResult3.hits,
        queryResult3disabled.hits);
    assertEquals("Containing: twoPhaseIterator 4", queryResult4.hits,
        queryResult4disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search containing 3.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchContaining3() throws IOException {
    String cql = "(([]{0,5}[pos=\"N\"]) !containing (<s>)) containing (<s>)";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql,
        null, null, null, true);
    assertTrue("Containing: " + cql + " has hits (" + queryResult1.hits + ")",
        queryResult1.hits == 0);
    assertEquals("Containing: twoPhaseIterator", queryResult1.hits,
        queryResult1disabled.hits);
  }

  /**
   * Basic search containing 4.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchContaining4() throws IOException {
    String cql = "([pos=\"N\"][]) containing ([pos=\"N\"][])";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql,
        null, null, null, true);
    assertFalse(
        "Containing: " + cql + " has no hits (" + queryResult1.hits + ")",
        queryResult1.hits == 0);
    assertEquals("Containing: - twoPhaseIterator", queryResult1.hits,
        queryResult1disabled.hits);
  }

  /**
   * Basic search followed by 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchFollowedBy1() throws IOException {
    String cql1 = "[pos=\"LID\"] followedby []?[pos=\"ADJ\"]";
    String cql2 = "[pos=\"LID\"][]?[pos=\"ADJ\"]";
    String cql3 = "[pos=\"LID\"][pos=\"ADJ\"][pos=\"ADJ\"]";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null, false);
    QueryResult queryResult3disabled = doQuery(indexReader, FIELD_CONTENT, cql3,
        null, null, null, true);
    assertEquals("Article followed by Adjective", queryResult1.hits,
        (long) queryResult2.hits - queryResult3.hits);
    assertEquals("Article followed by Adjective - disabled twoPhaseIterator",
        queryResult1.hits,
        (long) queryResult2disabled.hits - queryResult3disabled.hits);
    assertEquals("FollowedBy: twoPhaseIterator 1", queryResult1.hits,
        queryResult1disabled.hits);
    assertEquals("FollowedBy: twoPhaseIterator 2", queryResult2.hits,
        queryResult2disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search followed by 2.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchFollowedBy2() throws IOException {
    String cql1 = "[pos=\"LID\"] followedby []?[pos=\"ADJ\"]";
    String cql2 = "[pos=\"LID\"][]?[pos=\"ADJ\"]";
    String cql3 = "[pos=\"LID\"][pos=\"ADJ\"][pos=\"ADJ\"]";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null, false);
    QueryResult queryResult3disabled = doQuery(indexReader, FIELD_CONTENT, cql3,
        null, null, null, true);
    assertEquals("Article followed by Adjective", queryResult1.hits,
        (long) queryResult2.hits - queryResult3.hits);
    assertEquals("Article followed by Adjective - disabled twoPhaseIterator",
        queryResult1disabled.hits,
        (long) queryResult2disabled.hits - queryResult3disabled.hits);
    assertEquals("FollowedBy: twoPhaseIterator 1", queryResult1.hits,
        queryResult1disabled.hits);
    assertEquals("FollowedBy: twoPhaseIterator 2", queryResult2.hits,
        queryResult2disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search preceded by 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchPrecededBy1() throws IOException {
    String cql1 = "[pos=\"ADJ\"] precededby [pos=\"LID\"][]?";
    String cql2 = "[pos=\"LID\"][]?[pos=\"ADJ\"]";
    String cql3 = "[pos=\"LID\"][pos=\"LID\"][pos=\"ADJ\"]";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null, false);
    QueryResult queryResult3disabled = doQuery(indexReader, FIELD_CONTENT, cql3,
        null, null, null, true);
    assertEquals("Adjective preceded by Article", queryResult1.hits,
        (long) queryResult2.hits - queryResult3.hits);
    assertEquals("Adjective preceded by Article - disabled twoPhaseIterator",
        queryResult1disabled.hits,
        (long) queryResult2disabled.hits - queryResult3disabled.hits);
    assertEquals("PrecededBy: twoPhaseIterator 1", queryResult1.hits,
        queryResult1disabled.hits);
    assertEquals("PrecededBy: twoPhaseIterator 2", queryResult2.hits,
        queryResult2disabled.hits);
    assertEquals("PrecededBy: twoPhaseIterator 3", queryResult3.hits,
        queryResult3disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search preceded by 2.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchPrecededBy2() throws IOException {
    String cql1 = "[]?[pos=\"ADJ\"] precededby [pos=\"LID\"]";
    String cql2 = "[pos=\"LID\"][]?[pos=\"ADJ\"]";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    assertEquals("Adjective preceded by Article", queryResult1.hits,
        queryResult2.hits);
    assertEquals("Adjective preceded by Article - disabled twoPhaseIterator",
        queryResult1disabled.hits, queryResult2disabled.hits);
    assertEquals("PrecededBy: twoPhaseIterator 1", queryResult1.hits,
        queryResult1disabled.hits);
    assertEquals("PrecededBy: twoPhaseIterator 2", queryResult2.hits,
        queryResult2disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search fully aligned with 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchFullyAlignedWith1() throws IOException {
    String cql1 = "[pos=\"N\"]";
    String cql2 = "[] fullyalignedwith [pos=\"N\"]";
    String cql3 = "[pos=\"N\"]{2}";
    String cql4 = "[pos=\"N\"]{1} fullyalignedwith [pos=\"N\"]{2}";
    String cql5 = "[pos=\"N\"]{2} fullyalignedwith [pos=\"N\"]{2}";
    String cql6 = "[pos=\"N\"]{0,3} fullyalignedwith [pos=\"N\"]{2}";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null, false);
    QueryResult queryResult3disabled = doQuery(indexReader, FIELD_CONTENT, cql3,
        null, null, null, true);
    QueryResult queryResult4 = doQuery(indexReader, FIELD_CONTENT, cql4, null,
        null, null, false);
    QueryResult queryResult4disabled = doQuery(indexReader, FIELD_CONTENT, cql4,
        null, null, null, true);
    QueryResult queryResult5 = doQuery(indexReader, FIELD_CONTENT, cql5, null,
        null, null, false);
    QueryResult queryResult5disabled = doQuery(indexReader, FIELD_CONTENT, cql5,
        null, null, null, true);
    QueryResult queryResult6 = doQuery(indexReader, FIELD_CONTENT, cql6, null,
        null, null, false);
    QueryResult queryResult6disabled = doQuery(indexReader, FIELD_CONTENT, cql6,
        null, null, null, true);
    assertEquals("Fully Aligned With (1)", queryResult1.hits,
        queryResult2.hits);
    assertEquals("Fully Aligned With (1) - disable twoPhaseIterator",
        queryResult1disabled.hits, queryResult2disabled.hits);
    assertTrue("Fully Aligned With (2): was " + queryResult4.hits,
        queryResult4.hits == 0);
    assertTrue("Fully Aligned With (2): was" + queryResult4.hits
        + " - disable twoPhaseIterator", queryResult4disabled.hits == 0);
    assertEquals("Fully Aligned With (3)", queryResult3.hits,
        queryResult5.hits);
    assertEquals("Fully Aligned With (3) - disable twoPhaseIterator",
        queryResult3disabled.hits, queryResult5disabled.hits);
    assertEquals("Fully Aligned With (4)", queryResult3.hits,
        queryResult6.hits);
    assertEquals("Fully Aligned With (4) - disable twoPhaseIterator",
        queryResult3disabled.hits, queryResult6disabled.hits);
    assertEquals("FullyAlignedWith: twoPhaseIterator 1", queryResult1.hits,
        queryResult1disabled.hits);
    assertEquals("FullyAlignedWith: twoPhaseIterator 2", queryResult2.hits,
        queryResult2disabled.hits);
    assertEquals("FullyAlignedWith: twoPhaseIterator 3", queryResult3.hits,
        queryResult3disabled.hits);
    assertEquals("FullyAlignedWith: twoPhaseIterator 4", queryResult4.hits,
        queryResult4disabled.hits);
    assertEquals("FullyAlignedWith: twoPhaseIterator 5", queryResult5.hits,
        queryResult5disabled.hits);
    assertEquals("FullyAlignedWith: twoPhaseIterator 6", queryResult6.hits,
        queryResult6disabled.hits);
    indexReader.close();
  }

  /**
   * Basic search fully aligned with 2.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void basicSearchFullyAlignedWith2() throws IOException {
    String cql = "([pos=\"N\"][]) fullyalignedwith ([pos=\"N\"][])";
    // get total number
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql,
        null, null, null, true);
    assertFalse(
        "Fullyalignedwith: " + cql + " has no hits (" + queryResult1.hits + ")",
        queryResult1.hits == 0);
    assertEquals("Fullyalignedwith: - twoPhaseIterator", queryResult1.hits,
        queryResult1disabled.hits);
  }

  /**
   * Collect stats positions 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void collectStatsPositions1() throws IOException {
    // get total number of words
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult = doQuery(indexReader, FIELD_CONTENT, "[]", null,
        null, null, false);
    indexReader.close();
    int averageNumberOfPositions = queryResult.hits / queryResult.docs;
    // do position query
    try {
      ArrayList<Integer> fullDocSet = docs;
      ComponentField fieldStats = new ComponentField(FIELD_ID);
      fieldStats.statsPositionList
          .add(new ComponentPosition("total", null, null, "all"));
      fieldStats.statsPositionList.add(new ComponentPosition("minimum",
          (double) (averageNumberOfPositions - 1), null, "n,sum,mean,min,max"));
      fieldStats.statsPositionList.add(new ComponentPosition("maximum", null,
          (double) averageNumberOfPositions, "sum"));
      Map<String, HashMap<String, Object>> response = doAdvancedSearch(
          fullDocSet, fieldStats);
      Map<String, Object> responseTotal = (Map<String, Object>) response
          .get("statsPositions").get("total");
      Map<String, Object> responseMinimum = (Map<String, Object>) response
          .get("statsPositions").get("minimum");
      Map<String, Object> responseMaximum = (Map<String, Object>) response
          .get("statsPositions").get("maximum");
      Double total = responseTotal != null ? (Double) responseTotal.get("sum")
          : 0;
      Long totalMinimum = responseTotal != null
          ? (Long) responseMinimum.get("sum") : 0;
      Long totalMaximum = responseTotal != null
          ? (Long) responseMaximum.get("sum") : 0;
      assertEquals("Number of positions", total.longValue(), queryResult.hits);
      assertEquals("Minimum and maximum on number of positions",
          total.longValue(), totalMinimum + totalMaximum);
    } catch (mtas.parser.function.ParseException e) {
      log.error(e);
    }
  }

  /**
   * Collect stats positions 2.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void collectStatsPositions2() throws IOException {
    ArrayList<Integer> fullDocSet = docs;
    try {
      // compute total
      ComponentField fieldStats = new ComponentField(FIELD_ID);
      fieldStats.statsPositionList
          .add(new ComponentPosition("total", null, null, "n,sum,min,max"));
      HashMap<String, HashMap<String, Object>> response = doAdvancedSearch(
          fullDocSet, fieldStats);
      HashMap<String, Object> responseTotal = (HashMap<String, Object>) response
          .get("statsPositions").get("total");
      Long docs = responseTotal != null ? (Long) responseTotal.get("n") : 0;
      Long total = responseTotal != null ? (Long) responseTotal.get("sum") : 0;
      Long minimum = responseTotal != null ? (Long) responseTotal.get("min")
          : 0;
      Long maximum = responseTotal != null ? (Long) responseTotal.get("max")
          : 0;
      // compute for each doc
      Long subDocs = Long.valueOf(0);
      Long subTotal = Long.valueOf(0);
      Long subMinimum = null;
      Long subMaximum = null;
      ArrayList<Integer> subDocSet = new ArrayList<>();
      for (Integer docId : fullDocSet) {
        subDocSet.add(docId);
        fieldStats = new ComponentField(FIELD_ID);
        fieldStats.statsPositionList
            .add(new ComponentPosition("total", null, null, "n,sum,min,max"));
        response = doAdvancedSearch(subDocSet, fieldStats);
        responseTotal = (HashMap<String, Object>) response.get("statsPositions")
            .get("total");
        subDocs += responseTotal != null ? (Long) responseTotal.get("n") : 0;
        subTotal += responseTotal != null ? (Long) responseTotal.get("sum") : 0;
        if (subMinimum == null) {
          subMinimum = responseTotal != null ? (Long) responseTotal.get("sum")
              : null;
        } else if (responseTotal != null) {
          subMinimum = Math.min(subMinimum, (Long) responseTotal.get("sum"));
        }
        if (subMaximum == null) {
          subMaximum = responseTotal != null ? (Long) responseTotal.get("sum")
              : null;
        } else if (responseTotal != null) {
          subMaximum = Math.max(subMaximum, (Long) responseTotal.get("sum"));
        }
        subDocSet.clear();
      }
      assertEquals("Number of docs", docs, Long.valueOf(files.size()));
      assertEquals("Number of docs", docs, subDocs);
      assertEquals("Total position", total, subTotal);
      assertEquals("Minimum positions", minimum, subMinimum);
      assertEquals("Maximum positions", maximum, subMaximum);
    } catch (mtas.parser.function.ParseException e) {
      log.error(e);
    }
  }

  /**
   * Collect stats tokens.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void collectStatsTokens() throws IOException {
    ArrayList<Integer> fullDocSet = docs;
    try {
      // compute total
      ComponentField fieldStats = new ComponentField(FIELD_ID);
      fieldStats.statsTokenList
          .add(new ComponentToken("total", null, null, "n,sum,min,max"));
      HashMap<String, HashMap<String, Object>> response = doAdvancedSearch(
          fullDocSet, fieldStats);
      HashMap<String, Object> responseTotal = (HashMap<String, Object>) response
          .get("statsTokens").get("total");
      Long docs = responseTotal != null ? (Long) responseTotal.get("n") : 0;
      Long total = responseTotal != null ? (Long) responseTotal.get("sum") : 0;
      Long minimum = responseTotal != null ? (Long) responseTotal.get("min")
          : 0;
      Long maximum = responseTotal != null ? (Long) responseTotal.get("max")
          : 0;
      // compute for each doc
      Long subDocs = Long.valueOf(0);
      Long subTotal = Long.valueOf(0);
      Long subMinimum = null;
      Long subMaximum = null;
      ArrayList<Integer> subDocSet = new ArrayList<>();
      for (Integer docId : fullDocSet) {
        subDocSet.add(docId);
        fieldStats = new ComponentField(FIELD_ID);
        fieldStats.statsTokenList
            .add(new ComponentToken("total", null, null, "n,sum,min,max"));
        response = doAdvancedSearch(subDocSet, fieldStats);
        responseTotal = (HashMap<String, Object>) response.get("statsTokens")
            .get("total");
        subDocs += responseTotal != null ? (Long) responseTotal.get("n") : 0;
        subTotal += responseTotal != null ? (Long) responseTotal.get("sum") : 0;
        if (subMinimum == null) {
          subMinimum = responseTotal != null ? (Long) responseTotal.get("sum")
              : null;
        } else if (responseTotal != null) {
          subMinimum = Math.min(subMinimum, (Long) responseTotal.get("sum"));
        }
        if (subMaximum == null) {
          subMaximum = responseTotal != null ? (Long) responseTotal.get("sum")
              : null;
        } else if (responseTotal != null) {
          subMaximum = Math.max(subMaximum, (Long) responseTotal.get("sum"));
        }
        subDocSet.clear();
      }
      assertEquals("Number of docs", docs, Long.valueOf(files.size()));
      assertEquals("Number of docs", docs, subDocs);
      assertEquals("Total position", total, subTotal);
      assertEquals("Minimum positions", minimum, subMinimum);
      assertEquals("Maximum positions", maximum, subMaximum);
    } catch (mtas.parser.function.ParseException e) {
      log.error(e);
    }
  }

  /**
   * Collect stats spans 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void collectStatsSpans1() throws IOException {
    String cql1 = "[pos=\"N\"]";
    String cql2 = "[pos=\"LID\"]";
    String cql3 = "[pos=\"N\" | pos=\"LID\"]";
    // get total number of nouns
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null, false);
    QueryResult queryResult1disabled = doQuery(indexReader, FIELD_CONTENT, cql1,
        null, null, null, true);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null, false);
    QueryResult queryResult2disabled = doQuery(indexReader, FIELD_CONTENT, cql2,
        null, null, null, true);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null, false);
    QueryResult queryResult3disabled = doQuery(indexReader, FIELD_CONTENT, cql3,
        null, null, null, true);
    indexReader.close();
    int averageNumberOfPositions = queryResult1.hits / queryResult1.docs;
    // do stats query for nouns
    try {
      ArrayList<Integer> fullDocSet = docs;
      ComponentField fieldStats = new ComponentField(FIELD_ID);
      MtasSpanQuery q1 = createQuery(FIELD_CONTENT, cql1, null, null, false);
      MtasSpanQuery q2 = createQuery(FIELD_CONTENT, cql2, null, null, false);
      MtasSpanQuery q3 = createQuery(FIELD_CONTENT, cql3, null, null, false);
      MtasSpanQuery[] queries1 = new MtasSpanQuery[] { q1 };
      MtasSpanQuery[] queries2 = new MtasSpanQuery[] { q2 };
      MtasSpanQuery[] queries12 = new MtasSpanQuery[] { q1, q2 };
      MtasSpanQuery[] queries3 = new MtasSpanQuery[] { q3 };
      fieldStats.spanQueryList.add(q1);
      fieldStats.spanQueryList.add(q2);
      fieldStats.spanQueryList.add(q3);
      fieldStats.statsSpanList.add(new ComponentSpan(queries1, "total1", null,
          null, "all", null, null, null));
      fieldStats.statsSpanList.add(new ComponentSpan(queries1, "minimum1",
          (double) (averageNumberOfPositions - 1), null, "n,sum,mean,min,max",
          null, null, null));
      fieldStats.statsSpanList.add(new ComponentSpan(queries1, "maximum1", null,
          (double) averageNumberOfPositions, "sum", null, null, null));
      fieldStats.statsSpanList.add(new ComponentSpan(queries2, "total2", null,
          null, "sum", null, null, null));
      fieldStats.statsSpanList.add(new ComponentSpan(queries12, "total12", null,
          null, "sum", new String[] { "difference12" },
          new String[] { "$q0-$q1" }, new String[] { "sum" }));
      fieldStats.statsSpanList.add(new ComponentSpan(queries3, "total3", null,
          null, "sum", null, null, null));
      HashMap<String, HashMap<String, Object>> response = doAdvancedSearch(
          fullDocSet, fieldStats);
      HashMap<String, Object> responseTotal1 = (HashMap<String, Object>) response
          .get("statsSpans").get("total1");
      HashMap<String, Object> responseTotal2 = (HashMap<String, Object>) response
          .get("statsSpans").get("total2");
      HashMap<String, Object> responseTotal12 = (HashMap<String, Object>) response
          .get("statsSpans").get("total12");
      HashMap<String, HashMap<String, Object>> responseFunctionsTotal12 = (HashMap<String, HashMap<String, Object>>) response
          .get("statsSpansFunctions").get("total12");
      HashMap<String, Object> responseTotal3 = (HashMap<String, Object>) response
          .get("statsSpans").get("total3");
      HashMap<String, Object> responseMinimum1 = (HashMap<String, Object>) response
          .get("statsSpans").get("minimum1");
      HashMap<String, Object> responseMaximum1 = (HashMap<String, Object>) response
          .get("statsSpans").get("maximum1");
      Double total1 = responseTotal1 != null
          ? (Double) responseTotal1.get("sum") : 0;
      Long total2 = responseTotal2 != null ? (Long) responseTotal2.get("sum")
          : 0;
      Long total12 = responseTotal12 != null ? (Long) responseTotal12.get("sum")
          : 0;
      Long total3 = responseTotal3 != null ? (Long) responseTotal3.get("sum")
          : 0;
      Long difference12 = responseFunctionsTotal12 != null
          ? (Long) responseFunctionsTotal12.get("difference12").get("sum") : 0;
      Long totalMinimum1 = responseTotal1 != null
          ? (Long) responseMinimum1.get("sum") : 0;
      Long totalMaximum1 = responseTotal1 != null
          ? (Long) responseMaximum1.get("sum") : 0;
      assertEquals("Number of nouns", total1.longValue(), queryResult1.hits);
      assertEquals("Number of nouns - disabled twoPhaseIterator",
          total1.longValue(), queryResult1disabled.hits);
      assertEquals("Number of articles", total2.longValue(), queryResult2.hits);
      assertEquals("Number of articles - disabled twoPhaseIterator",
          total2.longValue(), queryResult2disabled.hits);
      assertEquals("Number of nouns and articles - external 1",
          total12.longValue(), (long) queryResult1.hits + queryResult2.hits);
      assertEquals(
          "Number of nouns and articles - external 1 - disabled twoPhaseIterator",
          total12.longValue(),
          (long) queryResult1disabled.hits + queryResult2disabled.hits);
      assertEquals("Number of nouns and articles - external 2",
          total12.longValue(), queryResult3.hits);
      assertEquals(
          "Number of nouns and articles - external 2 - disabled twoPhaseIterator",
          total12.longValue(), queryResult3disabled.hits);
      assertEquals("Number of nouns and articles - internal",
          total12.longValue(), total3.longValue());
      assertEquals("Number of nouns and articles - functions",
          difference12.longValue(),
          (long) queryResult1.hits - queryResult2.hits);
      assertEquals(
          "Number of nouns and articles - functions - disabled twoPhaseIterator",
          difference12.longValue(),
          (long) queryResult1disabled.hits - queryResult2disabled.hits);
      assertEquals("Minimum and maximum on number of positions nouns",
          total1.longValue(), totalMinimum1 + totalMaximum1);
    } catch (mtas.parser.function.ParseException | ParseException e) {
      log.error(e);
    }
  }

  /**
   * Collect group.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void collectGroup() throws IOException {
    String cql = "[pos=\"LID\"]";
    DirectoryReader indexReader = DirectoryReader.open(directory);
    try {
      ArrayList<Integer> fullDocSet = docs;
      ComponentField fieldStats = new ComponentField(FIELD_ID);
      MtasSpanQuery q = createQuery(FIELD_CONTENT, cql, null, null, false);
      fieldStats.spanQueryList.add(q);
      fieldStats.statsSpanList.add(new ComponentSpan(new MtasSpanQuery[] { q },
          "total", null, null, "sum", null, null, null));
      fieldStats.groupList.add(new ComponentGroup(q, "articles",
          Integer.MAX_VALUE, 0, "t_lc", null, null, null, null, null, null, null,
          null, null, null, null, null));
      HashMap<String, HashMap<String, Object>> response = doAdvancedSearch(
          fullDocSet, fieldStats);
      ArrayList<HashMap<String, Object>> list = (ArrayList<HashMap<String, Object>>) response
          .get("group").get("articles");
      int subTotal = 0;
      for (HashMap<String, Object> listItem : list) {
        HashMap<String, HashMap<Integer, HashMap<String, String>[]>> group = (HashMap<String, HashMap<Integer, HashMap<String, String>[]>>) listItem
            .get("group");
        HashMap<Integer, HashMap<String, String>[]> hitList = group.get("hit");
        HashMap<String, String> hitListItem = hitList.get(0)[0];
        cql = "[pos=\"LID\" & " + hitListItem.get("prefix") + "=\""
            + hitListItem.get("value") + "\"]";
        QueryResult queryResult = doQuery(indexReader, FIELD_CONTENT, cql, null,
            null, null, false);
        assertEquals(
            "number of hits for articles equals to " + hitListItem.get("value"),
            listItem.get("sum"), Long.valueOf(queryResult.hits));
        subTotal += queryResult.hits;
      }
      HashMap<String, Object> responseTotal = (HashMap<String, Object>) response
          .get("statsSpans").get("total");
      Long total = responseTotal != null ? (Long) responseTotal.get("sum") : 0;
      assertEquals("Total number of articles", total, Long.valueOf(subTotal));
      indexReader.close();
    } catch (ParseException | mtas.parser.function.ParseException e) {
      log.error(e);
    } finally {
      indexReader.close();
    }
  }

  /**
   * Collect termvector.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void collectTermvector() throws IOException {
    String prefix = "t_lc";
    Integer number = 100;
    IndexReader indexReader = DirectoryReader.open(directory);
    try {
      ArrayList<Integer> fullDocSet = docs;
      ComponentField fieldStats = new ComponentField(FIELD_ID);
      fieldStats.statsPositionList
          .add(new ComponentPosition("total", null, null, "sum"));
      fieldStats.termVectorList
          .add(new ComponentTermVector("toplist", prefix, null, false, "sum",
              CodecUtil.STATS_TYPE_SUM, CodecUtil.SORT_DESC, null, number, null,
              null, null, null, null, null, prefix, null, null));
      fieldStats.termVectorList.add(new ComponentTermVector("fulllist", prefix,
          null, true, "sum", CodecUtil.STATS_TYPE_SUM, CodecUtil.SORT_DESC,
          null, Integer.MAX_VALUE, null, null, null, null, null, null, prefix,
          null, null));
      HashMap<String, HashMap<String, Object>> response = doAdvancedSearch(
          fullDocSet, fieldStats);
      HashMap<String, Object> responseTotal = (HashMap<String, Object>) response
          .get("statsPositions").get("total");
      Long total = responseTotal != null ? (Long) responseTotal.get("sum") : 0;
      Map<String, Object> topList = (Map<String, Object>) response
          .get("termvector").get("toplist");
      Map<String, Object> fullList = (Map<String, Object>) response
          .get("termvector").get("fulllist");

      for (Entry<String, Object> entry : topList.entrySet()) {
        HashMap<String, Object> responseTopTotal = (HashMap<String, Object>) entry
            .getValue();
        HashMap<String, Object> responseFullTotal = (HashMap<String, Object>) fullList
            .get(entry.getKey());
        Long topTotal = responseTopTotal != null
            ? (Long) responseTopTotal.get("sum") : 0;
        Long subFullTotal = responseFullTotal != null
            ? (Long) responseFullTotal.get("sum") : 0;
        // recompute
        String termBase = prefix + MtasToken.DELIMITER + entry.getKey();
        MtasSpanQuery q = new MtasSpanRegexpQuery(new Term(FIELD_CONTENT,
            "\"" + termBase.replace("\"", "\"\\\"\"") + "\"\u0000*"), true);
        QueryResult queryResult = doQuery(indexReader, FIELD_CONTENT, q, null);
        assertEquals(
            "Number of hits for topItem for " + termBase + " computed directly",
            topTotal, Long.valueOf(queryResult.hits));
        assertEquals("Number of hits for topItem for " + termBase
            + " compared with fullItem", topTotal, subFullTotal);
      }
      Long fullTotal = Long.valueOf(0);
      for (Entry<String, Object> entry : fullList.entrySet()) {
        HashMap<String, Object> responseFullTotal = (HashMap<String, Object>) entry
            .getValue();
        Long subFullTotal = responseFullTotal != null
            ? (Long) responseFullTotal.get("sum") : 0;
        fullTotal += subFullTotal;
      }
      assertEquals("Total number of hits for full list and positions", total,
          fullTotal);
      indexReader.close();
    } catch (mtas.parser.function.ParseException e) {
      log.error(e);
    } finally {
      indexReader.close();
    }
  }

  /**
   * Do advanced search.
   *
   * @param fullDocSet the full doc set
   * @param fieldStats the field stats
   * @return the hash map
   */
  private HashMap<String, HashMap<String, Object>> doAdvancedSearch(
      ArrayList<Integer> fullDocSet, ComponentField fieldStats) {
    HashMap<String, HashMap<String, Object>> response = new HashMap<>();
    IndexReader indexReader;
    try {
      indexReader = DirectoryReader.open(directory);
      IndexSearcher searcher = new IndexSearcher(indexReader);
      ArrayList<Integer> fullDocList = new ArrayList<>();
      CodecUtil.collectField(FIELD_CONTENT, searcher, indexReader, fullDocList,
          fullDocSet, fieldStats);
      // add stats - position
      response.put("statsPositions", new HashMap<String, Object>());
      for (ComponentPosition cp : fieldStats.statsPositionList) {
        response.get("statsPositions").put(cp.key,
            cp.dataCollector.getResult().getData().rewrite(false));
      }
      response.put("statsTokens", new HashMap<String, Object>());
      for (ComponentToken ct : fieldStats.statsTokenList) {
        response.get("statsTokens").put(ct.key,
            ct.dataCollector.getResult().getData().rewrite(false));
      }
      response.put("statsSpans", new HashMap<String, Object>());
      response.put("statsSpansFunctions", new HashMap<String, Object>());
      for (ComponentSpan cs : fieldStats.statsSpanList) {
        response.get("statsSpans").put(cs.key,
            cs.dataCollector.getResult().getData().rewrite(false));
        HashMap<String, Object> functions = new HashMap<>();
        response.get("statsSpansFunctions").put(cs.key, functions);
        for (SubComponentFunction scf : cs.functions) {
          functions.put(scf.key,
              scf.dataCollector.getResult().getData().rewrite(false));
        }
      }
      response.put("group", new HashMap<String, Object>());
      for (ComponentGroup cg : fieldStats.groupList) {
        SortedMap<String, ?> list = cg.dataCollector.getResult().getList();
        ArrayList<HashMap<String, Object>> groupList = new ArrayList<>();
        for (Entry<String, ?> entry : list.entrySet()) {
          HashMap<String, Object> subList = new HashMap<>();
          StringBuilder newKey = new StringBuilder("");
          subList.put("group", GroupHit.keyToObject(entry.getKey(), newKey));
          subList.put("key", newKey.toString().trim());
          subList
              .putAll(((MtasDataItem<?, ?>) entry.getValue()).rewrite(false));
          groupList.add(subList);
        }
        response.get("group").put(cg.key, groupList);
      }
      response.put("termvector", new HashMap<String, Object>());
      for (ComponentTermVector ct : fieldStats.termVectorList) {
        HashMap<String, Map<String, Object>> tvList = new HashMap<>();
        Map<String, ?> tcList = ct.subComponentFunction.dataCollector
            .getResult().getList();
        for (Entry<String, ?> entry : tcList.entrySet()) {
          tvList.put(entry.getKey(),
              ((MtasDataItem<?, ?>) entry.getValue()).rewrite(false));
        }
        response.get("termvector").put(ct.key, tvList);
      }
      indexReader.close();
    } catch (IOException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      log.error(e);
    }
    return response;
  }

  /**
   * Creates the index.
   *
   * @param configFile the config file
   * @param files the files
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createIndex(String configFile,
      HashMap<String, String> files) throws IOException {
    // analyzer
    Map<String, String> paramsCharFilterMtas = new HashMap<>();
    paramsCharFilterMtas.put("type", "file");
    Map<String, String> paramsTokenizer = new HashMap<>();
    paramsTokenizer.put("configFile", configFile);
    Analyzer mtasAnalyzer = CustomAnalyzer
        .builder(Paths.get("docker").toAbsolutePath())
        .addCharFilter("mtas", paramsCharFilterMtas)
        .withTokenizer("mtas", paramsTokenizer).build();
    Map<String, Analyzer> analyzerPerField = new HashMap<>();
    analyzerPerField.put(FIELD_CONTENT, mtasAnalyzer);
    PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(
        new StandardAnalyzer(), analyzerPerField);
    // indexwriter
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setCodec(Codec.forName("MtasCodec"));
    IndexWriter w = new IndexWriter(directory, config);
    // delete
    w.deleteAll();
    // add
    int counter = 0;
    for (Entry<String, String> entry : files.entrySet()) {
      addDoc(w, counter, entry.getKey(), entry.getValue());
      if (counter == 0) {
        w.commit();
      } else {
        addDoc(w, counter, entry.getKey(), entry.getValue());
        addDoc(w, counter, "deletable", entry.getValue());
        w.commit();
        w.deleteDocuments(new Term(FIELD_ID, Integer.toString(counter)));
        w.deleteDocuments(new Term(FIELD_TITLE, "deletable"));
        addDoc(w, counter, entry.getKey(), entry.getValue());
      }
      counter++;
    }
    w.commit();
    // finish
    w.close();
  }

  /**
   * Adds the doc.
   *
   * @param w the w
   * @param id the id
   * @param title the title
   * @param file the file
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void addDoc(IndexWriter w, Integer id, String title,
      String file) throws IOException {
    try {
      Document doc = new Document();
      doc.add(new StringField(FIELD_ID, id.toString(), Field.Store.YES));
      doc.add(new StringField(FIELD_TITLE, title, Field.Store.YES));
      doc.add(new TextField(FIELD_CONTENT, file, Field.Store.YES));
      w.addDocument(doc);
    } catch (Exception e) {
      log.error("Couldn't add " + title + " (" + file + ")", e);
    }
  }

  /**
   * Gets the live docs.
   *
   * @param indexReader the index reader
   * @return the live docs
   */
  private static ArrayList<Integer> getLiveDocs(IndexReader indexReader) {
    ArrayList<Integer> list = new ArrayList<>();
    ListIterator<LeafReaderContext> iterator = indexReader.leaves()
        .listIterator();
    while (iterator.hasNext()) {
      LeafReaderContext lrc = iterator.next();
      SegmentReader r = (SegmentReader) lrc.reader();
      for (int docId = 0; docId < r.maxDoc(); docId++) {
        if (r.numDocs() == r.maxDoc() || r.getLiveDocs().get(docId)) {
          list.add(lrc.docBase + docId);
        }
      }
    }
    return list;
  }

  /**
   * Creates the query.
   *
   * @param field the field
   * @param cql the cql
   * @param ignore the ignore
   * @param maximumIgnoreLength the maximum ignore length
   * @param disableTwoPhaseIterator the disable two phase iterator
   * @return the mtas span query
   * @throws ParseException the parse exception
   */
  private MtasSpanQuery createQuery(String field, String cql,
      MtasSpanQuery ignore, Integer maximumIgnoreLength,
      boolean disableTwoPhaseIterator) throws ParseException {
    Reader reader = new BufferedReader(new StringReader(cql));
    MtasCQLParser p = new MtasCQLParser(reader);
    MtasSpanQuery q = p.parse(field, null, null, ignore, maximumIgnoreLength);
    if (disableTwoPhaseIterator) {
      return new MtasDisabledTwoPhaseIteratorSpanQuery(q);
    } else {
      return q;
    }
  }

  /**
   * Do query.
   *
   * @param indexReader the index reader
   * @param field the field
   * @param cql the cql
   * @param ignore the ignore
   * @param maximumIgnoreLength the maximum ignore length
   * @param prefixes the prefixes
   * @param disableTwoPhaseIterator the disable two phase iterator
   * @return the query result
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private QueryResult doQuery(IndexReader indexReader, String field, String cql,
      MtasSpanQuery ignore, Integer maximumIgnoreLength,
      ArrayList<String> prefixes, boolean disableTwoPhaseIterator)
      throws IOException {
    QueryResult queryResult = new QueryResult();
    try {
      MtasSpanQuery q = createQuery(field, cql, ignore, maximumIgnoreLength,
          disableTwoPhaseIterator);
      queryResult = doQuery(indexReader, field, q, prefixes);
    } catch (mtas.parser.cql.ParseException e) {
      log.error(e);
    }
    return queryResult;
  }

  /**
   * Do query.
   *
   * @param indexReader the index reader
   * @param field the field
   * @param q the q
   * @param prefixes the prefixes
   * @return the query result
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private QueryResult doQuery(IndexReader indexReader, String field,
      MtasSpanQuery q, ArrayList<String> prefixes) throws IOException {
    QueryResult queryResult = new QueryResult();
    ListIterator<LeafReaderContext> iterator = indexReader.leaves()
        .listIterator();
    IndexSearcher searcher = new IndexSearcher(indexReader);
    final float boost = 0;
    SpanWeight spanweight = q.rewrite(indexReader).createWeight(searcher,
        false, boost);

    while (iterator.hasNext()) {
      LeafReaderContext lrc = iterator.next();
      Spans spans = spanweight.getSpans(lrc, SpanWeight.Postings.POSITIONS);
      SegmentReader r = (SegmentReader) lrc.reader();
      Terms t = r.terms(field);
      CodecInfo mtasCodecInfo = CodecInfo.getCodecInfoFromTerms(t);
      if (spans != null) {
        while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
          if (r.numDocs() == r.maxDoc() || r.getLiveDocs().get(spans.docID())) {
            queryResult.docs++;
            while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
              queryResult.hits++;
              if (prefixes != null && !prefixes.isEmpty()) {
                List<MtasTreeHit<String>> terms = mtasCodecInfo
                    .getPositionedTermsByPrefixesAndPositionRange(field,
                        spans.docID(), prefixes, spans.startPosition(),
                        (spans.endPosition() - 1));
                for (MtasTreeHit<String> term : terms) {
                  queryResult.resultList.add(new QueryHit(
                      lrc.docBase + spans.docID(), term.startPosition,
                      term.endPosition, CodecUtil.termPrefix(term.data),
                      CodecUtil.termValue(term.data)));
                }
              }
            }
          }
        }
      }
    }
    return queryResult;
  }

  /**
   * Test number of hits.
   *
   * @param indexReader the index reader
   * @param field the field
   * @param cqls1 the cqls 1
   * @param cqls2 the cqls 2
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void testNumberOfHits(IndexReader indexReader, String field,
      List<String> cqls1, List<String> cqls2) throws IOException {
    int sum1 = 0;
    int sum2 = 0;
    int sum3 = 0;
    int sum4 = 0;
    QueryResult queryResult;
    for (String cql1 : cqls1) {
      queryResult = doQuery(indexReader, field, cql1, null, null, null, false);
      sum1 += queryResult.hits;
      queryResult = doQuery(indexReader, field, cql1, null, null, null, true);
      sum2 += queryResult.hits;
    }
    for (String cql2 : cqls2) {
      queryResult = doQuery(indexReader, field, cql2, null, null, null, false);
      sum3 += queryResult.hits;
      queryResult = doQuery(indexReader, field, cql2, null, null, null, true);
      sum4 += queryResult.hits;
    }
    assertEquals("twoPhaseIterator enabled for " + cqls1 + " (" + sum1
        + ") and " + cqls2 + " (" + sum3 + ")", sum1, sum3);
    assertEquals("twoPhaseIterator disabled for " + cqls1 + " (" + sum2
        + ") and " + cqls2 + " (" + sum4 + ")", sum2, sum4);
    assertEquals(
        "twoPhaseIterator for " + cqls1 + " (" + sum1 + " / " + sum2 + ")",
        sum1, sum2);
  }

  /**
   * The Class QueryResult.
   */
  private static class QueryResult {

    /** The docs. */
    public int docs;

    /** The hits. */
    public int hits;

    /** The result list. */
    public List<QueryHit> resultList;

    /**
     * Instantiates a new query result.
     */
    public QueryResult() {
      docs = 0;
      hits = 0;
      resultList = new ArrayList<>();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append(docs + " document(s), ");
      buffer.append(hits + " hit(s)");
      return buffer.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      QueryResult other = (QueryResult) obj;
      return other.hits == hits && other.docs == docs;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      int h = this.getClass().getSimpleName().hashCode();
      h = (h * 5) ^ docs;
      h = (h * 7) ^ hits;
      return h;
    }

  }

  /**
   * The Class QueryHit.
   */
  private static class QueryHit {

    /**
     * Instantiates a new query hit.
     *
     * @param docId the doc id
     * @param startPosition the start position
     * @param endPosition the end position
     * @param prefix the prefix
     * @param value the value
     */
    protected QueryHit(int docId, int startPosition, int endPosition,
        String prefix, String value) {
    }
  }

}