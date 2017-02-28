package mtas.search;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.IntStream;

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
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import mtas.codec.util.CodecInfo;
import mtas.codec.util.CodecUtil;
import mtas.codec.util.collector.MtasDataItem;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentGroup;
import mtas.codec.util.CodecComponent.ComponentPosition;
import mtas.codec.util.CodecComponent.ComponentSpan;
import mtas.codec.util.CodecComponent.ComponentToken;
import mtas.codec.util.CodecComponent.GroupHit;
import mtas.codec.util.CodecComponent.SubComponentFunction;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;
import mtas.parser.cql.MtasCQLParser;
import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanSequenceQuery;
import mtas.search.spans.util.MtasSpanQuery;

public class MtasSearchTestConsistency {

  private static String FIELD_ID = "id";
  private static String FIELD_TITLE = "title";
  private static String FIELD_CONTENT = "content";

  private static Directory directory;

  private static HashMap<String, String> files;

  @org.junit.BeforeClass
  public static void initialize() {
    try {
      String path = new File("junit/data").getCanonicalPath() + File.separator;
      // directory = FSDirectory.open(Paths.get("testindexMtas"));
      directory = new RAMDirectory();
      files = new HashMap<String, String>();
      files.put("Een onaangenaam mens in de Haarlemmerhout",
          path + "resources/beets1.xml.gz");
      files.put("Een oude kennis", path + "resources/beets2.xml.gz");
      files.put("Varen en Rijden", path + "resources/beets3.xml.gz");
      createIndex(path + "conf/folia.xml", files);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void basicSearchNumberOfWords() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("[]"),
        Arrays.asList("[][]", "[#0]"));
    indexReader.close();
  }

  @org.junit.Test
  public void basicSearchStartSentence1() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("<s/>"),
        Arrays.asList("<s>[]"));
    indexReader.close();
  }

  @org.junit.Test
  public void basicSearchStartSentence2() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT,
        Arrays.asList("[]</s><s>[]", "[#0]"), Arrays.asList("<s>[]"));
    indexReader.close();
  }

  @org.junit.Test
  public void basicSearchContaining() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("<s/>"),
        Arrays.asList("<s/> containing [pos=\"ADJ\"]",
            "<s/> !containing [pos=\"ADJ\"]"));
    indexReader.close();
  }

  @org.junit.Test
  public void basicSearchIntersecting() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("<s/>"),
        Arrays.asList("<s/> intersecting [pos=\"ADJ\"]",
            "<s/> !intersecting [pos=\"ADJ\"]"));
    indexReader.close();
  }

  @org.junit.Test
  public void basicSearchWithin() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    testNumberOfHits(indexReader, FIELD_CONTENT, Arrays.asList("[]"),
        Arrays.asList("[] within <s/>"));
    indexReader.close();
  }

  @org.junit.Test
  public void basicSearchIgnore() throws IOException, ParseException {
    int ignoreNumber = 10;
    String cql1 = "[pos=\"LID\"][pos=\"ADJ\"]{0," + ignoreNumber
        + "}[pos=\"N\"]";
    String cql2 = "[pos=\"LID\"][pos=\"N\"]";
    String cql2ignore = "[pos=\"ADJ\"]";
    // get total number of nouns
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null);
    MtasSpanQuery ignore = createQuery(FIELD_CONTENT, cql2ignore, null, null);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, ignore,
        ignoreNumber, null);
    assertEquals("Article followed by Noun ignoring Adjectives",
        queryResult1.hits, queryResult2.hits);
    indexReader.close();
  }

  @org.junit.Test
  public void collectStatsPositions1() throws IOException {
    // get total number of words
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult = doQuery(indexReader, FIELD_CONTENT, "[]", null,
        null, null);
    indexReader.close();
    int averageNumberOfPositions = Math
        .round(queryResult.hits / queryResult.docs);
    // do position query
    try {
      ArrayList<Integer> fullDocSet = new ArrayList<Integer>(
          Arrays.asList(IntStream.rangeClosed(0, files.size() - 1).boxed()
              .toArray(Integer[]::new)));
      ComponentField fieldStats = new ComponentField(FIELD_CONTENT, FIELD_ID);
      fieldStats.statsPositionList.add(
          new ComponentPosition(FIELD_CONTENT, "total", null, null, "all"));
      fieldStats.statsPositionList.add(new ComponentPosition(FIELD_CONTENT,
          "minimum", (double) (averageNumberOfPositions - 1), null,
          "n,sum,mean,min,max"));
      fieldStats.statsPositionList.add(new ComponentPosition(FIELD_CONTENT,
          "maximum", null, (double) averageNumberOfPositions, "sum"));
      HashMap<String, HashMap<String, Object>> response = doAdvancedSearch(
          fullDocSet, fieldStats);
      HashMap<String, Object> responseTotal = (HashMap<String, Object>) response
          .get("statsPositions").get("total");
      HashMap<String, Object> responseMinimum = (HashMap<String, Object>) response
          .get("statsPositions").get("minimum");
      HashMap<String, Object> responseMaximum = (HashMap<String, Object>) response
          .get("statsPositions").get("maximum");
      Double total = responseTotal != null ? (Double) responseTotal.get("sum")
          : 0;
      Long totalMinimum = responseTotal != null
          ? (Long) responseMinimum.get("sum") : 0;
      Long totalMaximum = responseTotal != null
          ? (Long) responseMaximum.get("sum") : 0;
      assertEquals("Number of positions", new Long(total.longValue()),
          new Long(queryResult.hits));
      assertEquals("Minimum and maximum on number of positions",
          new Long(total.longValue()), new Long(totalMinimum + totalMaximum));
    } catch (mtas.parser.function.ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void collectStatsPositions2() throws IOException {
    ArrayList<Integer> fullDocSet = new ArrayList<Integer>(
        Arrays.asList(IntStream.rangeClosed(0, files.size() - 1).boxed()
            .toArray(Integer[]::new)));
    try {
      // compute total
      ComponentField fieldStats = new ComponentField(FIELD_CONTENT, FIELD_ID);
      fieldStats.statsPositionList.add(new ComponentPosition(FIELD_CONTENT,
          "total", null, null, "n,sum,min,max"));
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
      Long subDocs = Long.valueOf(0), subTotal = Long.valueOf(0),
          subMinimum = null, subMaximum = null;
      ArrayList<Integer> subDocSet = new ArrayList<Integer>();
      for (Integer docId : fullDocSet) {
        subDocSet.add(docId);
        fieldStats = new ComponentField(FIELD_CONTENT, FIELD_ID);
        fieldStats.statsPositionList.add(new ComponentPosition(FIELD_CONTENT,
            "total", null, null, "n,sum,min,max"));
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
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void collectStatsTokens() throws IOException {
    ArrayList<Integer> fullDocSet = new ArrayList<Integer>(
        Arrays.asList(IntStream.rangeClosed(0, files.size() - 1).boxed()
            .toArray(Integer[]::new)));
    try {
      // compute total
      ComponentField fieldStats = new ComponentField(FIELD_CONTENT, FIELD_ID);
      fieldStats.statsTokenList.add(new ComponentToken(FIELD_CONTENT, "total",
          null, null, "n,sum,min,max"));
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
      Long subDocs = Long.valueOf(0), subTotal = Long.valueOf(0),
          subMinimum = null, subMaximum = null;
      ArrayList<Integer> subDocSet = new ArrayList<Integer>();
      for (Integer docId : fullDocSet) {
        subDocSet.add(docId);
        fieldStats = new ComponentField(FIELD_CONTENT, FIELD_ID);
        fieldStats.statsTokenList.add(new ComponentToken(FIELD_CONTENT, "total",
            null, null, "n,sum,min,max"));
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
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void collectStatsSpans() throws IOException {
    String cql1 = "[pos=\"N\"]";
    String cql2 = "[pos=\"LID\"]";
    String cql3 = "[pos=\"N\" | pos=\"LID\"]";
    // get total number of nouns
    IndexReader indexReader = DirectoryReader.open(directory);
    QueryResult queryResult1 = doQuery(indexReader, FIELD_CONTENT, cql1, null,
        null, null);
    QueryResult queryResult2 = doQuery(indexReader, FIELD_CONTENT, cql2, null,
        null, null);
    QueryResult queryResult3 = doQuery(indexReader, FIELD_CONTENT, cql3, null,
        null, null);
    indexReader.close();
    int averageNumberOfPositions = Math
        .round(queryResult1.hits / queryResult1.docs);
    // do stats query for nouns
    try {
      ArrayList<Integer> fullDocSet = new ArrayList<Integer>(
          Arrays.asList(IntStream.rangeClosed(0, files.size() - 1).boxed()
              .toArray(Integer[]::new)));
      ComponentField fieldStats = new ComponentField(FIELD_CONTENT, FIELD_ID);
      MtasSpanQuery q1 = createQuery(FIELD_CONTENT, cql1, null, null);
      MtasSpanQuery q2 = createQuery(FIELD_CONTENT, cql2, null, null);
      MtasSpanQuery q3 = createQuery(FIELD_CONTENT, cql3, null, null);
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
      assertEquals("Number of nouns", new Long(total1.longValue()),
          new Long(queryResult1.hits));
      assertEquals("Number of articles", new Long(total2.longValue()),
          new Long(queryResult2.hits));
      assertEquals("Number of nouns and articles - external 1",
          new Long(total12.longValue()),
          new Long(queryResult1.hits + queryResult2.hits));
      assertEquals("Number of nouns and articles - external 2",
          new Long(total12.longValue()), new Long(queryResult3.hits));
      assertEquals("Number of nouns and articles - internal",
          new Long(total12.longValue()), new Long(total3.longValue()));
      assertEquals("Number of nouns and articles - functions",
          new Long(difference12.longValue()),
          new Long(queryResult1.hits - queryResult2.hits));
      assertEquals("Minimum and maximum on number of positions nouns",
          new Long(total1.longValue()),
          new Long(totalMinimum1 + totalMaximum1));
    } catch (mtas.parser.function.ParseException | ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void collectGroup() throws IOException {
    String cql = "[pos=\"LID\"]";
    try {
      ArrayList<Integer> fullDocSet = new ArrayList<Integer>(
          Arrays.asList(IntStream.rangeClosed(0, files.size() - 1).boxed()
              .toArray(Integer[]::new)));
      ComponentField fieldStats = new ComponentField(FIELD_CONTENT, FIELD_ID);
      MtasSpanQuery q = createQuery(FIELD_CONTENT, cql, null, null);
      fieldStats.spanQueryList.add(q);
      fieldStats.statsSpanList.add(new ComponentSpan(new MtasSpanQuery[]{q}, "total", null,
          null, "sum", null, null, null));
      fieldStats.groupList.add(new ComponentGroup(q, FIELD_CONTENT, cql, "cql",
          null, null, "articles", Integer.MAX_VALUE, "t_lc", null, null, null, null, null, null,
          null, null, null, null, null, null));
      HashMap<String, HashMap<String, Object>> response = doAdvancedSearch(
          fullDocSet, fieldStats);
      ArrayList<HashMap<String,Object>> list = (ArrayList<HashMap<String, Object>>) response.get("group").get("articles");
      DirectoryReader indexReader = DirectoryReader.open(directory);
      IndexSearcher searcher = new IndexSearcher(indexReader);
      int subTotal = 0;
      for(HashMap<String, Object> listItem: list) {
        HashMap<String, HashMap<Integer, HashMap<String, String>[]>> group = (HashMap<String, HashMap<Integer, HashMap<String, String>[]>>) listItem.get("group");
        HashMap<Integer, HashMap<String, String>[]> hitList = group.get("hit");
        HashMap<String, String> hitListItem = hitList.get(0)[0];
        cql = "[pos=\"LID\" & "+hitListItem.get("prefix")+"=\""+hitListItem.get("value")+"\"]";
        QueryResult queryResult = doQuery(indexReader, FIELD_CONTENT, cql, null, null, null);
        assertEquals("number of hits for articles equals to "+hitListItem.get("value"),listItem.get("sum"), Long.valueOf(queryResult.hits));
        subTotal+=queryResult.hits;
      }
      HashMap<String, Object> responseTotal = (HashMap<String, Object>) response
          .get("statsSpans").get("total");
      Long total = responseTotal != null
          ? (Long) responseTotal.get("sum") : 0;
      assertEquals("Total number of articles",total, Long.valueOf(subTotal));
      indexReader.close();
    } catch (ParseException | mtas.parser.function.ParseException e) {
      e.printStackTrace();
    }
  }

  private HashMap<String, HashMap<String, Object>> doAdvancedSearch(
      ArrayList<Integer> fullDocSet, ComponentField fieldStats) {
    HashMap<String, HashMap<String, Object>> response = new HashMap<String, HashMap<String, Object>>();
    IndexReader indexReader;
    try {
      indexReader = DirectoryReader.open(directory);
      IndexSearcher searcher = new IndexSearcher(indexReader);
      ArrayList<Integer> fullDocList = new ArrayList<Integer>();
      CodecUtil.collect(FIELD_CONTENT, searcher, indexReader, fullDocList,
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
        HashMap<String, Object> functions = new HashMap<String, Object>();
        response.get("statsSpansFunctions").put(cs.key, functions);
        for (SubComponentFunction scf : cs.functions) {
          functions.put(scf.key,
              scf.dataCollector.getResult().getData().rewrite(false));
        }
      }
      response.put("group", new HashMap<String, Object>());
      for (ComponentGroup cg : fieldStats.groupList) {
        SortedMap<String, ?> list = cg.dataCollector.getResult().getList();
        ArrayList<HashMap<String, Object>> groupList = new ArrayList<HashMap<String, Object>>();
        for(String key : list.keySet()) {
          HashMap<String, Object> subList = new HashMap<String, Object>();
          StringBuilder newKey = new StringBuilder("");         
          subList.put("group", GroupHit.keyToObject(key, newKey));
          subList.put("key", newKey.toString().trim());                    
          subList.putAll(((MtasDataItem<?,?>) list.get(key)).rewrite(false));          
          groupList.add(subList);
        }
        response.get("group").put(cg.key, groupList);        
      }
      indexReader.close();
    } catch (IOException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return response;
  }

  private static void createIndex(String configFile,
      HashMap<String, String> files) throws IOException {
    // analyzer
    Map<String, String> paramsCharFilterMtas = new HashMap<String, String>();
    paramsCharFilterMtas.put("type", "file");
    Map<String, String> paramsTokenizer = new HashMap<String, String>();
    paramsTokenizer.put("configFile", configFile);
    Analyzer mtasAnalyzer = CustomAnalyzer
        .builder(Paths.get("docker").toAbsolutePath())
        .addCharFilter("mtas", paramsCharFilterMtas)
        .withTokenizer("mtas", paramsTokenizer).build();
    Map<String, Analyzer> analyzerPerField = new HashMap<String, Analyzer>();
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
    for (String title : files.keySet()) {
      addDoc(w, title, files.get(title));
      if (counter > 0) {
        w.commit();
      }
      counter++;
    }
    w.commit();
    // finish
    w.close();
  }

  private static void addDoc(IndexWriter w, String title, String file)
      throws IOException {
    try {
      Document doc = new Document();
      doc.add(new StringField(FIELD_TITLE, title, Field.Store.YES));
      doc.add(new TextField(FIELD_CONTENT, file, Field.Store.YES));
      w.addDocument(doc);
    } catch (Exception e) {
      System.out.println("Couldn't add " + title + " (" + file + ")");
      e.printStackTrace();
    }
  }

  private MtasSpanQuery createQuery(String field, String cql,
      MtasSpanQuery ignore, Integer maximumIgnoreLength) throws ParseException {
    Reader reader = new BufferedReader(new StringReader(cql));
    MtasCQLParser p = new MtasCQLParser(reader);
    return p.parse(field, null, null, ignore, maximumIgnoreLength);
  }

  private QueryResult doQuery(IndexReader indexReader, String field, String cql,
      MtasSpanQuery ignore, Integer maximumIgnoreLength,
      ArrayList<String> prefixes) throws IOException {
    QueryResult queryResult = new QueryResult();
    try {
      MtasSpanQuery q = createQuery(field, cql, ignore, maximumIgnoreLength);
      queryResult.query = q.toString(field);
      ListIterator<LeafReaderContext> iterator = indexReader.leaves()
          .listIterator();
      IndexSearcher searcher = new IndexSearcher(indexReader);
      SpanWeight spanweight = ((MtasSpanQuery) q.rewrite(indexReader))
          .createWeight(searcher, false);

      while (iterator.hasNext()) {
        LeafReaderContext lrc = iterator.next();
        Spans spans = spanweight.getSpans(lrc, SpanWeight.Postings.POSITIONS);
        SegmentReader r = (SegmentReader) lrc.reader();
        Terms t = r.terms(field);
        CodecInfo mtasCodecInfo = CodecInfo.getCodecInfoFromTerms(t);
        if (spans != null) {
          while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
            queryResult.docs++;
            while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
              queryResult.hits++;
              if (prefixes != null && prefixes.size() > 0) {
                ArrayList<MtasTreeHit<String>> terms = mtasCodecInfo
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
    } catch (mtas.parser.cql.ParseException e) {
      e.printStackTrace();
    }
    return queryResult;
  }

  private void testNumberOfHits(IndexReader indexReader, String field,
      List<String> cqls1, List<String> cqls2) throws IOException {
    Integer sum1 = 0, sum2 = 0;
    QueryResult queryResult;
    for (String cql1 : cqls1) {
      queryResult = doQuery(indexReader, field, cql1, null, null, null);
      sum1 += queryResult.hits;
    }
    for (String cql2 : cqls2) {
      queryResult = doQuery(indexReader, field, cql2, null, null, null);
      sum2 += queryResult.hits;
    }
    assertEquals(sum1, sum2);
  }

  public class QueryResult {

    public String query;
    public int docs;
    public int hits;
    public List<QueryHit> resultList;

    public QueryResult() {
      docs = 0;
      hits = 0;
      resultList = new ArrayList<QueryHit>();
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append(docs + " document(s), ");
      buffer.append(hits + " hit(s)");
      return buffer.toString();
    }

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

  }

  public class QueryHit {
    int docId, startposition, endPosition;
    String prefix, value;

    public QueryHit(int docId, int startPosition, int endPosition,
        String prefix, String value) {
      this.docId = docId;
      this.startposition = startPosition;
      this.endPosition = endPosition;
      this.prefix = prefix;
      this.value = value;
    }
  }

}
