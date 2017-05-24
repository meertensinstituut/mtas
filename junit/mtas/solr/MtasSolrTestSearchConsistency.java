package mtas.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;

/**
 * The Class MtasSolrTestSearchConsistency.
 */
public class MtasSolrTestSearchConsistency {

  /** The log. */
  private static Log log = LogFactory
      .getLog(MtasSolrTestSearchConsistency.class);

  /** The server. */
  private static EmbeddedSolrServer server;

  /** The solr path. */
  private static Path solrPath;

  /**
   * Setup.
   */
  @org.junit.BeforeClass
  public static void setup() {
    try {

      Path dataPath = Paths.get("junit").resolve("data");
      // data
      Map<Integer, SolrInputDocument> solrDocuments = MtasSolrBase
          .createDocuments(true);

      // create
      ArrayList<String> collections = new ArrayList<>(
          Arrays.asList("collection1", "collection2", "collection3"));
      initializeDirectory(dataPath, collections);
      CoreContainer container = new CoreContainer(
          solrPath.toAbsolutePath().toString());
      container.load();
      server = new EmbeddedSolrServer(container, collections.get(0));

      // add
      server.add("collection1", solrDocuments.get(1));
      server.commit("collection1");
      server.add("collection1", solrDocuments.get(2));
      server.add("collection1", solrDocuments.get(3));
      server.commit("collection1");

      server.add("collection2", solrDocuments.get(1));
      server.commit("collection2");

      server.add("collection3", solrDocuments.get(3));
      server.add("collection3", solrDocuments.get(2));
      server.commit("collection3");

    } catch (IOException | SolrServerException e) {
      log.error(e);
    }
  }

  /**
   * Initialize directory.
   *
   * @param dataPath the data path
   * @param collections the collections
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void initializeDirectory(Path dataPath,
      List<String> collections) throws IOException {
    solrPath = Files.createTempDirectory("junitSolr");
    // create and fill
    Files.copy(dataPath.resolve("conf").resolve("solr.xml"),
        solrPath.resolve("solr.xml"));
    // create collection(s)
    for (String collectionName : collections) {
      createCollection(collectionName, dataPath);
    }
  }

  /**
   * Creates the collection.
   *
   * @param collectionName the collection name
   * @param dataPath the data path
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createCollection(String collectionName, Path dataPath)
      throws IOException {
    File solrFile;
    BufferedWriter writer;
    // create directories
    if (solrPath.resolve(collectionName).toFile().mkdir()
        && solrPath.resolve(collectionName).resolve("conf").toFile().mkdir()
        && solrPath.resolve(collectionName).resolve("data").toFile().mkdir()) {
      // copy files
      Files.copy(dataPath.resolve("conf").resolve("solrconfig.xml"), solrPath
          .resolve(collectionName).resolve("conf").resolve("solrconfig.xml"));
      Files.copy(dataPath.resolve("conf").resolve("schema.xml"), solrPath
          .resolve(collectionName).resolve("conf").resolve("schema.xml"));
      Files.copy(dataPath.resolve("conf").resolve("folia.xml"), solrPath
          .resolve(collectionName).resolve("conf").resolve("folia.xml"));
      Files.copy(dataPath.resolve("conf").resolve("mtas.xml"),
          solrPath.resolve(collectionName).resolve("conf").resolve("mtas.xml"));
      // create core.properties
      solrFile = solrPath.resolve(collectionName).resolve("core.properties")
          .toFile();
      writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(solrFile), StandardCharsets.UTF_8));
      try {
        writer.write("name=" + collectionName + "\n");
      } finally {
        writer.close();
      }
    } else {
      throw new IOException("couldn't make directories");
    }
  }

  /**
   * Shutdown.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.AfterClass
  public static void shutdown() throws IOException {
    server.close();
    MtasSolrBase.deleteDirectory(solrPath.toFile());
  }

  /**
   * Cql query parser.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void cqlQueryParser() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q",
        "{!mtas_cql field=\"mtas\" query=\"[pos=\\\"ADJ\\\"]{2}[pos=\\\"N\\\"]\"}");
    try {
      QueryResponse qResp1 = server.query("collection1", params);
      QueryResponse qResp2 = server.query("collection2", params);
      QueryResponse qResp3 = server.query("collection3", params);
      SolrDocumentList docList1 = qResp1.getResults();
      SolrDocumentList docList2 = qResp2.getResults();
      SolrDocumentList docList3 = qResp3.getResults();
      assertFalse(docList1.isEmpty());
      assertEquals(docList1.size(), (long) docList2.size() + docList3.size());
    } catch (SolrServerException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * Cql query parser filter.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void cqlQueryParserFilter() throws IOException {
    ModifiableSolrParams params1 = new ModifiableSolrParams();
    ModifiableSolrParams params2 = new ModifiableSolrParams();
    params1.set("q",
        "{!mtas_cql field=\"mtas\" query=\"[pos=\\\"ADJ\\\"]{2}[pos=\\\"N\\\"]\"}");
    params2.set("q", "*");
    params2.set("fq",
        "{!mtas_cql field=\"mtas\" query=\"[pos=\\\"ADJ\\\"]{2}[pos=\\\"N\\\"]\"}");
    try {
      QueryResponse qResp1 = server.query("collection1", params1);
      QueryResponse qResp2 = server.query("collection1", params2);
      SolrDocumentList docList1 = qResp1.getResults();
      SolrDocumentList docList2 = qResp2.getResults();
      assertFalse(docList1.isEmpty());
      assertEquals(docList1.size(), docList2.size());
    } catch (SolrServerException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * Mtas request handler stats spans and positions.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerStatsSpansAndPositions() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("stats", true);
    params.set("stats.field", "numberOfPositions");
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.spans", "true");
    params.set("mtas.stats.spans.0.field", "mtas");
    params.set("mtas.stats.spans.0.key", "statsKey");
    params.set("mtas.stats.spans.0.query.0.type", "cql");
    params.set("mtas.stats.spans.0.query.0.value", "[]");
    params.set("mtas.stats.spans.0.type", "n,sum,mean");
    params.set("mtas.stats.positions", "true");
    params.set("mtas.stats.positions.0.field", "mtas");
    params.set("mtas.stats.positions.0.key", "statsKey");
    params.set("mtas.stats.positions.0.type", "n,sum,mean");
    params.set("rows", "0");
    SolrRequest<?> request = new QueryRequest(params, METHOD.POST);
    NamedList<Object> response;
    try {
      response = server.request(request, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    assertEquals(
        MtasSolrBase.getFromMtasStats(response, "spans", "statsKey", "sum"),
        MtasSolrBase.getFromMtasStats(response, "positions", "statsKey",
            "sum"));
    assertEquals("number of positions",
        MtasSolrBase.getFromMtasStats(response, "positions", "statsKey", "sum"),
        MtasSolrBase.getFromStats(response, "numberOfPositions", "sum", true));

  }

  /**
   * Mtas request handler stats tokens.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerStatsTokens() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String[] types = new String[] { "n", "sum", "mean", "min", "max" };
    params.set("q", "*:*");
    params.set("rows", 10);
    params.set("stats", true);
    params.set("stats.field", "numberOfTokens");
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.tokens", "true");
    params.set("mtas.stats.tokens.0.field", "mtas");
    params.set("mtas.stats.tokens.0.key", "statsKey");
    params.set("mtas.stats.tokens.0.type", String.join(",", types));
    params.set("mtas.stats.tokens.0.minimum", 1);
    params.set("mtas.stats.tokens.0.maximum", 1000000);
    SolrRequest<?> request = new QueryRequest(params, METHOD.POST);
    NamedList<Object> response;
    try {
      response = server.request(request, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    assertEquals("number of tokens",
        MtasSolrBase.getFromMtasStats(response, "tokens", "statsKey", "sum"),
        MtasSolrBase.getFromStats(response, "numberOfTokens", "sum", true));
  }

  /**
   * Mtas request handler termvector 1.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerTermvector1() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("rows", 0);
    params.set("mtas", "true");
    params.set("mtas.termvector", "true");
    params.set("mtas.termvector.0.field", "mtas");
    params.set("mtas.termvector.0.prefix", "t_lc");
    params.set("mtas.termvector.0.key", "tv");
    params.set("mtas.termvector.0.sort.type", "sum");
    params.set("mtas.termvector.0.sort.direction", "asc");
    params.set("mtas.termvector.0.type", "n,sum");
    // create full
    params.set("mtas.termvector.0.full", true);
    params.set("mtas.termvector.0.number", -1);
    SolrRequest<?> requestFull = new QueryRequest(params, METHOD.POST);
    NamedList<Object> responseFull;
    try {
      responseFull = server.request(requestFull, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    params.remove("mtas.termvector.0.full");
    params.remove("mtas.termvector.0.number");
    // create tests
    for (int number = 1; number <= 1000; number *= 10) {
      params.set("mtas.termvector.0.number", number);
      SolrRequest<?> request = new QueryRequest(params, METHOD.POST);
      NamedList<Object> response;
      try {
        response = server.request(request, "collection1");
      } catch (SolrServerException e) {
        throw new IOException(e);
      }
      createTermvectorAssertions(response, responseFull, "tv",
          new String[] { "n", "sum" });
      params.remove("mtas.termvector.0.number");
    }
  }

  /**
   * Mtas request handler termvector 2.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerTermvector2() throws IOException {
    String[] list = new String[] { "de", "het", "een",
        "not existing word just for testing" };
    String[] types = new String[] { "n", "sum", "sumsq" };
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("rows", 0);
    params.set("mtas", "true");
    params.set("mtas.termvector", "true");
    params.set("mtas.termvector.0.field", "mtas");
    params.set("mtas.termvector.0.prefix", "t_lc");
    params.set("mtas.termvector.0.key", "tv");
    params.set("mtas.termvector.0.type", String.join(",", types));
    // create full
    params.set("mtas.termvector.0.list", String.join(",", list));
    SolrRequest<?> request = new QueryRequest(params, METHOD.POST);
    NamedList<Object> response;
    try {
      response = server.request(request, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    List<NamedList> tv = MtasSolrBase.getFromMtasTermvector(response, "tv");
    for (String key : list) {
      params.clear();
      params.set("q", "*:*");
      params.set("rows", 0);
      params.set("mtas", "true");
      params.set("mtas.stats", "true");
      params.set("mtas.stats.spans", "true");
      params.set("mtas.stats.spans.0.field", "mtas");
      params.set("mtas.stats.spans.0.key", "statsKey0");
      params.set("mtas.stats.spans.0.minimum", 1);
      params.set("mtas.stats.spans.0.query.0.type", "cql");
      params.set("mtas.stats.spans.0.query.0.value", "[t_lc=\"" + key + "\"]");
      params.set("mtas.stats.spans.0.type", String.join(",", types));
      params.set("mtas.stats.spans.1.field", "mtas");
      params.set("mtas.stats.spans.1.key", "statsKey1");
      params.set("mtas.stats.spans.1.minimum", 0);
      params.set("mtas.stats.spans.1.query.0.type", "cql");
      params.set("mtas.stats.spans.1.query.0.value", "[t_lc=\"" + key + "\"]");
      params.set("mtas.stats.spans.1.type", String.join(",", types));
      params.set("rows", "0");
      SolrRequest<?> requestStats = new QueryRequest(params, METHOD.POST);
      NamedList<Object> responseStats;
      try {
        responseStats = server.request(requestStats, "collection1");
      } catch (SolrServerException e) {
        throw new IOException(e);
      }
      NamedList<Object> tvItem = null;
      for (NamedList<Object> item : tv) {
        if (item.get("key").equals(key)) {
          tvItem = item;
          break;
        }
      }
      if (tvItem == null) {
        Object itemSum = MtasSolrBase.getFromMtasStats(responseStats, "spans",
            "statsKey1", "sum");
        assertFalse("No item in tv list for " + key + " but stats found",
            itemSum == null || ((Number) itemSum).longValue() != 0);
      } else {
        for (String type : types) {
          Object itemValue = MtasSolrBase.getFromMtasStats(responseStats,
              "spans", "statsKey0", type);
          assertEquals("different " + type + " for " + key + ": " + itemValue
              + " and " + tvItem.get(type), itemValue, tvItem.get(type));
        }
      }
    }

  }

  /**
   * Mtas request handler termvector 3.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerTermvector3() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("rows", 0);
    params.set("mtas", "true");
    params.set("mtas.termvector", "true");
    params.set("mtas.termvector.0.field", "mtas");
    params.set("mtas.termvector.0.prefix", "t_lc");
    params.set("mtas.termvector.0.key", "tv");
    params.set("mtas.termvector.0.regexp", "een[a-z]*");
    params.set("mtas.termvector.0.ignoreRegexp", ".*d");
    params.set("mtas.termvector.0.list", ".*g,.*l");
    params.set("mtas.termvector.0.listRegexp", true);
    params.set("mtas.termvector.0.ignoreList", ".*st.*,.*nm.*");
    params.set("mtas.termvector.0.ignoreListRegexp", true);
    SolrRequest<?> request = new QueryRequest(params, METHOD.POST);
    NamedList<Object> response;
    try {
      response = server.request(request, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    List<NamedList> tv = MtasSolrBase.getFromMtasTermvector(response, "tv");
    Set<String> keys = new HashSet<>();
    for (NamedList<Object> item : tv) {
      if (item != null && item.get("key") != null
          && item.get("key") instanceof String) {
        keys.add((String) item.get("key"));
      }
    }
    // checks
    assertFalse("no keys matching", keys.isEmpty());
    for (String key : keys) {
      assertTrue(key + " not matching regexp", key.matches("een[a-z]*"));
      assertFalse(key + " matching ignoreRegexp", key.matches(".*d"));
      assertTrue(key + " not matching list regexps", key.matches("(.*g|.*l)"));
      assertFalse(key + " matching ignoreList regexps",
          key.matches("(.*st.*|.*nm.*)"));
    }
  }

  /**
   * Mtas solr schema pre analyzed parser and field.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasSolrSchemaPreAnalyzedParserAndField() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("stats", "true");
    params.set("stats.field", "numberOfPositions");
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.spans", "true");
    params.set("mtas.stats.spans.0.field", "mtas");
    params.set("mtas.stats.spans.0.key", "statsKey");
    params.set("mtas.stats.spans.0.query.0.type", "cql");
    params.set("mtas.stats.spans.0.query.0.value", "[]");
    params.set("mtas.stats.spans.0.type", "n,sum,sumsq");
    params.set("mtas.stats.positions", "true");
    params.set("mtas.stats.positions.0.field", "mtas");
    params.set("mtas.stats.positions.0.key", "statsKey");
    params.set("mtas.stats.positions.0.type", "n,sum,sumsq");
    params.set("mtas.stats.tokens", "true");
    params.set("mtas.stats.tokens.0.field", "mtas");
    params.set("mtas.stats.tokens.0.key", "statsKey");
    params.set("mtas.stats.tokens.0.type", "n,sum,sumsq");
    params.set("rows", "0");
    SolrRequest<?> request = new QueryRequest(params, METHOD.POST);
    NamedList<Object> response1;
    NamedList<Object> response2;
    try {
      response1 = server.request(request, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    params.remove("mtas.stats.spans.0.field");
    params.remove("mtas.stats.positions.0.field");
    params.remove("mtas.stats.tokens.0.field");
    params.set("mtas.stats.spans.0.field", "mtasAdvanced");
    params.set("mtas.stats.positions.0.field", "mtasAdvanced");
    params.set("mtas.stats.tokens.0.field", "mtasAdvanced");
    try {
      response2 = server.request(request, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    assertEquals(
        MtasSolrBase.getFromMtasStats(response1, "spans", "statsKey", "sum"),
        MtasSolrBase.getFromStats(response1, "numberOfPositions", "sum", true));
    for (String type : new String[] { "spans", "tokens", "positions" }) {
      for (String stats : new String[] { "sum", "n", "sumsq" }) {
        assertEquals(
            MtasSolrBase.getFromMtasStats(response1, type, "statsKey", stats),
            MtasSolrBase.getFromMtasStats(response2, type, "statsKey", stats));
      }
    }
  }

  /**
   * Creates the termvector assertions.
   *
   * @param response1 the response 1
   * @param response2 the response 2
   * @param key the key
   * @param names the names
   */
  private static void createTermvectorAssertions(NamedList<Object> response1,
      NamedList<Object> response2, String key, String[] names) {
    List<NamedList> list1 = MtasSolrBase.getFromMtasTermvector(response1, key);
    List<NamedList> list2 = MtasSolrBase.getFromMtasTermvector(response2, key);
    assertFalse("list should be defined", list1 == null || list2 == null);
    if (list1 != null && list2 != null) {
      assertFalse("first list should not be longer",
          list1.size() > list2.size());
      assertFalse("list should not be empty", list1.isEmpty());
      for (int i = 0; i < list1.size(); i++) {
        Object key1 = list1.get(i).get("key");
        Object key2 = list2.get(i).get("key");
        assertFalse("key should be provided", (key1 == null) || (key2 == null));
        assertTrue("key should be string",
            (key1 instanceof String) && (key2 instanceof String));
        assertEquals(
            "element " + i + " should be equal: " + key1 + " - " + key2, key1,
            key2);
        for (int j = 0; j < names.length; j++) {
          Object value1 = list1.get(i).get(names[j]);
          Object value2 = list2.get(i).get(names[j]);
          assertFalse(names[j] + " should be provided",
              (value1 == null) || (value2 == null));
          assertEquals(
              names[j] + " should be equal: " + value1 + " - " + value2, value1,
              value2);
        }
      }
    }
  }
}
