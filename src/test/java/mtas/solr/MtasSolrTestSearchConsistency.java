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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrRequest;
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
      Path dataPath = Paths.get("src" + File.separator + "test"
          + File.separator + "resources" + File.separator + "data");
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
   * @param dataPath
   *          the data path
   * @param collections
   *          the collections
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @param collectionName
   *          the collection name
   * @param dataPath
   *          the data path
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @org.junit.AfterClass
  public static void shutdown() throws IOException {
    server.close();
    MtasSolrBase.deleteDirectory(solrPath.toFile());
  }

  /**
   * Cql query parser.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
    params.set("mtas.stats.spans.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.stats.spans.0.key", "statsKey");
    params.set("mtas.stats.spans.0.query.0.type", "cql");
    params.set("mtas.stats.spans.0.query.0.value", "[]");
    params.set("mtas.stats.spans.0.type", "n,sum,mean");
    params.set("mtas.stats.positions", "true");
    params.set("mtas.stats.positions.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.stats.positions.0.key", "statsKey");
    params.set("mtas.stats.positions.0.type", "n,sum,mean");
    params.set("rows", "0");
    SolrRequest<?> request = new QueryRequest(params);
    //request1.setContentWriter(contentWriter)
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
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
    params.set("mtas.stats.tokens.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.stats.tokens.0.key", "statsKey");
    params.set("mtas.stats.tokens.0.type", String.join(",", types));
    params.set("mtas.stats.tokens.0.minimum", 1);
    params.set("mtas.stats.tokens.0.maximum", 1000000);
    SolrRequest<?> request = new QueryRequest(params);
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
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerTermvector1() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("rows", 0);
    params.set("mtas", "true");
    params.set("mtas.termvector", "true");
    params.set("mtas.termvector.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.termvector.0.prefix", "t_lc");
    params.set("mtas.termvector.0.key", "tv");
    params.set("mtas.termvector.0.sort.type", "sum");
    params.set("mtas.termvector.0.sort.direction", "asc");
    params.set("mtas.termvector.0.type", "n,sum");
    // create full
    params.set("mtas.termvector.0.full", true);
    params.set("mtas.termvector.0.number", -1);
    SolrRequest<?> requestFull = new QueryRequest(params);
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
      SolrRequest<?> request = new QueryRequest(params);
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
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
    params.set("mtas.termvector.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.termvector.0.prefix", "t_lc");
    params.set("mtas.termvector.0.key", "tv");
    params.set("mtas.termvector.0.type", String.join(",", types));
    // create full
    params.set("mtas.termvector.0.list", String.join(",", list));
    SolrRequest<?> request = new QueryRequest(params);
    NamedList<Object> response;
    try {
      response = server.request(request, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    List<NamedList<Object>> tv = MtasSolrBase.getFromMtasTermvector(response,
        "tv");
    for (String key : list) {
      params.clear();
      params.set("q", "*:*");
      params.set("rows", 0);
      params.set("mtas", "true");
      params.set("mtas.stats", "true");
      params.set("mtas.stats.spans", "true");
      params.set("mtas.stats.spans.0.field", MtasSolrBase.FIELD_MTAS);
      params.set("mtas.stats.spans.0.key", "statsKey0");
      params.set("mtas.stats.spans.0.minimum", 1);
      params.set("mtas.stats.spans.0.query.0.type", "cql");
      params.set("mtas.stats.spans.0.query.0.value", "[t_lc=\"" + key + "\"]");
      params.set("mtas.stats.spans.0.type", String.join(",", types));
      params.set("mtas.stats.spans.1.field", MtasSolrBase.FIELD_MTAS);
      params.set("mtas.stats.spans.1.key", "statsKey1");
      params.set("mtas.stats.spans.1.minimum", 0);
      params.set("mtas.stats.spans.1.query.0.type", "cql");
      params.set("mtas.stats.spans.1.query.0.value", "[t_lc=\"" + key + "\"]");
      params.set("mtas.stats.spans.1.type", String.join(",", types));
      params.set("rows", "0");
      SolrRequest<?> requestStats = new QueryRequest(params);
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
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerTermvector3() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("rows", 0);
    params.set("mtas", "true");
    params.set("mtas.termvector", "true");
    params.set("mtas.termvector.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.termvector.0.prefix", "t_lc");
    params.set("mtas.termvector.0.key", "tv");
    params.set("mtas.termvector.0.regexp", "een[a-z]*");
    params.set("mtas.termvector.0.ignoreRegexp", ".*d");
    params.set("mtas.termvector.0.list", ".*g,.*l");
    params.set("mtas.termvector.0.listRegexp", true);
    params.set("mtas.termvector.0.ignoreList", ".*st.*,.*nm.*");
    params.set("mtas.termvector.0.ignoreListRegexp", true);
    SolrRequest<?> request = new QueryRequest(params);
    NamedList<Object> response;
    try {
      response = server.request(request, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    List<NamedList<Object>> tv = MtasSolrBase.getFromMtasTermvector(response,
        "tv");
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
   * Mtas request handler collection 1.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerCollection1() throws IOException {
    // create
    ModifiableSolrParams paramsCreate = new ModifiableSolrParams();
    paramsCreate.set("q", "*:*");
    paramsCreate.set("mtas", "true");
    paramsCreate.set("mtas.collection", "true");
    paramsCreate.set("mtas.collection.0.key", "create");
    paramsCreate.set("mtas.collection.0.action", "create");
    paramsCreate.set("mtas.collection.0.id", "idCreate");
    paramsCreate.set("mtas.collection.0.field", "id");
    SolrRequest<?> requestCreate = new QueryRequest(paramsCreate);
    NamedList<Object> responseCreate;
    try {
      responseCreate = server.request(requestCreate, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    long n = MtasSolrBase.getNumFound(responseCreate);
    NamedList<Object> create = MtasSolrBase
        .getFromMtasCollection(responseCreate, "create");
    assertFalse("create - id not found", create == null);
    assertTrue("create - no valid version", create.get("version") != null
        && create.get("version") instanceof String);
    assertTrue("create - no valid size",
        create.get("size") != null && create.get("size") instanceof Number);
    String createVersion = (String) create.get("version");
    Number createSize = (Number) create.get("size");
    assertEquals("number of values", n, createSize.longValue());
    // post
    ModifiableSolrParams paramsPost = new ModifiableSolrParams();
    paramsPost.set("q", "*:*");
    paramsPost.set("mtas", "true");
    paramsPost.set("mtas.collection", "true");
    paramsPost.set("mtas.collection.0.key", "post");
    paramsPost.set("mtas.collection.0.action", "post");
    paramsPost.set("mtas.collection.0.id", "idPost");
    paramsPost.set("mtas.collection.0.post", "[1,2,3,4]");
    SolrRequest<?> requestPost = new QueryRequest(paramsPost);
    NamedList<Object> responsePost;
    try {
      responsePost = server.request(requestPost, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    NamedList<Object> post = MtasSolrBase.getFromMtasCollection(responsePost,
        "post");
    assertFalse("post - id not found", post == null);
    assertTrue("post - no valid version",
        post.get("version") != null && post.get("version") instanceof String);
    assertTrue("post - no valid size",
        post.get("size") != null && post.get("size") instanceof Number);
    String postVersion = (String) post.get("version");
    Number postSize = (Number) post.get("size");
    assertTrue("post - incorrect size", postSize.longValue() == 4);
    // list
    ModifiableSolrParams paramsList = new ModifiableSolrParams();
    paramsList.set("q", "*:*");
    paramsList.set("mtas", "true");
    paramsList.set("mtas.collection", "true");
    paramsList.set("mtas.collection.0.key", "list");
    paramsList.set("mtas.collection.0.action", "list");
    SolrRequest<?> requestList1 = new QueryRequest(paramsList);
    NamedList<Object> responseList1;
    try {
      responseList1 = server.request(requestList1, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // check create
    NamedList<Object> listCreateItem1 = MtasSolrBase
        .getFromMtasCollectionList(responseList1, "list", "idCreate");
    assertFalse("list - create - id not found", listCreateItem1 == null);
    assertTrue("list - create - incorrect version",
        listCreateItem1.get("version") != null
            && listCreateItem1.get("version") instanceof String
            && listCreateItem1.get("version").equals(createVersion));
    assertTrue("list - create - incorrect size",
        listCreateItem1.get("size") != null
            && listCreateItem1.get("size") instanceof Number
            && ((Number) listCreateItem1.get("size")).longValue() == createSize
                .longValue());
    // check post
    NamedList<Object> listPostItem1 = MtasSolrBase
        .getFromMtasCollectionList(responseList1, "list", "idPost");
    assertFalse("list - post - id not found", listPostItem1 == null);
    assertTrue("list - post - incorrect version",
        listPostItem1.get("version") != null
            && listPostItem1.get("version") instanceof String
            && listPostItem1.get("version").equals(postVersion));
    assertTrue("list - post - incorrect size",
        listPostItem1.get("size") != null
            && listPostItem1.get("size") instanceof Number
            && ((Number) listPostItem1.get("size")).longValue() == postSize
                .longValue());
    // check
    ModifiableSolrParams paramsCheck = new ModifiableSolrParams();
    paramsCheck.set("q", "*:*");
    paramsCheck.set("mtas", "true");
    paramsCheck.set("mtas.collection", "true");
    paramsCheck.set("mtas.collection.0.key", "check1");
    paramsCheck.set("mtas.collection.0.action", "check");
    paramsCheck.set("mtas.collection.0.id", "idCreate");
    paramsCheck.set("mtas.collection.1.key", "check2");
    paramsCheck.set("mtas.collection.1.action", "check");
    paramsCheck.set("mtas.collection.1.id", "idPost");
    SolrRequest<?> requestCheck = new QueryRequest(paramsCheck);
    NamedList<Object> responseCheck;
    try {
      responseCheck = server.request(requestCheck, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // check create
    NamedList<Object> check1 = MtasSolrBase.getFromMtasCollection(responseCheck,
        "check1");
    assertFalse("check - create - no response", check1 == null);
    assertTrue("check - create - no valid version",
        check1.get("version") != null
            && check1.get("version") instanceof String);
    assertTrue("check - create - no valid size",
        check1.get("size") != null && check1.get("size") instanceof Number);
    String check1Version = (String) check1.get("version");
    Number check1Size = (Number) check1.get("size");
    assertEquals("check - create - version", check1Version, createVersion);
    assertEquals("check - create - number of values", check1Size.longValue(),
        createSize.longValue());
    // check post
    NamedList<Object> check2 = MtasSolrBase.getFromMtasCollection(responseCheck,
        "check2");
    assertFalse("check - post - no response", check2 == null);
    assertTrue("check - post - no valid version", check2.get("version") != null
        && check2.get("version") instanceof String);
    assertTrue("check - post - no valid size",
        check2.get("size") != null && check2.get("size") instanceof Number);
    String check2Version = (String) check2.get("version");
    Number check2Size = (Number) check2.get("size");
    assertEquals("check - post - version", check2Version, postVersion);
    assertEquals("check - post - number of values", check2Size.longValue(), 4);
    // delete
    ModifiableSolrParams paramsDelete = new ModifiableSolrParams();
    paramsDelete.set("q", "*:*");
    paramsDelete.set("mtas", "true");
    paramsDelete.set("mtas.collection", "true");
    paramsDelete.set("mtas.collection.0.key", "delete1");
    paramsDelete.set("mtas.collection.0.action", "delete");
    paramsDelete.set("mtas.collection.0.id", "idCreate");
    paramsDelete.set("mtas.collection.1.key", "delete2");
    paramsDelete.set("mtas.collection.1.action", "delete");
    paramsDelete.set("mtas.collection.1.id", "idPost");
    SolrRequest<?> requestDelete = new QueryRequest(paramsDelete);
    NamedList<Object> responseDelete;
    try {
      responseDelete = server.request(requestDelete, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // check create
    NamedList<Object> delete1 = MtasSolrBase
        .getFromMtasCollection(responseDelete, "delete1");
    assertFalse("delete - create - no response", delete1 == null);
    // check post
    NamedList<Object> delete2 = MtasSolrBase
        .getFromMtasCollection(responseDelete, "delete2");
    assertFalse("delete - post - no response", delete2 == null);
    // list (again)
    SolrRequest<?> requestList2 = new QueryRequest(paramsList);
    NamedList<Object> responseList2;
    try {
      responseList2 = server.request(requestList2, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // check create
    NamedList<Object> listCreateItem2 = MtasSolrBase
        .getFromMtasCollectionList(responseList2, "list", "idCreate");
    assertTrue("list - create - id found", listCreateItem2 == null);
    // check post
    NamedList<Object> listPostItem2 = MtasSolrBase
        .getFromMtasCollectionList(responseList2, "list", "idPost");
    assertTrue("list - post - id found", listPostItem2 == null);
  }

  /**
   * Mtas request handler collection 2.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerCollection2() throws IOException {
    // post
    ModifiableSolrParams paramsPost = new ModifiableSolrParams();
    paramsPost.set("q", "*:*");
    paramsPost.set("mtas", "true");
    paramsPost.set("mtas.collection", "true");
    paramsPost.set("mtas.collection.0.key", "postKey1");
    paramsPost.set("mtas.collection.0.action", "post");
    paramsPost.set("mtas.collection.0.id", "postSet1");
    paramsPost.set("mtas.collection.0.post", "[1,3,4]");
    paramsPost.set("mtas.collection.1.key", "postKey2");
    paramsPost.set("mtas.collection.1.action", "post");
    paramsPost.set("mtas.collection.1.id", "postSet2");
    paramsPost.set("mtas.collection.1.post", "[2]");
    paramsPost.set("mtas.collection.2.key", "createKey1");
    paramsPost.set("mtas.collection.2.action", "create");
    paramsPost.set("mtas.collection.2.id", "createSet1");
    paramsPost.set("mtas.collection.2.field", MtasSolrBase.FIELD_ID);
    SolrRequest<?> requestPost = new QueryRequest(paramsPost);
    NamedList<Object> responsePost;
    try {
      responsePost = server.request(requestPost, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    MtasSolrBase.getFromMtasCollection(responsePost, "post");
    // query set1
    ModifiableSolrParams paramsSelect1 = new ModifiableSolrParams();
    paramsSelect1.set("q", "{!mtas_join field=\"" + MtasSolrBase.FIELD_ID
        + "\" collection=\"postSet1\"}");
    paramsSelect1.set("rows", "0");
    SolrRequest<?> request1 = new QueryRequest(paramsSelect1);
    NamedList<Object> response1;
    try {
      response1 = server.request(request1, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    long n1 = MtasSolrBase.getNumFound(response1);
    assertTrue("incorrect number of matching documents : " + n1, n1 == 2);
    // query set2
    ModifiableSolrParams paramsSelect2 = new ModifiableSolrParams();
    paramsSelect2.set("q", "{!mtas_join field=\"" + MtasSolrBase.FIELD_ID
        + "\" collection=\"postSet2\"}");
    paramsSelect2.set("rows", "0");
    SolrRequest<?> request2 = new QueryRequest(paramsSelect2);
    NamedList<Object> response2;
    try {
      response2 = server.request(request2, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    long n2 = MtasSolrBase.getNumFound(response2);
    assertTrue("incorrect number of matching documents : " + n2, n2 == 1);
    // query set3
    ModifiableSolrParams paramsSelect3 = new ModifiableSolrParams();
    paramsSelect3.set("q", "{!mtas_join field=\"" + MtasSolrBase.FIELD_ID
        + "\" collection=\"createSet1\"}");
    paramsSelect3.set("rows", "0");
    SolrRequest<?> request3 = new QueryRequest(paramsSelect3);
    NamedList<Object> response3;
    try {
      response3 = server.request(request3, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    long n3 = MtasSolrBase.getNumFound(response3);
    assertTrue("incorrect number of matching documents : " + n3, n3 == 3);
    // query set1 or set2
    ModifiableSolrParams paramsSelect4 = new ModifiableSolrParams();
    paramsSelect4.set("q",
        "({!mtas_join field=\"" + MtasSolrBase.FIELD_ID
            + "\" collection=\"postSet1\"}) OR ({!mtas_join field=\""
            + MtasSolrBase.FIELD_ID + "\" collection=\"postSet2\"})");
    paramsSelect4.set("rows", "0");
    SolrRequest<?> request4 = new QueryRequest(paramsSelect4);
    NamedList<Object> response4;
    try {
      response4 = server.request(request4, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    long n4 = MtasSolrBase.getNumFound(response4);
    assertTrue("incorrect number of matching documents : " + n4, n4 == 3);
  }

  /**
   * Mtas request handler collection 3.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerCollection3() throws IOException {
    // post
    ModifiableSolrParams paramsPost = new ModifiableSolrParams();
    paramsPost.set("q", "*:*");
    paramsPost.set("mtas", "true");
    paramsPost.set("mtas.collection", "true");
    paramsPost.set("mtas.collection.0.key", "setCreatedByPost");
    paramsPost.set("mtas.collection.0.action", "post");
    paramsPost.set("mtas.collection.0.id", "setCreatedByPost");
    paramsPost.set("mtas.collection.0.post", "[1,3,4]");
    SolrRequest<?> requestPost = new QueryRequest(paramsPost);
    try {
      server.request(requestPost, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // import
    ModifiableSolrParams paramsImport = new ModifiableSolrParams();
    paramsImport.set("q", "*:*");
    paramsImport.set("mtas", "true");
    paramsImport.set("mtas.collection", "true");
    paramsImport.set("mtas.collection.0.key", "setCreatedByImport");
    paramsImport.set("mtas.collection.0.action", "post");
    paramsImport.set("mtas.collection.0.id", "setCreatedByImport");
    paramsImport.set("mtas.collection.0.post", "[1,3,4]");
    SolrRequest<?> requestImport = new QueryRequest(paramsImport);
    try {
      server.request(requestImport, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // query post
    ModifiableSolrParams paramsSelect1 = new ModifiableSolrParams();
    paramsSelect1.set("q", "{!mtas_join field=\"" + MtasSolrBase.FIELD_ID
        + "\" collection=\"setCreatedByPost\"}");
    paramsSelect1.set("rows", "0");
    SolrRequest<?> request1 = new QueryRequest(paramsSelect1);
    NamedList<Object> response1;
    try {
      response1 = server.request(request1, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    long n1 = MtasSolrBase.getNumFound(response1);
    assertTrue("no matching documents for posted set: " + n1, n1 > 0);
    // query import
    ModifiableSolrParams paramsSelect2 = new ModifiableSolrParams();
    paramsSelect2.set("q", "{!mtas_join field=\"" + MtasSolrBase.FIELD_ID
        + "\" collection=\"setCreatedByImport\"}");
    paramsSelect2.set("rows", "0");
    SolrRequest<?> request2 = new QueryRequest(paramsSelect2);
    NamedList<Object> response2;
    try {
      response2 = server.request(request2, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    long n2 = MtasSolrBase.getNumFound(response2);
    assertTrue("no matching documents for imported set: " + n2, n2 > 0);
    // compare
    assertTrue("posted set and imported set give different results : " + n1
        + " and " + n2, n1 == n2);
  }

  @org.junit.Test
  public void mtasRequestHandlerGroup1() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    final int maxNumber = 1000;
    String cql = "[pos=\"LID\"]";
    String prefix = "t_lc";
    // get group
    params.set("q", "*:*");
    params.set("rows", 0);
    params.set("mtas", "true");
    params.set("mtas.group", "true");
    params.set("mtas.group.0.key", "groupKey");
    params.set("mtas.group.0.number", maxNumber);
    params.set("mtas.group.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.group.0.query.type", "cql");
    params.set("mtas.group.0.query.value", cql);
    params.set("mtas.group.0.grouping.hit.inside.prefixes", prefix);
    SolrRequest<?> requestGroup = new QueryRequest(params);
    NamedList<Object> responseGroup = null;
    try {
      responseGroup = server.request(requestGroup, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // get stats
    params.clear();
    params.set("q", "*:*");
    params.set("rows", 0);
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.spans", "true");
    params.set("mtas.stats.spans.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.stats.spans.0.key", "statsKey");
    params.set("mtas.stats.spans.0.minimum", 1);
    params.set("mtas.stats.spans.0.query.0.type", "cql");
    params.set("mtas.stats.spans.0.query.0.value", cql);
    params.set("mtas.stats.spans.0.type", "n,sum");
    params.set("rows", "0");
    SolrRequest<?> requestStats = new QueryRequest(params);
    NamedList<Object> responseStats = null;
    try {
      responseStats = server.request(requestStats, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // get group list
    List<NamedList<Object>> groupList = MtasSolrBase
        .getFromMtasGroup(responseGroup, "groupKey");
    long groupSum = 0;
    long statsSum = MtasSolrBase
        .getFromMtasStats(responseStats, "spans", "statsKey", "sum")
        .longValue();
    for (NamedList<Object> groupListItem : groupList) {
      if (groupListItem.get("sum") instanceof Number) {
        groupSum += (Long) groupListItem.get("sum");
      } else {
        throw new IOException("no sum for item in grouping " + groupListItem);
      }
      Object subListRaw = groupListItem.get("group");
      if (subListRaw != null && subListRaw instanceof Map) {
        Object subSubListRaw = ((Map<String, Object>) subListRaw).get("hit");
        if (subSubListRaw != null && subSubListRaw instanceof Map) {
          Object subSubSubListRaw = ((Map<String, Object>) subSubListRaw)
              .get(0);
          if (subSubSubListRaw != null && subSubSubListRaw instanceof List) {
            Object subSubSubSubListRaw = ((List<Object>) subSubSubListRaw)
                .get(0);
            if (subSubSubSubListRaw != null
                && subSubSubSubListRaw instanceof Map) {
              String subKey = (String) ((Map<String, Object>) subSubSubSubListRaw)
                  .get("value");
              String subcql = "[" + prefix + "=\"" + subKey + "\"] within "
                  + cql;
              // get stats
              params.clear();
              params.set("q", "*:*");
              params.set("rows", 0);
              params.set("mtas", "true");
              params.set("mtas.stats", "true");
              params.set("mtas.stats.spans", "true");
              params.set("mtas.stats.spans.0.field", MtasSolrBase.FIELD_MTAS);
              params.set("mtas.stats.spans.0.key", "statsKey");
              params.set("mtas.stats.spans.0.minimum", 1);
              params.set("mtas.stats.spans.0.query.0.type", "cql");
              params.set("mtas.stats.spans.0.query.0.value", subcql);
              params.set("mtas.stats.spans.0.type", "n,sum");
              params.set("rows", "0");
              SolrRequest<?> requestSubStats = new QueryRequest(params);
              NamedList<Object> responseSubStats = null;
              try {
                responseSubStats = server.request(requestSubStats,
                    "collection1");
              } catch (SolrServerException e) {
                throw new IOException(e);
              }
              long subStatsSum = MtasSolrBase.getFromMtasStats(responseSubStats,
                  "spans", "statsKey", "sum").longValue();
              long subGroupSum = (Long) groupListItem.get("sum");
              assertEquals(
                  "Sum of hit " + subKey
                      + " not equal to sum for cql expression " + subcql,
                  subGroupSum, subStatsSum);
            } else {
              throw new IOException("problem parsing response (4)");
            }
          } else {
            throw new IOException("problem parsing response (3)");
          }
        } else {
          throw new IOException("problem parsing response (2)");
        }
      } else {
        throw new IOException("problem parsing response (1)");
      }
    }
    assertEquals("Sum of hits grouping not equal to sum for cql expression",
        groupSum, statsSum);
  }

  @org.junit.Test
  public void mtasRequestHandlerGroup2() throws IOException {
    Map<String, String> params = new HashMap<>();
    String cql;
    // test hit inside
    cql = "[pos=\"LID\"][pos=\"ADJ\"]";
    params.put("hit.inside.prefixes", "t,lemma");
    createGroupTest(cql, params);
    params.clear();
    // test hit left
    cql = "[pos=\"LID\"][pos=\"ADJ\"]";
    params.put("hit.left.0.prefixes", "t");
    params.put("hit.left.0.position", "0-3");
    params.put("hit.left.1.prefixes", "lemma");
    params.put("hit.left.1.position", "2");
    params.put("hit.left.2.prefixes", "pos");
    params.put("hit.left.2.position", "3");
    createGroupTest(cql, params);
    params.clear();
    // test hit insideLeft
    cql = "[pos=\"LID\"][pos=\"ADJ\"]";
    params.put("hit.insideLeft.0.prefixes", "t");
    params.put("hit.insideLeft.0.position", "0-3");
    params.put("hit.insideLeft.1.prefixes", "lemma");
    params.put("hit.insideLeft.1.position", "2");
    params.put("hit.insideLeft.2.prefixes", "pos");
    params.put("hit.insideLeft.2.position", "4-5");
    createGroupTest(cql, params);
    params.clear();
    // test hit right
    cql = "[pos=\"LID\"][pos=\"ADJ\"]";
    params.put("hit.right.0.prefixes", "t");
    params.put("hit.right.0.position", "0-3");
    params.put("hit.right.1.prefixes", "lemma");
    params.put("hit.right.1.position", "2");
    params.put("hit.right.2.prefixes", "pos");
    params.put("hit.right.2.position", "3");
    createGroupTest(cql, params);
    params.clear();
    // test hit insideRight
    cql = "[pos=\"LID\"][pos=\"ADJ\"]";
    params.put("hit.insideRight.0.prefixes", "t");
    params.put("hit.insideRight.0.position", "0-3");
    params.put("hit.insideRight.1.prefixes", "lemma");
    params.put("hit.insideRight.1.position", "2");
    params.put("hit.insideRight.2.prefixes", "pos");
    params.put("hit.insideRight.2.position", "4-5");
    createGroupTest(cql, params);
    params.clear();
    // test left
    cql = "[pos=\"LID\"][pos=\"ADJ\"]";
    params.put("left.0.prefixes", "t");
    params.put("left.0.position", "0-3");
    params.put("left.1.prefixes", "lemma");
    params.put("left.1.position", "2");
    params.put("left.2.prefixes", "pos");
    params.put("left.2.position", "3");
    createGroupTest(cql, params);
    params.clear();
    // test right
    cql = "[pos=\"LID\"][pos=\"ADJ\"]";
    params.put("right.0.prefixes", "t");
    params.put("right.0.position", "0-3");
    params.put("right.1.prefixes", "lemma");
    params.put("right.1.position", "2");
    params.put("right.2.prefixes", "pos");
    params.put("right.2.position", "3");
    createGroupTest(cql, params);
    params.clear();
    // test combi
    cql = "[pos=\"LID\"][pos=\"ADJ\"]";
    params.put("hit.inside.prefixes", "t");
    params.put("hit.insideLeft.0.prefixes", "t_lc");
    params.put("hit.insideLeft.0.position", "0-3");
    params.put("hit.insideRight.0.prefixes", "t_lc");
    params.put("hit.insideRight.0.position", "1");
    params.put("hit.left.0.prefixes", "pos");
    params.put("hit.left.0.position", "0-3");
    params.put("hit.right.0.prefixes", "lemma");
    params.put("hit.right.0.position", "1");
    params.put("left.0.prefixes", "s");
    params.put("left.0.position", "2");
    params.put("right.2.prefixes", "p");
    params.put("right.2.position", "3");
    createGroupTest(cql, params);
    params.clear();
  }

  /**
   * Mtas solr schema pre analyzed parser and field.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
    params.set("mtas.stats.spans.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.stats.spans.0.key", "statsKey");
    params.set("mtas.stats.spans.0.query.0.type", "cql");
    params.set("mtas.stats.spans.0.query.0.value", "[]");
    params.set("mtas.stats.spans.0.type", "n,sum,sumsq");
    params.set("mtas.stats.positions", "true");
    params.set("mtas.stats.positions.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.stats.positions.0.key", "statsKey");
    params.set("mtas.stats.positions.0.type", "n,sum,sumsq");
    params.set("mtas.stats.tokens", "true");
    params.set("mtas.stats.tokens.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.stats.tokens.0.key", "statsKey");
    params.set("mtas.stats.tokens.0.type", "n,sum,sumsq");
    params.set("rows", "0");
    SolrRequest<?> request = new QueryRequest(params);
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
    params.set("mtas.stats.spans.0.field", MtasSolrBase.FIELD_MTAS_ADVANCED);
    params.set("mtas.stats.positions.0.field",
        MtasSolrBase.FIELD_MTAS_ADVANCED);
    params.set("mtas.stats.tokens.0.field", MtasSolrBase.FIELD_MTAS_ADVANCED);
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
   * @param response1
   *          the response 1
   * @param response2
   *          the response 2
   * @param key
   *          the key
   * @param names
   *          the names
   */
  private static void createTermvectorAssertions(NamedList<Object> response1,
      NamedList<Object> response2, String key, String[] names) {
    List<NamedList<Object>> list1 = MtasSolrBase
        .getFromMtasTermvector(response1, key);
    List<NamedList<Object>> list2 = MtasSolrBase
        .getFromMtasTermvector(response2, key);
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

  private static void createGroupTest(String cql, Map<String, String> grouping)
      throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    final int maxNumber = Integer.MAX_VALUE;
    String prefix = "t_lc";
    // get group
    params.set("q", "*:*");
    params.set("rows", 0);
    params.set("mtas", "true");
    params.set("mtas.group", "true");
    params.set("mtas.group.0.key", "groupKey");
    params.set("mtas.group.0.number", maxNumber);
    params.set("mtas.group.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.group.0.query.type", "cql");
    params.set("mtas.group.0.query.value", cql);
    for (Entry<String, String> entry : grouping.entrySet()) {
      params.set("mtas.group.0.grouping." + entry.getKey(), entry.getValue());
    }
    SolrRequest<?> requestGroup = new QueryRequest(params);
    NamedList<Object> responseGroup = null;
    try {
      responseGroup = server.request(requestGroup, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // System.out.println(responseGroup);
    // get stats
    params.clear();
    params.set("q", "*:*");
    params.set("rows", 0);
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.spans", "true");
    params.set("mtas.stats.spans.0.field", MtasSolrBase.FIELD_MTAS);
    params.set("mtas.stats.spans.0.key", "statsKey");
    params.set("mtas.stats.spans.0.minimum", 1);
    params.set("mtas.stats.spans.0.query.0.type", "cql");
    params.set("mtas.stats.spans.0.query.0.value", cql);
    params.set("mtas.stats.spans.0.type", "n,sum");
    params.set("rows", "0");
    SolrRequest<?> requestStats = new QueryRequest(params);
    NamedList<Object> responseStats = null;
    try {
      responseStats = server.request(requestStats, "collection1");
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    // get group list
    List<NamedList<Object>> groupList = MtasSolrBase
        .getFromMtasGroup(responseGroup, "groupKey");
    long groupSum = 0;
    long statsSum = MtasSolrBase
        .getFromMtasStats(responseStats, "spans", "statsKey", "sum")
        .longValue();
    for (NamedList<Object> groupListItem : groupList) {
      if (groupListItem.get("sum") instanceof Number) {
        groupSum += (Long) groupListItem.get("sum");
      } else {
        throw new IOException("no sum for item in grouping " + groupListItem);
      }
    }
    assertEquals("Sum of hits grouping not equal to sum for cql expression "
        + cql + " and grouping " + grouping, groupSum, statsSum);
  }

}
