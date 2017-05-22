package mtas.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import com.google.common.io.Files;

/**
 * The Class MtasSolrTestDistributedSearchConsistency.
 */
public class MtasSolrTestDistributedSearchConsistency {

  /** The log. */
  private static Log log = LogFactory
      .getLog(MtasSolrTestDistributedSearchConsistency.class);

  /** The Constant COLLECTION_ALL_OPTIMIZED. */
  private static final String COLLECTION_ALL_OPTIMIZED = "collection1";

  /** The Constant COLLECTION_ALL_MULTIPLE_SEGMENTS. */
  private static final String COLLECTION_ALL_MULTIPLE_SEGMENTS = "collection2";

  /** The Constant COLLECTION_PART1_OPTIMIZED. */
  private static final String COLLECTION_PART1_OPTIMIZED = "collection3";

  /** The Constant COLLECTION_PART2_MULTIPLE_SEGMENTS. */
  private static final String COLLECTION_PART2_MULTIPLE_SEGMENTS = "collection4";

  /** The Constant COLLECTION_DISTRIBUTED. */
  private static final String COLLECTION_DISTRIBUTED = "collection5";

  /** The cloud cluster. */
  private static MiniSolrCloudCluster cloudCluster;

  /** The cloud base dir. */
  private static Path cloudBaseDir;

  /** The solr documents. */
  private static Map<Integer, SolrInputDocument> solrDocuments;

  /**
   * Setup.
   */
  @org.junit.BeforeClass
  public static void setup() {
    solrDocuments = MtasSolrBase.createDocuments(false);
    createCloud();
  }

  /**
   * Shutdown.
   */
  @org.junit.AfterClass
  public static void shutdown() {
    shutdownCloud();
  }

  /**
   * Cql query parser.
   */
  @org.junit.Test
  public void cqlQueryParser() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q",
        "{!mtas_cql field=\"mtas\" query=\"[pos=\\\"ADJ\\\"]{2}[pos=\\\"N\\\"]\"}");
    Map<String, QueryResponse> list = createResults(params, null);
    assertEquals(list.get(COLLECTION_ALL_OPTIMIZED).getResults().size(),
        list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResults().size());
    assertEquals(list.get(COLLECTION_ALL_OPTIMIZED).getResults().size(),
        list.get(COLLECTION_PART1_OPTIMIZED).getResults().size(),
        list.get(COLLECTION_PART2_MULTIPLE_SEGMENTS).getResults().size());
    assertEquals(list.get(COLLECTION_ALL_OPTIMIZED).getResults().size(),
        list.get(COLLECTION_DISTRIBUTED).getResults().size());
  }

  /**
   * Cql query parser filter.
   */
  @org.junit.Test
  public void cqlQueryParserFilter() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("fq",
        "{!mtas_cql field=\"mtas\" query=\"[pos=\\\"ADJ\\\"]{2}[pos=\\\"N\\\"]\"}");
    Map<String, QueryResponse> list = createResults(params, null);
    assertEquals(list.get(COLLECTION_ALL_OPTIMIZED).getResults().size(),
        list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResults().size());
    assertEquals(list.get(COLLECTION_ALL_OPTIMIZED).getResults().size(),
        list.get(COLLECTION_PART1_OPTIMIZED).getResults().size(),
        list.get(COLLECTION_PART2_MULTIPLE_SEGMENTS).getResults().size());
    assertEquals(list.get(COLLECTION_ALL_OPTIMIZED).getResults().size(),
        list.get(COLLECTION_DISTRIBUTED).getResults().size());
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
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.tokens", "true");
    params.set("mtas.stats.tokens.0.field", "mtas");
    params.set("mtas.stats.tokens.0.key", "statsKey");
    params.set("mtas.stats.tokens.0.type", String.join(",", types));
    Map<String, QueryResponse> list = createResults(params, null);
    createStatsAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
        list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResponse(), "tokens",
        "statsKey", types);
  }

  /**
   * Mtas request handler stats positions.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerStatsPositions() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String[] types = new String[] { "n", "sum", "mean", "min", "max" };
    params.set("q", "*:*");
    params.set("rows", 10);
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.positions", "true");
    params.set("mtas.stats.positions.0.field", "mtas");
    params.set("mtas.stats.positions.0.key", "statsKey");
    params.set("mtas.stats.positions.0.type", String.join(",", types));
    Map<String, QueryResponse> list = createResults(params, null);
    createStatsAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
        list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResponse(), "positions",
        "statsKey", types);
  }

  /**
   * Mtas request handler stats spans.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerStatsSpans() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String[] types = new String[] { "n", "sum", "mean", "min", "max" };
    params.set("q", "*:*");
    params.set("rows", 10);
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.spans", "true");
    params.set("mtas.stats.spans.0.field", "mtas");
    params.set("mtas.stats.spans.0.key", "statsKey");
    params.set("mtas.stats.spans.0.query.0.type", "cql");
    params.set("mtas.stats.spans.0.query.0.value", "[pos=$pos]");
    params.set("mtas.stats.spans.0.query.0.variable.0.name", "pos");
    params.set("mtas.stats.spans.0.query.0.variable.0.value", "LID,N");
    params.set("mtas.stats.spans.0.type", String.join(",", types));
    Map<String, QueryResponse> list = createResults(params, null);
    createStatsAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
        list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResponse(), "spans",
        "statsKey", types);
  }

  @org.junit.Test
  public void mtasRequestHandlerTermvector1() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String[] types = new String[] { "n", "sum", "sumsq", "min", "max" };
    String[] collections = new String[] { COLLECTION_ALL_OPTIMIZED,
        COLLECTION_ALL_MULTIPLE_SEGMENTS, COLLECTION_DISTRIBUTED };
    params.set("q", "*:*");
    params.set("rows", 10);
    params.set("mtas", "true");
    params.set("mtas.termvector", "true");
    params.set("mtas.termvector.0.field", "mtas");
    params.set("mtas.termvector.0.prefix", "t_lc");
    params.set("mtas.termvector.0.regexp", "[a-z0-9]+");
    params.set("mtas.termvector.0.key", "tv");
    params.set("mtas.termvector.0.sort.type", "term");
    params.set("mtas.termvector.0.sort.direction", "asc");
    params.set("mtas.termvector.0.type", String.join(",", types));
    params.set("mtas.termvector.0.number", -1);
    params.set("mtas.termvector.0.full", true);
    Map<String, QueryResponse> list = createResults(params,
        Arrays.asList(collections));
    createTermvectorAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
        list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResponse(), "tv",
        new String[] { "n", "sum" });
    createTermvectorAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
        list.get(COLLECTION_DISTRIBUTED).getResponse(), "tv",
        new String[] { "n", "sum" });
    for (Entry<String, QueryResponse> entry : list.entrySet()) {
      List<NamedList> tv = MtasSolrBase
          .getFromMtasTermvector(entry.getValue().getResponse(), "tv");
      for (NamedList<Object> item : tv) {
        String key = item.get("key").toString();
        params.clear();
        params.set("q", "*:*");
        params.set("rows", 0);
        params.set("mtas", "true");
        params.set("mtas.stats", "true");
        params.set("mtas.stats.spans", "true");
        params.set("mtas.stats.spans.0.field", "mtas");
        params.set("mtas.stats.spans.0.key", "statsKey");
        params.set("mtas.stats.spans.0.minimum", 1);
        params.set("mtas.stats.spans.0.query.0.type", "cql");
        params.set("mtas.stats.spans.0.query.0.value",
            "[t_lc=\"" + key + "\"]");
        params.set("mtas.stats.spans.0.type", String.join(",", types));
        params.set("rows", "0");
        Map<String, QueryResponse> itemList = createResults(params,
            Arrays.asList(new String[] { entry.getKey() }));
        for (String type : types) {
          Object itemValue = MtasSolrBase.getFromMtasStats(
              itemList.get(entry.getKey()).getResponse(), "spans", "statsKey",
              type);
          assertTrue(
              "for " + entry.getKey() + ", stats for '" + key + "' - " + type
                  + " are wrong : " + item.get(type) + " and " + itemValue,
              item.get(type).equals(itemValue));
        }
      }

    }

  }

  @org.junit.Test
  public void mtasRequestHandlerTermvector2() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String[] types = new String[] { "n", "sum", "sumsq", "min", "max" };
    String[] collections = new String[] { COLLECTION_ALL_OPTIMIZED,
        COLLECTION_ALL_MULTIPLE_SEGMENTS, COLLECTION_DISTRIBUTED };
    for (String sort : new String[] { "term", "sum" }) {
      for (String direction : new String[] { "asc", "desc" }) {
        for (int number : new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20,
            100, 1000 }) {
          params.clear();
          params.set("q", "*:*");
          params.set("rows", 10);
          params.set("mtas", "true");
          params.set("mtas.termvector", "true");
          params.set("mtas.termvector.0.field", "mtas");
          params.set("mtas.termvector.0.prefix", "t_lc");
          params.set("mtas.termvector.0.key", "tv");
          params.set("mtas.termvector.0.sort.type", sort);
          params.set("mtas.termvector.0.sort.direction", direction);
          params.set("mtas.termvector.0.type", String.join(",", types));
          params.set("mtas.termvector.0.number", number);
          Map<String, QueryResponse> list1 = createResults(params,
              Arrays.asList(collections));
          params.set("mtas.termvector.0.full", true);
          Map<String, QueryResponse> list2 = createResults(params,
              Arrays.asList(collections));
          createTermvectorAssertions(
              list1.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
              list1.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResponse(), "tv",
              types);
          createTermvectorAssertions(
              list1.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
              list1.get(COLLECTION_DISTRIBUTED).getResponse(), "tv", types);
          createTermvectorAssertions(
              list1.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
              list2.get(COLLECTION_ALL_OPTIMIZED).getResponse(), "tv", types);
          createTermvectorAssertions(
              list2.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
              list2.get(COLLECTION_DISTRIBUTED).getResponse(), "tv", types);
        }
      }
    }
  }
  
  @org.junit.Test
  public void mtasRequestHandlerPrefix() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String[] collections = new String[] { COLLECTION_ALL_OPTIMIZED,
        COLLECTION_ALL_MULTIPLE_SEGMENTS, COLLECTION_DISTRIBUTED };
    params.set("q", "*:*");
    params.set("mtas", "true");
    params.set("mtas.prefix", "true");
    params.set("mtas.prefix.0.key", "prefixKey");
    params.set("mtas.prefix.0.field", "mtas");
    Map<String, QueryResponse> list = createResults(params, Arrays.asList(collections));
    createPrefixAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(), list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResponse(), "prefixKey");
    createPrefixAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(), list.get(COLLECTION_DISTRIBUTED).getResponse(), "prefixKey");
  }

  /**
   * Creates the results.
   *
   * @param params
   *          the params
   * @return the hash map
   */
  private static HashMap<String, QueryResponse> createResults(
      final ModifiableSolrParams params, List<String> collections) {
    HashMap<String, QueryResponse> list = new HashMap<>();
    CloudSolrClient client = cloudCluster.getSolrClient();
    try {
      if (collections == null
          || collections.contains(COLLECTION_ALL_OPTIMIZED)) {
        list.put(COLLECTION_ALL_OPTIMIZED,
            client.query(COLLECTION_ALL_OPTIMIZED, params));
      }
      if (collections == null
          || collections.contains(COLLECTION_ALL_MULTIPLE_SEGMENTS)) {
        list.put(COLLECTION_ALL_MULTIPLE_SEGMENTS,
            client.query(COLLECTION_ALL_MULTIPLE_SEGMENTS, params));
      }
      if (collections == null
          || collections.contains(COLLECTION_PART1_OPTIMIZED)) {
        list.put(COLLECTION_PART1_OPTIMIZED,
            client.query(COLLECTION_PART1_OPTIMIZED, params));
      }
      if (collections == null
          || collections.contains(COLLECTION_PART2_MULTIPLE_SEGMENTS)) {
        list.put(COLLECTION_PART2_MULTIPLE_SEGMENTS,
            client.query(COLLECTION_PART2_MULTIPLE_SEGMENTS, params));
      }
      params.set("collection", COLLECTION_PART1_OPTIMIZED + ","
          + COLLECTION_PART2_MULTIPLE_SEGMENTS);
      if (collections == null || collections.contains(COLLECTION_DISTRIBUTED)) {
        list.put(COLLECTION_DISTRIBUTED,
            client.query(COLLECTION_DISTRIBUTED, params));
      }
    } catch (SolrServerException | IOException e) {
      log.error(e);
    }
    return list;
  }

  /**
   * Creates the stats assertions.
   *
   * @param response1
   *          the response 1
   * @param response2
   *          the response 2
   * @param type
   *          the type
   * @param key
   *          the key
   * @param names
   *          the names
   */
  private static void createStatsAssertions(NamedList<Object> response1,
      NamedList<Object> response2, String type, String key, String[] names) {
    NamedList<Object>[] responses2 = new NamedList[] { response2 };
    createStatsAssertions(response1, responses2, type, key, names);
  }

  /**
   * Creates the stats assertions.
   *
   * @param response1
   *          the response 1
   * @param responses2
   *          the responses 2
   * @param type
   *          the type
   * @param key
   *          the key
   * @param names
   *          the names
   */
  private static void createStatsAssertions(NamedList<Object> response1,
      NamedList<Object>[] responses2, String type, String key, String[] names) {
    for (String name : names) {
      assertFalse("no " + type + " - " + name,
          MtasSolrBase.getFromMtasStats(response1, type, key, name).equals(0));
      for (NamedList<Object> response2 : responses2) {
        assertEquals(MtasSolrBase.getFromMtasStats(response1, type, key, name),
            MtasSolrBase.getFromMtasStats(response2, type, key, name));
      }
    }
  }
  
  private static void createPrefixAssertions(NamedList<Object> response1,
      NamedList<Object> response2, String key) {
    Map<String,List<String>> prefix1 = MtasSolrBase.getFromMtasPrefix(response1, key);
    Map<String,List<String>> prefix2 = MtasSolrBase.getFromMtasPrefix(response2, key);
    assertTrue("inequal sizes keysets results", prefix1.keySet().size()==prefix2.keySet().size());
    for(Entry<String,List<String>> entry : prefix1.entrySet()) {
      assertTrue("doesn't contain "+entry.getKey(), prefix2.containsKey(entry.getKey()));
      if(prefix2.containsKey(entry.getKey())) {
        assertTrue("inequal result for key "+entry.getKey(), prefix1.keySet().size()==prefix2.keySet().size());
        assertTrue("inequal result for key "+entry.getKey(), prefix1.keySet().containsAll(prefix2.keySet()));
      }
    }
  }

  private static void createTermvectorAssertions(NamedList<Object> response1,
      NamedList<Object> response2, String key, String[] names) {
    List<NamedList> list1 = MtasSolrBase.getFromMtasTermvector(response1, key);
    List<NamedList> list2 = MtasSolrBase.getFromMtasTermvector(response2, key);
    assertFalse("list should be defined", list1 == null || list2 == null);
    if (list1 != null && list2 != null) {
      assertEquals("lists should have equal size", list1.size(), list2.size());
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

  /**
   * Creates the cloud.
   */
  private static void createCloud() {
    Path dataPath = Paths.get("junit").resolve("data");
    String solrxml = MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML;
    JettyConfig jettyConfig = JettyConfig.builder().setContext("/solr").build();
    File cloudBase = Files.createTempDir();
    cloudBaseDir = cloudBase.toPath();
    // create subdirectories
    Path clusterDir = cloudBaseDir.resolve("cluster");
    Path logDir = cloudBaseDir.resolve("log");
    if (clusterDir.toFile().mkdir() && logDir.toFile().mkdir()) {
      // set log directory
      System.setProperty("solr.log.dir", logDir.toAbsolutePath().toString());
      try {
        cloudCluster = new MiniSolrCloudCluster(1, clusterDir, solrxml,
            jettyConfig);
        CloudSolrClient client = cloudCluster.getSolrClient();
        client.connect();
        createCloudCollection(COLLECTION_ALL_OPTIMIZED, 1, 1,
            dataPath.resolve("conf"));
        createCloudCollection(COLLECTION_ALL_MULTIPLE_SEGMENTS, 1, 1,
            dataPath.resolve("conf"));
        createCloudCollection(COLLECTION_PART1_OPTIMIZED, 1, 1,
            dataPath.resolve("conf"));
        createCloudCollection(COLLECTION_PART2_MULTIPLE_SEGMENTS, 1, 1,
            dataPath.resolve("conf"));
        createCloudCollection(COLLECTION_DISTRIBUTED, 1, 1,
            dataPath.resolve("conf"));

        // collection1
        client.add(COLLECTION_ALL_OPTIMIZED, solrDocuments.get(1));
        client.add(COLLECTION_ALL_OPTIMIZED, solrDocuments.get(2));
        client.add(COLLECTION_ALL_OPTIMIZED, solrDocuments.get(3));
        client.commit(COLLECTION_ALL_OPTIMIZED);
        client.optimize(COLLECTION_ALL_OPTIMIZED);
        // collection2
        client.add(COLLECTION_ALL_MULTIPLE_SEGMENTS, solrDocuments.get(1));
        client.commit(COLLECTION_ALL_MULTIPLE_SEGMENTS);
        client.add(COLLECTION_ALL_MULTIPLE_SEGMENTS, solrDocuments.get(2));
        client.add(COLLECTION_ALL_MULTIPLE_SEGMENTS, solrDocuments.get(3));
        client.commit(COLLECTION_ALL_MULTIPLE_SEGMENTS);
        // collection3
        client.add(COLLECTION_PART1_OPTIMIZED, solrDocuments.get(1));
        client.commit(COLLECTION_PART1_OPTIMIZED);
        // collection4
        client.add(COLLECTION_PART2_MULTIPLE_SEGMENTS, solrDocuments.get(2));
        client.add(COLLECTION_PART2_MULTIPLE_SEGMENTS, solrDocuments.get(3));
        client.commit(COLLECTION_PART2_MULTIPLE_SEGMENTS);
      } catch (Exception e) {
        log.error(e);
      }
    } else {
      log.error("couldn't create directories");
    }
  }

  /**
   * Creates the cloud collection.
   *
   * @param collectionName
   *          the collection name
   * @param numShards
   *          the num shards
   * @param replicationFactor
   *          the replication factor
   * @param confDir
   *          the conf dir
   * @throws Exception
   *           the exception
   */
  private static void createCloudCollection(String collectionName,
      int numShards, int replicationFactor, Path confDir) throws Exception {
    CloudSolrClient client = cloudCluster.getSolrClient();
    String confName = collectionName + "Configuration";
    if (confDir != null) {
      SolrZkClient zkClient = client.getZkStateReader().getZkClient();
      ZkConfigManager zkConfigManager = new ZkConfigManager(zkClient);
      zkConfigManager.uploadConfigDir(confDir, confName);
    }
    ModifiableSolrParams modParams = new ModifiableSolrParams();
    modParams.set(CoreAdminParams.ACTION,
        CollectionParams.CollectionAction.CREATE.name());
    modParams.set("name", collectionName);
    modParams.set("numShards", numShards);
    modParams.set("replicationFactor", replicationFactor);
    int liveNodes = client.getZkStateReader().getClusterState().getLiveNodes()
        .size();
    int maxShardsPerNode = (int) Math
        .ceil(((double) numShards * replicationFactor) / liveNodes);
    modParams.set("maxShardsPerNode", maxShardsPerNode);
    modParams.set("collection.configName", confName);
    QueryRequest request = new QueryRequest(modParams);
    request.setPath("/admin/collections");
    client.request(request);
  }

  /**
   * Shutdown cloud.
   */
  private static void shutdownCloud() {
    try {
      System.clearProperty("solr.log.dir");
      cloudCluster.shutdown();
    } catch (Exception e) {
      log.error(e);
    } finally {
      MtasSolrBase.deleteDirectory(cloudBaseDir.toFile());
    }
  }

}
