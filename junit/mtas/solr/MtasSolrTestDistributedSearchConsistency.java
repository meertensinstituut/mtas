package mtas.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

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
    solrDocuments = MtasSolrBase.createDocuments();
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
    HashMap<String, QueryResponse> list = createResults(params);
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
    HashMap<String, QueryResponse> list = createResults(params);
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
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerStatsTokens() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String[] types = new String[] { "n", "sum", "mean", "min", "max" };
    params.set("q", "*:*");
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.tokens", "true");
    params.set("mtas.stats.tokens.0.field", "mtas");
    params.set("mtas.stats.tokens.0.key", "statsKey");
    params.set("mtas.stats.tokens.0.type", String.join(",", types));
    params.set("rows", "0");
    HashMap<String, QueryResponse> list = createResults(params);
    createStatsAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
        list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResponse(), "tokens",
        "statsKey", types);
  }

  /**
   * Mtas request handler stats positions.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerStatsPositions() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String[] types = new String[] { "n", "sum", "mean", "min", "max" };
    params.set("q", "*:*");
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.positions", "true");
    params.set("mtas.stats.positions.0.field", "mtas");
    params.set("mtas.stats.positions.0.key", "statsKey");
    params.set("mtas.stats.positions.0.type", String.join(",", types));
    params.set("rows", "0");
    HashMap<String, QueryResponse> list = createResults(params);
    createStatsAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
        list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResponse(), "positions",
        "statsKey", types);
  }

  /**
   * Mtas request handler stats spans.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @org.junit.Test
  public void mtasRequestHandlerStatsSpans() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String[] types = new String[] { "n", "sum", "mean", "min", "max" };
    params.set("q", "*:*");
    params.set("mtas", "true");
    params.set("mtas.stats", "true");
    params.set("mtas.stats.spans", "true");
    params.set("mtas.stats.spans.0.field", "mtas");
    params.set("mtas.stats.spans.0.key", "statsKey");
    params.set("mtas.stats.spans.0.query.0.type", "cql");
    params.set("mtas.stats.spans.0.query.0.value", "[pos=\"LID\"]");
    params.set("mtas.stats.spans.0.type", String.join(",", types));
    params.set("rows", "0");
    HashMap<String, QueryResponse> list = createResults(params);
    createStatsAssertions(list.get(COLLECTION_ALL_OPTIMIZED).getResponse(),
        list.get(COLLECTION_ALL_MULTIPLE_SEGMENTS).getResponse(), "spans",
        "statsKey", types);
  }

  /**
   * Creates the results.
   *
   * @param params the params
   * @return the hash map
   */
  private static HashMap<String, QueryResponse> createResults(
      final ModifiableSolrParams params) {
    HashMap<String, QueryResponse> list = new HashMap<>();
    CloudSolrClient client = cloudCluster.getSolrClient();
    try {
      list.put(COLLECTION_ALL_OPTIMIZED,
          client.query(COLLECTION_ALL_OPTIMIZED, params));
      list.put(COLLECTION_ALL_MULTIPLE_SEGMENTS,
          client.query(COLLECTION_ALL_MULTIPLE_SEGMENTS, params));
      list.put(COLLECTION_PART1_OPTIMIZED,
          client.query(COLLECTION_PART1_OPTIMIZED, params));
      list.put(COLLECTION_PART2_MULTIPLE_SEGMENTS,
          client.query(COLLECTION_PART2_MULTIPLE_SEGMENTS, params));
      params.set("collection", COLLECTION_PART1_OPTIMIZED + ","
          + COLLECTION_PART2_MULTIPLE_SEGMENTS);
      list.put(COLLECTION_DISTRIBUTED,
          client.query(COLLECTION_DISTRIBUTED, params));
    } catch (SolrServerException | IOException e) {
      log.error(e);
    }
    return list;
  }

  /**
   * Creates the stats assertions.
   *
   * @param response1 the response 1
   * @param response2 the response 2
   * @param type the type
   * @param key the key
   * @param names the names
   */
  private static void createStatsAssertions(NamedList<Object> response1,
      NamedList<Object> response2, String type, String key, String[] names) {
    NamedList<Object>[] responses2 = new NamedList[] { response2 };
    createStatsAssertions(response1, responses2, type, key, names);
  }

  /**
   * Creates the stats assertions.
   *
   * @param response1 the response 1
   * @param responses2 the responses 2
   * @param type the type
   * @param key the key
   * @param names the names
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
   * @param collectionName the collection name
   * @param numShards the num shards
   * @param replicationFactor the replication factor
   * @param confDir the conf dir
   * @throws Exception the exception
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
