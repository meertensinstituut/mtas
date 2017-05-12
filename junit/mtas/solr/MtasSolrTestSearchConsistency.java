package mtas.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.search.highlight.DefaultEncoder;
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

public class MtasSolrTestSearchConsistency {

  private static Log log = LogFactory
      .getLog(MtasSolrTestSearchConsistency.class);

  private static EmbeddedSolrServer server;

  private static Path solrPath;

  @org.junit.BeforeClass
  public static void setup() {
    try {

      Path dataPath = Paths.get("junit").resolve("data");
      // data
      Map<Integer, SolrInputDocument> solrDocuments = MtasSolrBase
          .createDocuments();

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
      server.add("collection1", solrDocuments.get(2));
      server.commit("collection1");
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

  @org.junit.AfterClass
  public static void shutdown() throws IOException {
    server.close();
    MtasSolrBase.deleteDirectory(solrPath.toFile());
  }

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

  @org.junit.Test
  public void mtasRequestHandler() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
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
  }

}
