package mtas.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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

  private static EmbeddedSolrServer server;
  private static CoreContainer container;

  private static Path solrPath;
  
  private static HashMap<Integer, SolrInputDocument> solrDocuments;

  @org.junit.BeforeClass
  public static void setup() {
    try {

      Path dataPath = Paths.get("junit").resolve("data");
      // data
      solrDocuments = MtasSolrBase.createDocuments();      

      // create
      ArrayList<String> collections = new ArrayList<>(
          Arrays.asList("collection1", "collection2", "collection3"));
      initializeDirectory(dataPath, collections);
      container = new CoreContainer(solrPath.toAbsolutePath().toString());
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

    } catch (IOException e) {
      e.printStackTrace();
    } catch (SolrServerException e) {
      e.printStackTrace();
    }
  }

  private static void initializeDirectory(Path dataPath,
      List<String> collections) throws IOException {
    solrPath = Files.createTempDirectory("junitSolr");
    // create and fill
    solrPath.toFile().mkdir();
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
    FileWriter writer;
    // create directories
    solrPath.resolve(collectionName).toFile().mkdir();
    solrPath.resolve(collectionName).resolve("conf").toFile().mkdir();
    solrPath.resolve(collectionName).resolve("data").toFile().mkdir();
    // copy files
    Files.copy(dataPath.resolve("conf").resolve("solrconfig.xml"), solrPath
        .resolve(collectionName).resolve("conf").resolve("solrconfig.xml"));
    Files.copy(dataPath.resolve("conf").resolve("schema.xml"),
        solrPath.resolve(collectionName).resolve("conf").resolve("schema.xml"));
    Files.copy(dataPath.resolve("conf").resolve("folia.xml"),
        solrPath.resolve(collectionName).resolve("conf").resolve("folia.xml"));
    // create core.properties
    solrFile = solrPath.resolve(collectionName).resolve("core.properties")
        .toFile();
    writer = new FileWriter(solrFile, false);
    writer.write("name=" + collectionName + "\n");
    writer.close();
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
      assertEquals(docList1.size(), docList2.size() + docList3.size());      
    } catch (SolrServerException e) {
      new IOException(e.getMessage(), e);
    }
  }
  
  @org.junit.Test
  public void cqlQueryParserFilter() throws IOException {
    ModifiableSolrParams params1 = new ModifiableSolrParams();
    ModifiableSolrParams params2 = new ModifiableSolrParams();
    params1.set("q",
        "{!mtas_cql field=\"mtas\" query=\"[pos=\\\"ADJ\\\"]{2}[pos=\\\"N\\\"]\"}");
    params2.set("q","*");
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
      new IOException(e.getMessage(), e);
    }

  }

  @org.junit.Test
  public void mtasRequestHandler() throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("mtas", "true");
    params.set("mtas.stats","true");
    params.set("mtas.stats.spans","true");
    params.set("mtas.stats.spans.0.field","mtas");
    params.set("mtas.stats.spans.0.key","statsKey");
    params.set("mtas.stats.spans.0.query.0.type","cql");
    params.set("mtas.stats.spans.0.query.0.value","[]");
    params.set("mtas.stats.spans.0.type","n,sum,mean");
    params.set("mtas.stats.positions","true");
    params.set("mtas.stats.positions.0.field","mtas");
    params.set("mtas.stats.positions.0.key","statsKey");
    params.set("mtas.stats.positions.0.type","n,sum,mean");
    params.set("rows","0");
    SolrRequest<?> request = new QueryRequest(params, METHOD.POST);
    NamedList<Object> response = server.request(request, "collection1");
    assertEquals(MtasSolrBase.getFromMtasStats(response, "spans", "statsKey", "sum"), MtasSolrBase.getFromMtasStats(response, "positions", "statsKey", "sum"));            
  }
  
  

}
