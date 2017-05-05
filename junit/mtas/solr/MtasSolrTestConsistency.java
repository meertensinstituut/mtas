package mtas.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;

public class MtasSolrTestConsistency {

  private static EmbeddedSolrServer server;
  private static CoreContainer container;

  private static Path solrPath;

  @org.junit.BeforeClass
  public static void initialize() {
    try {

      Path dataPath = Paths.get("junit").resolve("data");
      // data
      SolrInputDocument newDoc1 = new SolrInputDocument();
      newDoc1.addField("id", "1");
      newDoc1.addField("title", "Een onaangenaam mens in de Haarlemmerhout");
      newDoc1.addField("text", "Een onaangenaam mens in de Haarlemmerhout");
      newDoc1.addField("mtas", dataPath.resolve("resources")
          .resolve("beets1.xml.gz").toFile().getAbsolutePath());
      SolrInputDocument newDoc2 = new SolrInputDocument();
      newDoc2.addField("id", "2");
      newDoc2.addField("title", "Een oude kennis");
      newDoc2.addField("text", "Een oude kennis");
      newDoc2.addField("mtas", dataPath.resolve("resources")
          .resolve("beets2.xml.gz").toFile().getAbsolutePath());
      SolrInputDocument newDoc3 = new SolrInputDocument();
      newDoc3.addField("id", "3");
      newDoc3.addField("title", "Varen en Rijden");
      newDoc3.addField("text", "Varen en Rijden");
      newDoc3.addField("mtas", dataPath.resolve("resources")
          .resolve("beets3.xml.gz").toFile().getAbsolutePath());

      // create
      ArrayList<String> collections = new ArrayList<>(
          Arrays.asList("collection1", "collection2", "collection3"));
      initializeDirectory(dataPath, collections);
      container = new CoreContainer(solrPath.toAbsolutePath().toString());
      container.load();
      server = new EmbeddedSolrServer(container, collections.get(0));

      // add

      server.add("collection1", newDoc1);
      server.add("collection1", newDoc2);
      server.commit("collection1");
      server.add("collection1", newDoc3);
      server.commit("collection1");

      server.add("collection2", newDoc1);
      server.commit("collection2");

      server.add("collection3", newDoc3);
      server.add("collection3", newDoc2);
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
  public static void cleanup() throws IOException {
    server.close();
    deleteDirectory(solrPath.toFile());
  }

  private static boolean deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          if (files[i].isDirectory()) {
            deleteDirectory(files[i]);
          } else {
            files[i].delete();
          }
        }
      }
    }
    return (directory.delete());
  }

  @org.junit.Test
  public void cqlQueryParser1() throws IOException {
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
  public void cqlQueryParser2() throws IOException {
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
    assertEquals(getFromMtasStats(response, "spans", "statsKey", "sum"), getFromMtasStats(response, "positions", "statsKey", "sum"));            
  }
  
  private long getFromMtasStats(NamedList<Object> response, String type, String key, String name) {
    assertFalse("no (valid) response", response==null || !(response instanceof NamedList));
    NamedList<Object> mtasResponse = (NamedList<Object>) response.get("mtas");
    assertFalse("no (valid) mtas response", mtasResponse==null || !(mtasResponse.get("stats") instanceof NamedList));
    NamedList<Object> mtasStatsResponse = (NamedList<Object>) mtasResponse.get("stats");
    assertFalse("no (valid) mtas stats response", mtasStatsResponse==null || !(mtasStatsResponse.get(type) instanceof ArrayList));
    ArrayList<NamedList> mtasStatsTypeResponse = (ArrayList<NamedList>) mtasStatsResponse.get("spans");
    assertFalse("no (valid) mtas stats "+type+" response", mtasStatsTypeResponse==null || mtasStatsTypeResponse.isEmpty());
    NamedList<Object> item = null;
    for(NamedList<Object> mtasStatsSpansResponseItem : mtasStatsTypeResponse) {
      if(mtasStatsSpansResponseItem.get("key")!=null && (mtasStatsSpansResponseItem.get("key") instanceof String)) {
        if(mtasStatsSpansResponseItem.get("key").equals(key)) {
          item = mtasStatsSpansResponseItem;
          break;
        } 
      } 
    }
    assertFalse("no item with key "+key, item==null);
    assertFalse("no variable "+name, item.get(name)==null);
    assertFalse("variable "+name+" no long: "+item.get(name).getClass(), !(item.get(name) instanceof Long));   
    return (Long) item.get(name);
  }

}
