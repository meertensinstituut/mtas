package mtas.solr;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;

public class MtasSolrBase {
  
  public static Number getFromMtasStats(NamedList<Object> response, String type, String key, String name) {
    assertFalse("no (valid) response", response==null || !(response instanceof NamedList));
    NamedList<Object> mtasResponse = (NamedList<Object>) response.get("mtas");
    assertFalse("no (valid) mtas response", mtasResponse==null || !(mtasResponse.get("stats") instanceof NamedList));
    NamedList<Object> mtasStatsResponse = (NamedList<Object>) mtasResponse.get("stats");
    assertFalse("no (valid) mtas stats response", mtasStatsResponse==null || !(mtasStatsResponse.get(type) instanceof ArrayList));
    ArrayList<NamedList> mtasStatsTypeResponse = (ArrayList<NamedList>) mtasStatsResponse.get(type);
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
    assertFalse("variable "+name+" is "+item.get(name).getClass(), !(item.get(name) instanceof Long) && !(item.get(name) instanceof Double));   
    if(item.get(name) instanceof Long) {
      return (Long) item.get(name);
    } else {
      return (Double) item.get(name);
    }
  }
  
  public static boolean deleteDirectory(File directory) {
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
  
  public static HashMap<Integer, SolrInputDocument> createDocuments() {
    HashMap<Integer, SolrInputDocument> solrDocuments = new HashMap<>();
    Path dataPath = Paths.get("junit").resolve("data");
    solrDocuments = new HashMap<Integer, SolrInputDocument>();
    // data
    SolrInputDocument newDoc1 = new SolrInputDocument();
    newDoc1.addField("id", "1");
    newDoc1.addField("title", "Een onaangenaam mens in de Haarlemmerhout");
    newDoc1.addField("text", "Een onaangenaam mens in de Haarlemmerhout");
    newDoc1.addField("mtas", dataPath.resolve("resources")
        .resolve("beets1.xml.gz").toFile().getAbsolutePath());
    solrDocuments.put(1, newDoc1);
    SolrInputDocument newDoc2 = new SolrInputDocument();
    newDoc2.addField("id", "2");
    newDoc2.addField("title", "Een oude kennis");
    newDoc2.addField("text", "Een oude kennis");
    newDoc2.addField("mtas", dataPath.resolve("resources")
        .resolve("beets2.xml.gz").toFile().getAbsolutePath());
    SolrInputDocument newDoc3 = new SolrInputDocument();
    solrDocuments.put(2, newDoc2);
    newDoc3.addField("id", "3");
    newDoc3.addField("title", "Varen en Rijden");
    newDoc3.addField("text", "Varen en Rijden");
    newDoc3.addField("mtas", dataPath.resolve("resources")
        .resolve("beets3.xml.gz").toFile().getAbsolutePath());
    solrDocuments.put(3, newDoc3);
    return solrDocuments;
  }

  

}
