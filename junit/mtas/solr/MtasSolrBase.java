package mtas.solr;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;

public class MtasSolrBase {

  private MtasSolrBase() {
    // do nothing
  }

  private static Log log = LogFactory.getLog(MtasSolrBase.class);

  public static Number getFromMtasStats(NamedList<Object> response, String type,
      String key, String name) {
    if (response == null) {
      log.error("no (valid); response");
    } else {
      Object mtasResponseRaw = response.get("mtas");
      if (mtasResponseRaw != null && mtasResponseRaw instanceof NamedList) {
        NamedList<Object> mtasResponse = (NamedList) response.get("mtas");
        Object mtasStatsResponseRaw = mtasResponse.get("stats");
        if (mtasStatsResponseRaw != null
            && mtasStatsResponseRaw instanceof NamedList) {
          NamedList<Object> mtasStatsResponse = (NamedList) mtasStatsResponseRaw;
          Object mtasStatsTypeResponseRaw = mtasStatsResponse.get(type);
          if (mtasStatsTypeResponseRaw != null
              && mtasStatsTypeResponseRaw instanceof List) {
            List<NamedList> mtasStatsTypeResponse = (List) mtasStatsResponse
                .get(type);
            if (mtasStatsTypeResponse.isEmpty()) {
              log.error("no (valid) mtas stats " + type + " response");
            } else {
              NamedList<Object> item = null;
              for (NamedList<Object> mtasStatsSpansResponseItem : mtasStatsTypeResponse) {
                if (mtasStatsSpansResponseItem.get("key") != null
                    && (mtasStatsSpansResponseItem.get("key") instanceof String)
                    && mtasStatsSpansResponseItem.get("key").equals(key)) {
                  item = mtasStatsSpansResponseItem;
                  break;
                }
              }
              assertFalse("no item with key " + key, item == null);
              assertFalse("no variable " + name,
                  item != null && item.get(name) == null);
              if (item != null && item.get(name) instanceof Long) {
                return (Long) item.get(name);
              } else if (item != null) {
                return (Double) item.get(name);
              }
            }
          } else {
            log.error("unexpected " + mtasStatsTypeResponseRaw);
          }
        } else {
          log.error("unexpected " + mtasStatsResponseRaw);
        }
      } else {
        log.error("unexpected " + mtasResponseRaw);
      }
    }
    return null;
  }

  public static List<NamedList> getFromMtasTermvector(
      NamedList<Object> response, String key) {
    if (response == null) {
      log.error("no (valid); response");
    } else {
      Object mtasResponseRaw = response.get("mtas");
      if (mtasResponseRaw != null && mtasResponseRaw instanceof NamedList) {
        NamedList<Object> mtasResponse = (NamedList) response.get("mtas");
        Object mtasTermvectorResponseRaw = mtasResponse.get("termvector");
        if (mtasTermvectorResponseRaw != null
            && mtasTermvectorResponseRaw instanceof List) {
          List<NamedList> mtasTermvectorResponse = (List) mtasTermvectorResponseRaw;
          if (mtasTermvectorResponse.isEmpty()) {
            log.error("no (valid) mtas termvector response");
          } else {
            NamedList<Object> item = null;
            for (NamedList<Object> mtasTermvectorResponseItem : mtasTermvectorResponse) {
              if (mtasTermvectorResponseItem.get("key") != null
                  && (mtasTermvectorResponseItem.get("key") instanceof String)
                  && mtasTermvectorResponseItem.get("key").equals(key)) {
                item = mtasTermvectorResponseItem;
                break;
              }
            }
            assertFalse("no item with key " + key, item == null);
            if (item.get("list") != null
                && (item.get("list") instanceof List)) {
              return (List<NamedList>) item.get("list");
            }
          }
        } else {
          log.error("unexpected " + mtasTermvectorResponseRaw);
        }
      } else {
        log.error("unexpected " + mtasResponseRaw);
      }
    }
    return null;
  }

  public static boolean deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          if (files[i].isDirectory()) {
            deleteDirectory(files[i]);
          } else if (!files[i].delete()) {
            log.info("can't delete " + files[i]);
          }
        }
      }
    }
    return (directory.delete());
  }

  public static Map<Integer, SolrInputDocument> createDocuments() {
    Map<Integer, SolrInputDocument> solrDocuments = new HashMap<>();
    Path dataPath = Paths.get("junit").resolve("data");
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
