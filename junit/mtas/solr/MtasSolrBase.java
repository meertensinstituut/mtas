package mtas.solr;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;

/**
 * The Class MtasSolrBase.
 */
public class MtasSolrBase {

  /**
   * Instantiates a new mtas solr base.
   */
  private MtasSolrBase() {
    // do nothing
  }

  /** The log. */
  private static Log log = LogFactory.getLog(MtasSolrBase.class);

  /**
   * Gets the from stats.
   *
   * @param response the response
   * @param field the field
   * @param name the name
   * @param round the round
   * @return the from stats
   */
  public static Number getFromStats(NamedList<Object> response, String field,
      String name, boolean round) {
    if (response == null) {
      log.error("no (valid); response");
    } else {
      Object mtasStatsRaw = response.get("stats");
      if (mtasStatsRaw != null && mtasStatsRaw instanceof NamedList) {
        NamedList<Object> mtasStats = (NamedList) mtasStatsRaw;
        Object mtasStatsFieldsRaw = mtasStats.get("stats_fields");
        if (mtasStatsFieldsRaw != null
            && mtasStatsFieldsRaw instanceof NamedList) {
          NamedList<Object> mtasStatsFields = (NamedList) mtasStatsFieldsRaw;
          Object mtasStatsFieldsFieldRaw = mtasStatsFields.get(field);
          if (mtasStatsFieldsFieldRaw != null
              && mtasStatsFieldsFieldRaw instanceof NamedList) {
            NamedList<Object> mtasStatsFieldsField = (NamedList) mtasStatsFieldsFieldRaw;
            Object mtasStatsFieldsFieldNameRaw = mtasStatsFieldsField.get(name);
            if (mtasStatsFieldsFieldNameRaw != null
                && mtasStatsFieldsFieldNameRaw instanceof Number) {
              if (round) {
                return ((Number) mtasStatsFieldsFieldNameRaw).longValue();
              } else {
                return (Number) mtasStatsFieldsFieldNameRaw;
              }
            } else {
              log.error("unexpected " + mtasStatsFieldsFieldNameRaw);
            }
          } else {
            log.error("unexpected " + mtasStatsFieldsFieldRaw);
          }
        } else {
          log.error("unexpected " + mtasStatsFieldsRaw);
        }
      } else {
        log.error("unexpected " + mtasStatsRaw);
      }
    }
    return null;
  }

  /**
   * Gets the from mtas stats.
   *
   * @param response the response
   * @param type the type
   * @param key the key
   * @param name the name
   * @return the from mtas stats
   */
  public static Number getFromMtasStats(NamedList<Object> response, String type,
      String key, String name) {
    if (response == null) {
      log.error("no (valid); response");
    } else {
      Object mtasResponseRaw = response.get("mtas");
      if (mtasResponseRaw != null && mtasResponseRaw instanceof NamedList) {
        NamedList<Object> mtasResponse = (NamedList) mtasResponseRaw;
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

  /**
   * Gets the from mtas termvector.
   *
   * @param response the response
   * @param key the key
   * @return the from mtas termvector
   */
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

  /**
   * Gets the from mtas prefix.
   *
   * @param response the response
   * @param key the key
   * @return the from mtas prefix
   */
  public static Map<String, List<String>> getFromMtasPrefix(
      NamedList<Object> response, String key) {
    if (response == null) {
      log.error("no (valid); response");
    } else {
      Object mtasResponseRaw = response.get("mtas");
      if (mtasResponseRaw != null && mtasResponseRaw instanceof NamedList) {
        NamedList<Object> mtasResponse = (NamedList) response.get("mtas");
        Object mtasPrefixResponseRaw = mtasResponse.get("prefix");
        if (mtasPrefixResponseRaw != null
            && mtasPrefixResponseRaw instanceof List) {
          List<NamedList> mtasPrefixResponse = (List) mtasPrefixResponseRaw;
          if (mtasPrefixResponse.isEmpty()) {
            log.error("no (valid) mtas prefix response");
          } else {
            NamedList<Object> item = null;
            for (NamedList<Object> mtasPrefixResponseItem : mtasPrefixResponse) {
              if (mtasPrefixResponseItem.get("key") != null
                  && (mtasPrefixResponseItem.get("key") instanceof String)
                  && mtasPrefixResponseItem.get("key").equals(key)) {
                item = mtasPrefixResponseItem;
                break;
              }
            }
            assertFalse("no item with key " + key, item == null);
            Map<String, List<String>> result = new HashMap<>();
            Iterator<Entry<String, Object>> it = item.iterator();
            Entry<String, Object> entry;
            while (it.hasNext()) {
              entry = it.next();
              if (!entry.getKey().equals("key")) {
                assertTrue("invalid entry prefix",
                    entry.getValue() instanceof List);
                result.put(entry.getKey(), (List) entry.getValue());
              }
            }
            return result;
          }
        }
      }
    }
    return null;
  }

  /**
   * Delete directory.
   *
   * @param directory the directory
   * @return true, if successful
   */
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

  /**
   * Creates the documents.
   *
   * @param includeAdvanced the include advanced
   * @return the map
   */
  public static Map<Integer, SolrInputDocument> createDocuments(
      boolean includeAdvanced) {
    Map<Integer, SolrInputDocument> solrDocuments = new HashMap<>();
    Path dataPath = Paths.get("junit").resolve("data");
    // data
    SolrInputDocument newDoc1 = new SolrInputDocument();
    newDoc1.addField("id", "1");
    newDoc1.addField("title", "Een onaangenaam mens in de Haarlemmerhout");
    newDoc1.addField("text", "Een onaangenaam mens in de Haarlemmerhout");
    newDoc1.addField("mtas", dataPath.resolve("resources")
        .resolve("beets1.xml.gz").toFile().getAbsolutePath());
    if (includeAdvanced) {
      newDoc1.addField("source", "source1");
      newDoc1.addField("mtasAdvanced", dataPath.resolve("resources")
          .resolve("beets1").toFile().getAbsolutePath());
    }
    solrDocuments.put(1, newDoc1);
    SolrInputDocument newDoc2 = new SolrInputDocument();
    newDoc2.addField("id", "2");
    newDoc2.addField("title", "Een oude kennis");
    newDoc2.addField("text", "Een oude kennis");
    newDoc2.addField("mtas", dataPath.resolve("resources")
        .resolve("beets2.xml.gz").toFile().getAbsolutePath());
    if (includeAdvanced) {
      newDoc2.addField("source", "source2");
      newDoc2.addField("mtasAdvanced", dataPath.resolve("resources")
          .resolve("beets2.xml").toFile().getAbsolutePath());
    }
    SolrInputDocument newDoc3 = new SolrInputDocument();
    solrDocuments.put(2, newDoc2);
    newDoc3.addField("id", "3");
    newDoc3.addField("title", "Varen en Rijden");
    newDoc3.addField("text", "Varen en Rijden");
    newDoc3.addField("mtas", dataPath.resolve("resources")
        .resolve("beets3.xml.gz").toFile().getAbsolutePath());
    if (includeAdvanced) {
      newDoc3.addField("source", "source3");
      newDoc3.addField("mtasAdvanced", dataPath.resolve("resources")
          .resolve("beets3.xml.gz").toFile().getAbsolutePath());
    }
    solrDocuments.put(3, newDoc3);
    return solrDocuments;
  }

}
