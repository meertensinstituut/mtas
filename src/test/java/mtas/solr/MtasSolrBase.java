package mtas.solr;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MtasSolrBase {
  final static String FIELD_ID = "id";
  private final static String FIELD_TITLE = "title";
  private final static String FIELD_TEXT = "text";
  final static String FIELD_MTAS = "mtas";
  final static String FIELD_MTAS_ADVANCED = "mtasAdvanced";
  private final static String FIELD_SOURCE = "source";

  private MtasSolrBase() {
  }

  private static Log log = LogFactory.getLog(MtasSolrBase.class);

  static long getNumFound(NamedList<Object> response) {
    if (response == null) {
      log.error("no (valid); response");
      return 0;
    }
    Object mtasResponseRaw = response.get("response");
    if (mtasResponseRaw == null || !(mtasResponseRaw instanceof SolrDocumentList)) {
      log.error("unexpected " + mtasResponseRaw);
      return 0;
    }
    SolrDocumentList mtasResponse = (SolrDocumentList) mtasResponseRaw;
    return mtasResponse.getNumFound();
  }

  static Number getFromStats(NamedList<Object> response, String field, String name, boolean round) {
    if (response == null) {
      log.error("no (valid); response");
      return null;
    }
    Object mtasStatsRaw = response.get("stats");
    if (mtasStatsRaw == null || !(mtasStatsRaw instanceof NamedList)) {
      log.error("unexpected " + mtasStatsRaw);
      return null;
    }
    NamedList<Object> mtasStats = (NamedList) mtasStatsRaw;
    Object mtasStatsFieldsRaw = mtasStats.get("stats_fields");
    if (mtasStatsFieldsRaw == null || !(mtasStatsFieldsRaw instanceof NamedList)) {
      log.error("unexpected " + mtasStatsFieldsRaw);
      return null;
    }
    NamedList<Object> mtasStatsFields = (NamedList<Object>) mtasStatsFieldsRaw;
    Object mtasStatsFieldsFieldRaw = mtasStatsFields.get(field);
    if (mtasStatsFieldsFieldRaw == null || !(mtasStatsFieldsFieldRaw instanceof NamedList)) {
      log.error("unexpected " + mtasStatsFieldsFieldRaw);
      return null;
    }
    NamedList<Object> mtasStatsFieldsField = (NamedList<Object>) mtasStatsFieldsFieldRaw;
    Object mtasStatsFieldsFieldNameRaw = mtasStatsFieldsField.get(name);
    if (mtasStatsFieldsFieldNameRaw == null || !(mtasStatsFieldsFieldNameRaw instanceof Number)) {
      log.error("unexpected " + mtasStatsFieldsFieldNameRaw);
      return null;
    }
    Number num = (Number) mtasStatsFieldsFieldNameRaw;
    if (round) {
      num = num.longValue();
    }
    return num;
  }

  static Number getFromMtasStats(NamedList<Object> response, String type, String key, String name) {
    if (response == null) {
      log.error("no (valid); response");
      return null;
    }
    Object mtasResponseRaw = response.get("mtas");
    if (mtasResponseRaw == null || !(mtasResponseRaw instanceof NamedList)) {
      log.error("unexpected " + mtasResponseRaw);
      return null;
    }
    NamedList<Object> mtasResponse = (NamedList) mtasResponseRaw;
    Object mtasStatsResponseRaw = mtasResponse.get("stats");
    if (mtasStatsResponseRaw == null || !(mtasStatsResponseRaw instanceof NamedList)) {
      log.error("unexpected " + mtasStatsResponseRaw);
      return null;
    }
    NamedList<Object> mtasStatsResponse = (NamedList) mtasStatsResponseRaw;
    Object mtasStatsTypeResponseRaw = mtasStatsResponse.get(type);
    if (mtasStatsTypeResponseRaw == null || !(mtasStatsTypeResponseRaw instanceof List)) {
      log.error("unexpected " + mtasStatsTypeResponseRaw);
      return null;
    }
    List<NamedList> mtasStatsTypeResponse = (List) mtasStatsResponse.get(type);
    if (mtasStatsTypeResponse.isEmpty()) {
      log.error("no (valid) mtas stats " + type + " response");
      return null;
    }
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
    assertFalse("no variable " + name, item.get(name) == null);
    if (item.get(name) instanceof Long) {
      return (Long) item.get(name);
    } else {
      return (Double) item.get(name);
    }
  }

  static List<NamedList<Object>> getFromMtasTermvector(NamedList<Object> response, String key) {
    if (response == null) {
      log.error("no (valid); response");
      return null;
    }
    Object mtasResponseRaw = response.get("mtas");
    if (mtasResponseRaw == null || !(mtasResponseRaw instanceof NamedList)) {
      log.error("unexpected " + mtasResponseRaw);
      return null;
    }
    NamedList<Object> mtasResponse = (NamedList<Object>) response.get("mtas");
    Object mtasTermvectorResponseRaw = mtasResponse.get("termvector");
    if (mtasTermvectorResponseRaw == null || !(mtasTermvectorResponseRaw instanceof List)) {
      log.error("unexpected " + mtasTermvectorResponseRaw);
      return null;
    }
    List<NamedList<Object>> mtasTermvectorResponse = (List) mtasTermvectorResponseRaw;
    if (mtasTermvectorResponse.isEmpty()) {
      log.error("no (valid) mtas termvector response");
      return null;
    }
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
    if (item.get("list") == null || !(item.get("list") instanceof List)) {
      return null;
    }
    return (List<NamedList<Object>>) item.get("list");
  }

  static List<NamedList<Object>> getFromMtasGroup(NamedList<Object> response, String key) {
    if (response == null) {
      log.error("no (valid); response");
      return null;
    }
    Object mtasResponseRaw = response.get("mtas");
    if (mtasResponseRaw == null || !(mtasResponseRaw instanceof NamedList)) {
      log.error("unexpected " + mtasResponseRaw);
      return null;
    }
    NamedList<Object> mtasResponse = (NamedList<Object>) response.get("mtas");
    Object mtasGroupResponseRaw = mtasResponse.get("group");
    if (mtasGroupResponseRaw == null || !(mtasGroupResponseRaw instanceof List)) {
      log.error("unexpected " + mtasGroupResponseRaw);
      return null;
    }
    List<NamedList<Object>> mtasGroupResponse = (List) mtasGroupResponseRaw;
    if (mtasGroupResponse.isEmpty()) {
      log.error("no (valid) mtas group response");
      return null;
    }
    NamedList<Object> item = null;
    for (NamedList<Object> mtasGroupResponseItem : mtasGroupResponse) {
      if (mtasGroupResponseItem.get("key") != null
        && (mtasGroupResponseItem.get("key") instanceof String)
        && mtasGroupResponseItem.get("key").equals(key)) {
        item = mtasGroupResponseItem;
        break;
      }
    }
    assertFalse("no item with key " + key, item == null);
    if (item.get("list") == null || !(item.get("list") instanceof List)) {
      return null;
    }
    return (List<NamedList<Object>>) item.get("list");
  }

  static Map<String, List<String>> getFromMtasPrefix(
    NamedList<Object> response, String key) {
    if (response == null) {
      log.error("no (valid); response");
      return null;
    }
    Object mtasResponseRaw = response.get("mtas");
    if (mtasResponseRaw == null || !(mtasResponseRaw instanceof NamedList)) {
      return null;
    }
    NamedList<Object> mtasResponse = (NamedList) response.get("mtas");
    Object mtasPrefixResponseRaw = mtasResponse.get("prefix");
    if (mtasPrefixResponseRaw == null || !(mtasPrefixResponseRaw instanceof List)) {
      return null;
    }
    List<NamedList> mtasPrefixResponse = (List) mtasPrefixResponseRaw;
    if (mtasPrefixResponse.isEmpty()) {
      log.error("no (valid) mtas prefix response");
      return null;
    }
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
    for (Entry<String, Object> entry : item) {
      if (!entry.getKey().equals("key")) {
        assertTrue("invalid entry prefix", entry.getValue() instanceof List);
        result.put(entry.getKey(), (List) entry.getValue());
      }
    }
    return result;
  }

  static NamedList<Object> getFromMtasCollection(
    NamedList<Object> response, String key) {
    if (response == null) {
      log.error("no (valid); response");
      return null;
    }
    Object mtasResponseRaw = response.get("mtas");
    if (mtasResponseRaw == null || !(mtasResponseRaw instanceof NamedList)) {
      log.error("unexpected " + mtasResponseRaw);
      return null;
    }
    NamedList<Object> mtasResponse = (NamedList<Object>) response.get("mtas");
    Object mtasCollectionResponseRaw = mtasResponse.get("collection");
    if (mtasCollectionResponseRaw == null || !(mtasCollectionResponseRaw instanceof List)) {
      log.error("unexpected " + mtasCollectionResponseRaw);
      return null;
    }
    List<NamedList<Object>> mtasCollectionResponse = (List<NamedList<Object>>) mtasCollectionResponseRaw;
    if (mtasCollectionResponse.isEmpty()) {
      log.error("no (valid) mtas join response");
      return null;
    }
    for (NamedList<Object> mtasCollectionResponseItem : mtasCollectionResponse) {
      if (mtasCollectionResponseItem.get("key") != null
        && (mtasCollectionResponseItem.get("key") instanceof String)
        && mtasCollectionResponseItem.get("key").equals(key)) {
        return mtasCollectionResponseItem;
      }
    }
    return null;
  }

  static NamedList<Object> getFromMtasCollectionList(NamedList<Object> response, String key, String id) {
    NamedList<Object> collectionResponse = getFromMtasCollection(response, key);
    if (collectionResponse == null) {
      log.error("no collectionResponse (searching key " + key + ")");
      return null;
    }
    Object collectionResponseListRaw = collectionResponse.get("list");
    if (collectionResponseListRaw == null || !(collectionResponseListRaw instanceof List)) {
      log.error("unexpected " + collectionResponseListRaw + " (searching list)");
      return null;
    }
    List<NamedList<Object>> collectionResponseList = (List<NamedList<Object>>) collectionResponseListRaw;
    for (NamedList<Object> item : collectionResponseList) {
      if (item.get("id") != null && item.get("id") instanceof String) {
        if (id.equals(item.get("id"))) {
          return item;
        }
      }
    }
    return null;
  }

  static boolean deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (null != files) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else if (!file.delete()) {
            log.info("can't delete " + file);
          }
        }
      }
    }
    return directory.delete();
  }

  private static SolrInputDocument createDocument(boolean includeAdvanced, String id, String title, String basename)
    throws IOException {
    Path dataPath = Paths.get("src", "test", "resources", "data");
    SolrInputDocument doc = new SolrInputDocument();

    doc.addField(FIELD_ID, id);
    doc.addField(FIELD_TITLE, title);
    doc.addField(FIELD_TEXT, title);

    String path = dataPath.resolve("resources").resolve(basename).toAbsolutePath().toString();
    String content;
    try (InputStream in = new FileInputStream(path)) {
      content = IOUtils.toString(new GZIPInputStream(in), Charset.forName("UTF-8"));
    }
    doc.addField(FIELD_MTAS, content);

    if (includeAdvanced) {
      doc.addField(FIELD_SOURCE, "source" + id);
      doc.addField(FIELD_MTAS_ADVANCED, content);
      // doc.addField(FIELD_MTAS_ADVANCED, path); // XXX content?
    }
    return doc;
  }

  static Map<Integer, SolrInputDocument> createDocuments(boolean includeAdvanced) throws IOException {
    Map<Integer, SolrInputDocument> docs = new HashMap<>();
    docs.put(1, createDocument(includeAdvanced, "1", "Een onaangenaam mens in de Haarlemmerhout", "beets1.xml.gz"));
    docs.put(2, createDocument(includeAdvanced, "2", "Een oude kennis", "beets2.xml.gz"));
    docs.put(3, createDocument(includeAdvanced, "3", "Varen en Rijden", "beets3.xml.gz"));
    return docs;
  }
}
