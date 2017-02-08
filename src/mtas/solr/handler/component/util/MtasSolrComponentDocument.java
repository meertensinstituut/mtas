package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;

import mtas.codec.util.CodecComponent.ComponentDocument;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentDocument.
 */
public class MtasSolrComponentDocument {

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant PARAM_MTAS_DOCUMENT. */
  public static final String PARAM_MTAS_DOCUMENT = MtasSolrSearchComponent.PARAM_MTAS
      + ".document";

  /** The Constant NAME_MTAS_DOCUMENT_FIELD. */
  public static final String NAME_MTAS_DOCUMENT_FIELD = "field";

  /** The Constant NAME_MTAS_DOCUMENT_KEY. */
  public static final String NAME_MTAS_DOCUMENT_KEY = "key";

  /** The Constant NAME_MTAS_DOCUMENT_PREFIX. */
  public static final String NAME_MTAS_DOCUMENT_PREFIX = "prefix";

  /** The Constant NAME_MTAS_DOCUMENT_TYPE. */
  public static final String NAME_MTAS_DOCUMENT_TYPE = "type";

  /** The Constant NAME_MTAS_DOCUMENT_REGEXP. */
  public static final String NAME_MTAS_DOCUMENT_REGEXP = "regexp";

  /** The Constant NAME_MTAS_DOCUMENT_REGEXP. */
  public static final String NAME_MTAS_DOCUMENT_LIST = "list";
  
  public static final String NAME_MTAS_DOCUMENT_LIST_REGEXP = "listRegexp";

  public static final String NAME_MTAS_DOCUMENT_LIST_EXPAND = "listExpand";
  
  public static final String NAME_MTAS_DOCUMENT_LIST_EXPAND_NUMBER = "listExpandNumber";
  
  public static final String NAME_MTAS_DOCUMENT_IGNORE_REGEXP = "ignoreRegexp";
  
  public static final String NAME_MTAS_DOCUMENT_IGNORE_LIST = "ignoreList";
  
  public static final String NAME_MTAS_DOCUMENT_IGNORE_LIST_REGEXP = "ignoreListRegexp";

  /** The Constant NAME_MTAS_DOCUMENT_NUMBER. */
  public static final String NAME_MTAS_DOCUMENT_NUMBER = "number";

  /**
   * Instantiates a new mtas solr component document.
   *
   * @param searchComponent the search component
   */
  public MtasSolrComponentDocument(MtasSolrSearchComponent searchComponent) {
    this.searchComponent = searchComponent;
  }

  /**
   * Prepare.
   *
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void prepare(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = MtasSolrResultUtil
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_DOCUMENT);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] prefixes = new String[ids.size()];
      String[] types = new String[ids.size()];
      String[] regexps = new String[ids.size()];
      String[] lists = new String[ids.size()];
      Boolean[] listRegexps = new Boolean[ids.size()];
      Boolean[] listExpands = new Boolean[ids.size()];
      int[] listExpandNumbers = new int[ids.size()];
      String[] ignoreRegexps = new String[ids.size()];
      String[] ignoreLists = new String[ids.size()];
      Boolean[] ignoreListRegexps = new Boolean[ids.size()];
      String[] listNumbers = new String[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_FIELD,
            null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_KEY,
                String.valueOf(tmpCounter))
            .trim();
        prefixes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_PREFIX,
            null);
        types[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_TYPE,
            null);
        regexps[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_REGEXP,
            null);
        lists[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_LIST,
            null);
        listRegexps[tmpCounter] = rb.req.getParams().getBool(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_LIST_REGEXP,
            false);
        listExpands[tmpCounter] = rb.req.getParams().getBool(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_LIST_EXPAND,
            false);
        listExpandNumbers[tmpCounter] = rb.req.getParams().getInt(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_LIST_EXPAND_NUMBER,
            10);
        ignoreRegexps[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_IGNORE_REGEXP,
            null);
        ignoreLists[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_IGNORE_LIST,
            null);
        ignoreListRegexps[tmpCounter] = rb.req.getParams().getBool(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_IGNORE_LIST_REGEXP,
            false);
        listNumbers[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DOCUMENT + "." + id + "." + NAME_MTAS_DOCUMENT_NUMBER,
            null);
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doDocument = true;
      rb.setNeedDocList(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas kwic");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      MtasSolrResultUtil.compareAndCheck(keys, fields, NAME_MTAS_DOCUMENT_KEY,
          NAME_MTAS_DOCUMENT_FIELD, true);
      MtasSolrResultUtil.compareAndCheck(prefixes, fields,
          NAME_MTAS_DOCUMENT_PREFIX, NAME_MTAS_DOCUMENT_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(types, fields, NAME_MTAS_DOCUMENT_TYPE,
          NAME_MTAS_DOCUMENT_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(regexps, fields,
          NAME_MTAS_DOCUMENT_REGEXP, NAME_MTAS_DOCUMENT_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(lists, fields,
          NAME_MTAS_DOCUMENT_LIST, NAME_MTAS_DOCUMENT_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(ignoreRegexps, fields,
          NAME_MTAS_DOCUMENT_IGNORE_REGEXP, NAME_MTAS_DOCUMENT_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(ignoreLists, fields,
          NAME_MTAS_DOCUMENT_IGNORE_LIST, NAME_MTAS_DOCUMENT_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(listNumbers, fields,
          NAME_MTAS_DOCUMENT_NUMBER, NAME_MTAS_DOCUMENT_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + fields[i] + ":" + prefixes[i]
            : keys[i].trim();
        String prefix = prefixes[i];
        String type = types[i];
        String regexp = regexps[i];
        String[] list = null;
        Boolean listRegexp = listRegexps[i];
        Boolean listExpand = listExpands[i];
        int listExpandNumber = listExpandNumbers[i];
        if(lists[i]!=null) {
          ArrayList<String> tmpList = new ArrayList<String>();
          String[] subList = lists[i].split("(?<!\\\\),");
          for(int j=0; j<subList.length; j++) {
            tmpList.add(subList[j].replace("\\,", ",").replace("\\\\", "\\"));
          }
          list = tmpList.toArray(new String[tmpList.size()]);
        }
        int listNumber = Math.max(0, (listNumbers[i] == null) || (listNumbers[i].isEmpty())
            ? 0 : Integer.parseInt(listNumbers[i]));
        String ignoreRegexp = ignoreRegexps[i];
        String[] ignoreList = null;
        Boolean ignoreListRegexp = ignoreListRegexps[i];
        if(ignoreLists[i]!=null) {
          ArrayList<String> tmpList = new ArrayList<String>();
          String[] subList = ignoreLists[i].split("(?<!\\\\),");
          for(int j=0; j<subList.length; j++) {
            tmpList.add(subList[j].replace("\\,", ",").replace("\\\\", "\\"));
          }
          ignoreList = tmpList.toArray(new String[tmpList.size()]);
        }
        mtasFields.list.get(fields[i]).documentList
            .add(new ComponentDocument(key, prefix, type, regexp, list, listNumber, listRegexp, listExpand, listExpandNumber, ignoreRegexp, ignoreList, ignoreListRegexp));
      }
    }
  }

  /**
   * Creates the.
   *
   * @param document the document
   * @return the simple ordered map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public SimpleOrderedMap<Object> create(ComponentDocument document)
      throws IOException {
    SimpleOrderedMap<Object> mtasDocumentResponse = new SimpleOrderedMap<>();
    mtasDocumentResponse.add("key", document.key);
    ArrayList<NamedList<Object>> mtasDocumentItemResponses = new ArrayList<NamedList<Object>>();
    for (int docId : document.statsData.keySet()) {
      NamedList<Object> mtasDocumentItemResponse = new SimpleOrderedMap<>();
      MtasDataCollector<?, ?> stats = document.statsData.get(docId);
      MtasDataCollector<?, ?> list = null;
      HashMap<String, MtasDataCollector<?, ?>> expandedList = null;
      if (document.statsList != null) {
        list = document.statsList.get(docId);
      }
      mtasDocumentItemResponse.add("stats", new MtasSolrResult(stats,
          stats.getDataType(), stats.getStatsType(), stats.statsItems, null));
      mtasDocumentItemResponse.add("documentKey",
          document.uniqueKey.get(docId));
      if (list != null) {                
        if(document.listExpand) {
          mtasDocumentItemResponse.add("list", new MtasSolrResult(list,
            new String[] { list.getDataType(), list.getDataType()}, new String[] {list.getStatsType(), list.getStatsType()}, new TreeSet[] {list.statsItems, list.statsItems}, new String[] {null, null}, new String[] {null, null}, new Integer[] { 0 , 0}, new Integer[] { 1 , 1}, null));
        } else {
          mtasDocumentItemResponse.add("list", new MtasSolrResult(list,
              list.getDataType(), list.getStatsType(), list.statsItems, null));  
        }
                
      }
      // add
      mtasDocumentItemResponses.add(mtasDocumentItemResponse);
    }
    mtasDocumentResponse.add("list", mtasDocumentItemResponses);
    MtasSolrResultUtil.rewrite(mtasDocumentResponse);
    return mtasDocumentResponse;
  }

  /**
   * Modify request.
   *
   * @param rb the rb
   * @param who the who
   * @param sreq the sreq
   */
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq) {
    if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
      if (sreq.params.getBool(PARAM_MTAS_DOCUMENT, false)) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
          // do nothing
        } else {
          Set<String> keys = MtasSolrResultUtil
              .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_DOCUMENT);
          sreq.params.remove(PARAM_MTAS_DOCUMENT);
          for (String key : keys) {
            sreq.params.remove(PARAM_MTAS_DOCUMENT + "." + key + "."
                + NAME_MTAS_DOCUMENT_FIELD);
            sreq.params.remove(
                PARAM_MTAS_DOCUMENT + "." + key + "." + NAME_MTAS_DOCUMENT_KEY);
            sreq.params.remove(PARAM_MTAS_DOCUMENT + "." + key + "."
                + NAME_MTAS_DOCUMENT_PREFIX);
          }
        }
      }
    }
  }

  /**
   * Finish stage.
   *
   * @param rb the rb
   */
  public void finishStage(ResponseBuilder rb) {
    if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
      if (rb.stage >= ResponseBuilder.STAGE_EXECUTE_QUERY
          && rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
        for (ShardRequest sreq : rb.finished) {
          if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
              && sreq.params.getBool(PARAM_MTAS_DOCUMENT, false)) {
            // nothing to do
          }
        }
      }
    }
  }

  /**
   * Distributed process.
   *
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    // rewrite
    NamedList<Object> mtasResponse = null;
    try {
      mtasResponse = (NamedList<Object>) rb.rsp.getValues().get("mtas");
      if (mtasResponse != null) {
        ArrayList<Object> mtasResponseDocument;
        try {
          mtasResponseDocument = (ArrayList<Object>) mtasResponse
              .get("document");
          if (mtasResponseDocument != null) {
            MtasSolrResultUtil.rewrite(mtasResponseDocument);
          }
        } catch (ClassCastException e) {
          mtasResponseDocument = null;
        }
      }
    } catch (ClassCastException e) {
      mtasResponse = null;
    }
  }

}
