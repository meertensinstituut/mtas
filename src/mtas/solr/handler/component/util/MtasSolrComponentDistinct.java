package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;

import mtas.codec.util.CodecComponent.ComponentDistinct;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentDistinct.
 */
public class MtasSolrComponentDistinct {

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant PARAM_MTAS_DISTINCT. */
  public static final String PARAM_MTAS_DISTINCT = MtasSolrSearchComponent.PARAM_MTAS
      + ".distinct";

  /** The Constant NAME_MTAS_DISTINCT_FIELD. */
  public static final String NAME_MTAS_DISTINCT_FIELD = "field";

  /** The Constant NAME_MTAS_DISTINCT_KEY. */
  public static final String NAME_MTAS_DISTINCT_KEY = "key";

  /** The Constant NAME_MTAS_DISTINCT_PREFIX. */
  public static final String NAME_MTAS_DISTINCT_PREFIX = "prefix";

  /** The Constant NAME_MTAS_DISTINCT_TYPE. */
  public static final String NAME_MTAS_DISTINCT_TYPE = "type";

  /** The Constant NAME_MTAS_DISTINCT_REGEXP. */
  public static final String NAME_MTAS_DISTINCT_REGEXP = "regexp";

  /** The Constant NAME_MTAS_DISTINCT_NUMBER. */
  public static final String NAME_MTAS_DISTINCT_NUMBER = "number";

  /**
   * Instantiates a new mtas solr component distinct.
   *
   * @param searchComponent the search component
   */
  public MtasSolrComponentDistinct(MtasSolrSearchComponent searchComponent) {
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
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_DISTINCT);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] prefixes = new String[ids.size()];
      String[] types = new String[ids.size()];
      String[] regexps = new String[ids.size()];
      String[] numbers = new String[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DISTINCT + "." + id + "." + NAME_MTAS_DISTINCT_FIELD,
            null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_DISTINCT + "." + id + "." + NAME_MTAS_DISTINCT_KEY,
                String.valueOf(tmpCounter))
            .trim();
        prefixes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DISTINCT + "." + id + "." + NAME_MTAS_DISTINCT_PREFIX,
            null);
        types[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DISTINCT + "." + id + "." + NAME_MTAS_DISTINCT_TYPE,
            null);
        regexps[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DISTINCT + "." + id + "." + NAME_MTAS_DISTINCT_REGEXP,
            null);
        numbers[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_DISTINCT + "." + id + "." + NAME_MTAS_DISTINCT_NUMBER,
            null);
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doDistinct = true;
      rb.setNeedDocList(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas kwic");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      MtasSolrResultUtil.compareAndCheck(keys, fields, NAME_MTAS_DISTINCT_KEY,
          NAME_MTAS_DISTINCT_FIELD, true);
      MtasSolrResultUtil.compareAndCheck(prefixes, fields,
          NAME_MTAS_DISTINCT_PREFIX, NAME_MTAS_DISTINCT_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(types, fields, NAME_MTAS_DISTINCT_TYPE,
          NAME_MTAS_DISTINCT_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(regexps, fields,
          NAME_MTAS_DISTINCT_REGEXP, NAME_MTAS_DISTINCT_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(numbers, fields,
          NAME_MTAS_DISTINCT_NUMBER, NAME_MTAS_DISTINCT_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + fields[i] + ":" + prefixes[i]
            : keys[i].trim();
        String prefix = prefixes[i];
        String type = types[i];
        String regexp = regexps[i];
        int number = Math.max(0, (numbers[i] == null) || (numbers[i].isEmpty()) ? 0
            : Integer.parseInt(numbers[i]));
        mtasFields.list.get(fields[i]).distinctList
            .add(new ComponentDistinct(key, prefix, type, regexp, number));
      }
    }
  }

  /**
   * Creates the.
   *
   * @param distinct the distinct
   * @return the simple ordered map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public SimpleOrderedMap<Object> create(ComponentDistinct distinct)
      throws IOException {
    SimpleOrderedMap<Object> mtasDistinctResponse = new SimpleOrderedMap<>();
    mtasDistinctResponse.add("key", distinct.key);
    ArrayList<NamedList<Object>> mtasDistinctItemResponses = new ArrayList<NamedList<Object>>();
    for (int docId : distinct.stats.keySet()) {
      NamedList<Object> mtasDistinctItemResponse = new SimpleOrderedMap<>();
      MtasDataCollector<?, ?> stats = distinct.stats.get(docId);
      MtasDataCollector<?, ?> list = null;
      if(distinct.list!=null){
        list = distinct.list.get(docId);
      }
      mtasDistinctItemResponse.add("stats", new MtasSolrResult(stats,
          stats.getDataType(), stats.getStatsType(), stats.statsItems, null));
      mtasDistinctItemResponse.add("documentKey",
          distinct.uniqueKey.get(docId));
      if(list!=null) {
        mtasDistinctItemResponse.add("list",
            new MtasSolrResult(list, list.getDataType(), list.getStatsType(), list.statsItems, null));      
      }      
      //add
      mtasDistinctItemResponses.add(mtasDistinctItemResponse);
    }
    mtasDistinctResponse.add("list", mtasDistinctItemResponses);
    MtasSolrResultUtil.rewrite(mtasDistinctResponse);
    return mtasDistinctResponse;
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
      if (sreq.params.getBool(PARAM_MTAS_DISTINCT, false)) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
          // do nothing
        } else {
          Set<String> keys = MtasSolrResultUtil
              .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_DISTINCT);
          sreq.params.remove(PARAM_MTAS_DISTINCT);
          for (String key : keys) {
            sreq.params.remove(PARAM_MTAS_DISTINCT + "." + key + "."
                + NAME_MTAS_DISTINCT_FIELD);
            sreq.params.remove(
                PARAM_MTAS_DISTINCT + "." + key + "." + NAME_MTAS_DISTINCT_KEY);
            sreq.params.remove(PARAM_MTAS_DISTINCT + "." + key + "."
                + NAME_MTAS_DISTINCT_PREFIX);
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
              && sreq.params.getBool(PARAM_MTAS_DISTINCT, false)) {
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
        ArrayList<Object> mtasResponseDistinct;
        try {
          mtasResponseDistinct = (ArrayList<Object>) mtasResponse
              .get("distinct");
          if (mtasResponseDistinct != null) {
            MtasSolrResultUtil.rewrite(mtasResponseDistinct);
          }
        } catch (ClassCastException e) {
          mtasResponseDistinct = null;
        }
      }
    } catch (ClassCastException e) {
      mtasResponse = null;
    }
  }

}
