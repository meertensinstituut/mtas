package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;

import mtas.analysis.token.MtasToken;
import mtas.codec.util.CodecUtil;
import mtas.search.spans.util.MtasSpanQuery;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentList;
import mtas.codec.util.CodecComponent.ListHit;
import mtas.codec.util.CodecComponent.ListToken;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentList.
 */
public class MtasSolrComponentList implements MtasSolrComponent<ComponentList> {

  private static Log log = LogFactory.getLog(MtasSolrComponentList.class);

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant PARAM_MTAS_LIST. */
  public static final String PARAM_MTAS_LIST = MtasSolrSearchComponent.PARAM_MTAS
      + ".list";

  /** The Constant NAME_MTAS_LIST_FIELD. */
  public static final String NAME_MTAS_LIST_FIELD = "field";

  /** The Constant NAME_MTAS_LIST_QUERY_TYPE. */
  public static final String NAME_MTAS_LIST_QUERY_TYPE = "query.type";

  /** The Constant NAME_MTAS_LIST_QUERY_VALUE. */
  public static final String NAME_MTAS_LIST_QUERY_VALUE = "query.value";

  /** The Constant NAME_MTAS_LIST_QUERY_PREFIX. */
  public static final String NAME_MTAS_LIST_QUERY_PREFIX = "query.prefix";

  /** The Constant NAME_MTAS_LIST_QUERY_IGNORE. */
  public static final String NAME_MTAS_LIST_QUERY_IGNORE = "query.ignore";
  public static final String NAME_MTAS_LIST_QUERY_MAXIMUM_IGNORE_LENGTH = "query.maximumIgnoreLength";

  /** The Constant NAME_MTAS_LIST_QUERY_VARIABLE. */
  public static final String NAME_MTAS_LIST_QUERY_VARIABLE = "query.variable";

  /** The Constant SUBNAME_MTAS_LIST_QUERY_VARIABLE_NAME. */
  public static final String SUBNAME_MTAS_LIST_QUERY_VARIABLE_NAME = "name";

  /** The Constant SUBNAME_MTAS_LIST_QUERY_VARIABLE_VALUE. */
  public static final String SUBNAME_MTAS_LIST_QUERY_VARIABLE_VALUE = "value";

  /** The Constant NAME_MTAS_LIST_KEY. */
  public static final String NAME_MTAS_LIST_KEY = "key";

  /** The Constant NAME_MTAS_LIST_PREFIX. */
  public static final String NAME_MTAS_LIST_PREFIX = "prefix";

  /** The Constant NAME_MTAS_LIST_START. */
  public static final String NAME_MTAS_LIST_START = "start";

  /** The Constant NAME_MTAS_LIST_NUMBER. */
  public static final String NAME_MTAS_LIST_NUMBER = "number";

  /** The Constant NAME_MTAS_LIST_LEFT. */
  public static final String NAME_MTAS_LIST_LEFT = "left";

  /** The Constant NAME_MTAS_LIST_RIGHT. */
  public static final String NAME_MTAS_LIST_RIGHT = "right";

  /** The Constant NAME_MTAS_LIST_OUTPUT. */
  public static final String NAME_MTAS_LIST_OUTPUT = "output";

  /**
   * Instantiates a new mtas solr component list.
   *
   * @param searchComponent
   *          the search component
   */
  public MtasSolrComponentList(MtasSolrSearchComponent searchComponent) {
    this.searchComponent = searchComponent;
  }

  /**
   * Prepare.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void prepare(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = MtasSolrResultUtil
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_LIST);
    if (!ids.isEmpty()) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] queryTypes = new String[ids.size()];
      String[] queryValues = new String[ids.size()];
      String[] queryPrefixes = new String[ids.size()];
      HashMap<String, String[]>[] queryVariables = new HashMap[ids.size()];
      String[] queryIgnores = new String[ids.size()];
      String[] queryMaximumIgnoreLengths = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] prefixes = new String[ids.size()];
      String[] starts = new String[ids.size()];
      String[] numbers = new String[ids.size()];
      String[] lefts = new String[ids.size()];
      String[] rights = new String[ids.size()];
      String[] outputs = new String[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_FIELD, null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_KEY,
                String.valueOf(tmpCounter))
            .trim();
        queryTypes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_QUERY_TYPE, null);
        queryValues[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_QUERY_VALUE,
            null);
        queryPrefixes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_QUERY_PREFIX,
            null);
        queryIgnores[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_QUERY_IGNORE,
            null);
        queryMaximumIgnoreLengths[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_LIST + "." + id + "."
                + NAME_MTAS_LIST_QUERY_MAXIMUM_IGNORE_LENGTH, null);
        Set<String> vIds = MtasSolrResultUtil.getIdsFromParameters(
            rb.req.getParams(),
            PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_QUERY_VARIABLE);
        queryVariables[tmpCounter] = new HashMap<>();
        if (!vIds.isEmpty()) {
          HashMap<String, ArrayList<String>> tmpVariables = new HashMap<>();
          for (String vId : vIds) {
            String name = rb.req.getParams().get(
                PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_QUERY_VARIABLE
                    + "." + vId + "." + SUBNAME_MTAS_LIST_QUERY_VARIABLE_NAME,
                null);
            if (name != null) {
              if (!tmpVariables.containsKey(name)) {
                tmpVariables.put(name, new ArrayList<String>());
              }
              String value = rb.req.getParams()
                  .get(PARAM_MTAS_LIST + "." + id + "."
                      + NAME_MTAS_LIST_QUERY_VARIABLE + "." + vId + "."
                      + SUBNAME_MTAS_LIST_QUERY_VARIABLE_VALUE, null);
              if (value != null) {
                ArrayList<String> list = new ArrayList<>();
                String[] subList = value.split("(?<!\\\\),");
                for (int i = 0; i < subList.length; i++) {
                  list.add(
                      subList[i].replace("\\,", ",").replace("\\\\", "\\"));
                }
                tmpVariables.get(name).addAll(list);
              }
            }
          }
          for (Entry<String, ArrayList<String>> entry : tmpVariables
              .entrySet()) {
            queryVariables[tmpCounter].put(entry.getKey(),
                entry.getValue().toArray(new String[entry.getValue().size()]));
          }
        }
        prefixes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_PREFIX, null);
        starts[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_START, null);
        numbers[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_NUMBER, null);
        lefts[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_LEFT, null);
        rights[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_RIGHT, null);
        outputs[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_LIST + "." + id + "." + NAME_MTAS_LIST_OUTPUT, null);
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doList = true;
      rb.setNeedDocSet(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas list");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(uniqueKeyField));
        }
      }
      MtasSolrResultUtil.compareAndCheck(keys, fields, NAME_MTAS_LIST_KEY,
          NAME_MTAS_LIST_FIELD, true);
      MtasSolrResultUtil.compareAndCheck(prefixes, queryValues,
          NAME_MTAS_LIST_QUERY_VALUE, NAME_MTAS_LIST_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(prefixes, queryTypes,
          NAME_MTAS_LIST_QUERY_TYPE, NAME_MTAS_LIST_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(prefixes, queryPrefixes,
          NAME_MTAS_LIST_QUERY_PREFIX, NAME_MTAS_LIST_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(prefixes, queryIgnores,
          NAME_MTAS_LIST_QUERY_IGNORE, NAME_MTAS_LIST_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(prefixes, queryMaximumIgnoreLengths,
          NAME_MTAS_LIST_QUERY_MAXIMUM_IGNORE_LENGTH, NAME_MTAS_LIST_FIELD,
          false);
      MtasSolrResultUtil.compareAndCheck(prefixes, fields,
          NAME_MTAS_LIST_PREFIX, NAME_MTAS_LIST_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(starts, fields, NAME_MTAS_LIST_START,
          NAME_MTAS_LIST_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(numbers, fields, NAME_MTAS_LIST_NUMBER,
          NAME_MTAS_LIST_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(lefts, fields, NAME_MTAS_LIST_LEFT,
          NAME_MTAS_LIST_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(rights, fields, NAME_MTAS_LIST_RIGHT,
          NAME_MTAS_LIST_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(outputs, fields, NAME_MTAS_LIST_OUTPUT,
          NAME_MTAS_LIST_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        ComponentField cf = mtasFields.list.get(fields[i]);
        Integer maximumIgnoreLength = (queryMaximumIgnoreLengths[i] == null)
            ? null : Integer.parseInt(queryMaximumIgnoreLengths[i]);
        MtasSpanQuery q = MtasSolrResultUtil.constructQuery(queryValues[i],
            queryTypes[i], queryPrefixes[i], queryVariables[i], fields[i],
            queryIgnores[i], maximumIgnoreLength);
        // minimize number of queries
        if (cf.spanQueryList.contains(q)) {
          q = cf.spanQueryList.get(cf.spanQueryList.indexOf(q));
        } else {
          cf.spanQueryList.add(q);
        }
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + fields[i] + ":" + queryValues[i] + ":"
                + queryPrefixes[i]
            : keys[i].trim();
        String prefix = prefixes[i];
        int start = (starts[i] == null) || (starts[i].isEmpty()) ? 0
            : Integer.parseInt(starts[i]);
        int number = (numbers[i] == null) || (numbers[i].isEmpty()) ? 10
            : Integer.parseInt(numbers[i]);
        int left = (lefts[i] == null) || lefts[i].isEmpty() ? 0
            : Integer.parseInt(lefts[i]);
        int right = (rights[i] == null) || rights[i].isEmpty() ? 0
            : Integer.parseInt(rights[i]);
        String output = outputs[i];
        mtasFields.list.get(fields[i]).listList.add(new ComponentList(q,
            fields[i], queryValues[i], queryTypes[i], queryPrefixes[i],
            queryVariables[i], queryIgnores[i], queryMaximumIgnoreLengths[i],
            key, prefix, start, number, left, right, output));
      }
    }
  }

  /**
   * Modify request.
   *
   * @param rb
   *          the rb
   * @param who
   *          the who
   * @param sreq
   *          the sreq
   */
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq) {
    if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
        && sreq.params.getBool(PARAM_MTAS_LIST, false)) {
      // compute keys
      Set<String> keys = MtasSolrResultUtil
          .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_LIST);
      if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
        for (String key : keys) {
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_PREFIX);
          sreq.params
              .remove(PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_START);
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_NUMBER);
          sreq.params
              .remove(PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_LEFT);
          sreq.params
              .remove(PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_RIGHT);
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_OUTPUT);
          // don't get data
          sreq.params.add(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_NUMBER, "0");
        }
      } else {
        sreq.params.remove(PARAM_MTAS_LIST);
        for (String key : keys) {
          sreq.params
              .remove(PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_FIELD);
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_QUERY_VALUE);
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_QUERY_TYPE);
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_QUERY_PREFIX);
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_QUERY_IGNORE);
          sreq.params.remove(PARAM_MTAS_LIST + "." + key + "."
              + NAME_MTAS_LIST_QUERY_MAXIMUM_IGNORE_LENGTH);
          Set<String> subKeys = MtasSolrResultUtil
              .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_LIST + "."
                  + key + "." + NAME_MTAS_LIST_QUERY_VARIABLE);
          for (String subKey : subKeys) {
            sreq.params.remove(PARAM_MTAS_LIST + "." + key + "."
                + NAME_MTAS_LIST_QUERY_VARIABLE + "." + subKey + "."
                + SUBNAME_MTAS_LIST_QUERY_VARIABLE_NAME);
            sreq.params.remove(PARAM_MTAS_LIST + "." + key + "."
                + NAME_MTAS_LIST_QUERY_VARIABLE + "." + subKey + "."
                + SUBNAME_MTAS_LIST_QUERY_VARIABLE_VALUE);
          }
          sreq.params
              .remove(PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_KEY);
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_PREFIX);
          sreq.params
              .remove(PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_START);
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_NUMBER);
          sreq.params
              .remove(PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_LEFT);
          sreq.params
              .remove(PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_RIGHT);
          sreq.params.remove(
              PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_OUTPUT);
        }
      }
    }
  }

  /**
   * Distributed process.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   */
  @SuppressWarnings("unchecked")
  public void distributedProcess(ResponseBuilder rb,
      ComponentFields mtasFields) {

    if (mtasFields.doList) {
      // compute total from shards
      HashMap<String, HashMap<String, Integer>> listShardTotals = new HashMap<>();
      for (ShardRequest sreq : rb.finished) {
        if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
            && sreq.params.getBool(PARAM_MTAS_LIST, false)) {
          for (ShardResponse response : sreq.responses) {
            NamedList<Object> result = response.getSolrResponse().getResponse();
            try {
              ArrayList<NamedList<Object>> data = (ArrayList<NamedList<Object>>) result
                  .findRecursive("mtas", "list");
              if (data != null) {
                for (NamedList<Object> dataItem : data) {
                  Object key = dataItem.get("key");
                  Object total = dataItem.get("total");
                  if ((key != null) && (key instanceof String)
                      && (total != null) && (total instanceof Integer)) {
                    if (!listShardTotals.containsKey(key)) {
                      listShardTotals.put((String) key,
                          new HashMap<String, Integer>());
                    }
                    HashMap<String, Integer> listShardTotal = listShardTotals
                        .get(key);
                    listShardTotal.put(response.getShard(), (Integer) total);
                  }
                }
              }
            } catch (ClassCastException e) {
              log.debug(e);
            }
          }
        }
      }
      // compute shard requests
      HashMap<String, ModifiableSolrParams> shardRequests = new HashMap<>();
      int requestId = 0;
      for (String field : mtasFields.list.keySet()) {
        for (ComponentList list : mtasFields.list.get(field).listList) {
          requestId++;
          if (listShardTotals.containsKey(list.key) && (list.number > 0)) {
            Integer start = list.start;
            Integer number = list.number;
            HashMap<String, Integer> totals = listShardTotals.get(list.key);
            for (int i = 0; i < rb.shards.length; i++) {
              if (number < 0) {
                break;
              }
              int subTotal = totals.get(rb.shards[i]);
              // System.out.println(i + " : " + rb.shards[i] + " : "
              // + totals.get(rb.shards[i]) + " - " + start + " " + number);
              if ((start >= 0) && (start < subTotal)) {
                ModifiableSolrParams params;
                if (!shardRequests.containsKey(rb.shards[i])) {
                  shardRequests.put(rb.shards[i], new ModifiableSolrParams());
                }
                params = shardRequests.get(rb.shards[i]);
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_FIELD, list.field);
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_QUERY_VALUE, list.queryValue);
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_QUERY_TYPE, list.queryType);
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_QUERY_PREFIX, list.queryPrefix);
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_QUERY_IGNORE, list.queryIgnore);
                params.add(
                    PARAM_MTAS_LIST + "." + requestId + "."
                        + NAME_MTAS_LIST_QUERY_MAXIMUM_IGNORE_LENGTH,
                    list.queryMaximumIgnoreLength);
                int subRequestId = 0;
                for (String name : list.queryVariables.keySet()) {
                  if (list.queryVariables.get(name) == null
                      || list.queryVariables.get(name).length == 0) {
                    params.add(
                        PARAM_MTAS_LIST + "." + requestId + "."
                            + NAME_MTAS_LIST_QUERY_VARIABLE + "." + subRequestId
                            + "." + SUBNAME_MTAS_LIST_QUERY_VARIABLE_NAME,
                        name);
                    subRequestId++;
                  } else {
                    for (String value : list.queryVariables.get(name)) {
                      params.add(PARAM_MTAS_LIST + "." + requestId + "."
                          + NAME_MTAS_LIST_QUERY_VARIABLE + "." + subRequestId
                          + "." + SUBNAME_MTAS_LIST_QUERY_VARIABLE_NAME, name);
                      params.add(PARAM_MTAS_LIST + "." + requestId + "."
                          + NAME_MTAS_LIST_QUERY_VARIABLE + "." + subRequestId
                          + "." + SUBNAME_MTAS_LIST_QUERY_VARIABLE_VALUE,
                          value);
                      subRequestId++;
                    }
                  }
                }
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_KEY, list.key);
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_PREFIX, list.prefix);
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_START, Integer.toString(start));
                params.add(
                    PARAM_MTAS_LIST + "." + requestId + "."
                        + NAME_MTAS_LIST_NUMBER,
                    Integer.toString(Math.min(number, (subTotal - start))));
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_LEFT, Integer.toString(list.left));
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_RIGHT, Integer.toString(list.right));
                params.add(PARAM_MTAS_LIST + "." + requestId + "."
                    + NAME_MTAS_LIST_OUTPUT, list.output);
                number -= (subTotal - start);
                start = 0;
              } else {
                start -= subTotal;
              }
            }
          }
        }
      }

      for (Entry<String, ModifiableSolrParams> entry : shardRequests
          .entrySet()) {
        ShardRequest sreq = new ShardRequest();
        sreq.shards = new String[] { entry.getKey() };
        sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
        sreq.params = new ModifiableSolrParams();
        sreq.params.add("fq", rb.req.getParams().getParams("fq"));
        sreq.params.add("q", rb.req.getParams().getParams("q"));
        sreq.params.add("cache", rb.req.getParams().getParams("cache"));
        sreq.params.add("rows", "0");
        sreq.params.add(MtasSolrSearchComponent.PARAM_MTAS, rb.req
            .getOriginalParams().getParams(MtasSolrSearchComponent.PARAM_MTAS));
        sreq.params.add(PARAM_MTAS_LIST,
            rb.req.getOriginalParams().getParams(PARAM_MTAS_LIST));
        sreq.params.add(entry.getValue());
        rb.addRequest(searchComponent, sreq);
      }
    }
  }

  /**
   * Creates the.
   *
   * @param list
   *          the list
   * @return the simple ordered map
   */
  public SimpleOrderedMap<Object> create(ComponentList list, Boolean encode) {
    SimpleOrderedMap<Object> mtasListResponse = new SimpleOrderedMap<>();
    mtasListResponse.add("key", list.key);
    if (list.number == 0) {
      mtasListResponse.add("total", list.total);
    }
    if (list.output != null) {
      ArrayList<NamedList<Object>> mtasListItemResponses = new ArrayList<>();
      if (list.output.equals(ComponentList.LIST_OUTPUT_HIT)) {
        mtasListResponse.add("number", list.hits.size());
        for (ListHit hit : list.hits) {
          NamedList<Object> mtasListItemResponse = new SimpleOrderedMap<>();
          mtasListItemResponse.add("documentKey",
              list.uniqueKey.get(hit.docId));
          mtasListItemResponse.add("documentHitPosition", hit.docPosition);
          mtasListItemResponse.add("documentHitTotal",
              list.subTotal.get(hit.docId));
          mtasListItemResponse.add("documentMinPosition",
              list.minPosition.get(hit.docId));
          mtasListItemResponse.add("documentMaxPosition",
              list.maxPosition.get(hit.docId));
          mtasListItemResponse.add("startPosition", hit.startPosition);
          mtasListItemResponse.add("endPosition", hit.endPosition);

          TreeMap<Integer, ArrayList<ArrayList<String>>> hitData = new TreeMap<>();
          TreeMap<Integer, ArrayList<ArrayList<String>>> leftData = null;
          TreeMap<Integer, ArrayList<ArrayList<String>>> rightData = null;
          if (list.left > 0) {
            leftData = new TreeMap<>();
          }
          if (list.right > 0) {
            rightData = new TreeMap<>();
          }
          for (int position = Math.max(0,
              hit.startPosition - list.left); position <= (hit.endPosition
                  + list.right); position++) {
            ArrayList<ArrayList<String>> hitDataItem = new ArrayList<>();
            if (hit.hits.containsKey(position)) {
              for (String term : hit.hits.get(position)) {
                ArrayList<String> hitDataSubItem = new ArrayList<>();
                hitDataSubItem.add(CodecUtil.termPrefix(term));
                hitDataSubItem.add(CodecUtil.termValue(term));
                hitDataItem.add(hitDataSubItem);
              }
            }
            if (position < hit.startPosition) {
              if (leftData != null) {
                leftData.put(position, hitDataItem);
              }
            } else if (position > hit.endPosition) {
              if (rightData != null) {
                rightData.put(position, hitDataItem);
              }
            } else {
              hitData.put(position, hitDataItem);
            }
          }
          if (list.left > 0) {
            mtasListItemResponse.add("left", leftData);
          }
          mtasListItemResponse.add("hit", hitData);
          if (list.right > 0) {
            mtasListItemResponse.add("right", rightData);
          }
          mtasListItemResponses.add(mtasListItemResponse);
        }
      } else if (list.output.equals(ComponentList.LIST_OUTPUT_TOKEN)) {
        mtasListResponse.add("number", list.tokens.size());
        for (ListToken tokenHit : list.tokens) {
          NamedList<Object> mtasListItemResponse = new SimpleOrderedMap<>();
          mtasListItemResponse.add("documentKey",
              list.uniqueKey.get(tokenHit.docId));
          mtasListItemResponse.add("documentHitPosition", tokenHit.docPosition);
          mtasListItemResponse.add("documentHitTotal",
              list.subTotal.get(tokenHit.docId));
          mtasListItemResponse.add("documentMinPosition",
              list.minPosition.get(tokenHit.docId));
          mtasListItemResponse.add("documentMaxPosition",
              list.maxPosition.get(tokenHit.docId));
          mtasListItemResponse.add("startPosition", tokenHit.startPosition);
          mtasListItemResponse.add("endPosition", tokenHit.endPosition);

          ArrayList<NamedList<Object>> mtasListItemResponseItemTokens = new ArrayList<>();
          for (MtasToken token : tokenHit.tokens) {
            NamedList<Object> mtasListItemResponseItemToken = new SimpleOrderedMap<>();
            if (token.getId() != null) {
              mtasListItemResponseItemToken.add("mtasId", token.getId());
            }
            mtasListItemResponseItemToken.add("prefix", token.getPrefix());
            mtasListItemResponseItemToken.add("value", token.getPostfix());
            if (token.getPositionStart() != null) {
              mtasListItemResponseItemToken.add("positionStart",
                  token.getPositionStart());
              mtasListItemResponseItemToken.add("positionEnd",
                  token.getPositionEnd());
            }
            if (token.getPositions() != null) {
              mtasListItemResponseItemToken.add("positions",
                  token.getPositions());
            }
            if (token.getParentId() != null) {
              mtasListItemResponseItemToken.add("parentMtasId",
                  token.getParentId());
            }
            if (token.getPayload() != null) {
              mtasListItemResponseItemToken.add("payload", token.getPayload());
            }
            if (token.getOffsetStart() != null) {
              mtasListItemResponseItemToken.add("offsetStart",
                  token.getOffsetStart());
              mtasListItemResponseItemToken.add("offsetEnd",
                  token.getOffsetEnd());
            }
            if (token.getRealOffsetStart() != null) {
              mtasListItemResponseItemToken.add("realOffsetStart",
                  token.getRealOffsetStart());
              mtasListItemResponseItemToken.add("realOffsetEnd",
                  token.getRealOffsetEnd());
            }
            mtasListItemResponseItemTokens.add(mtasListItemResponseItemToken);
          }
          mtasListItemResponse.add("tokens", mtasListItemResponseItemTokens);
          mtasListItemResponses.add(mtasListItemResponse);
        }
      }
      mtasListResponse.add("list", mtasListItemResponses);
    }
    return mtasListResponse;
  }

  /**
   * Finish stage.
   *
   * @param rb
   *          the rb
   */
  public void finishStage(ResponseBuilder rb) {
    if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
        && rb.stage >= ResponseBuilder.STAGE_EXECUTE_QUERY
        && rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
      for (ShardRequest sreq : rb.finished) {
        if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
            && sreq.params.getBool(PARAM_MTAS_LIST, false)) {
          // nothing to do
        }
      }
    }
  }

}
