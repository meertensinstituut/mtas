package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;

import mtas.codec.util.CodecUtil;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentTermVector;
import mtas.codec.util.CodecComponent.SubComponentFunction;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.codec.util.collector.MtasDataItem.NumberComparator;
import mtas.parser.function.ParseException;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentTermvector.
 */
public class MtasSolrComponentTermvector {

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant PARAM_MTAS_TERMVECTOR. */
  public static final String PARAM_MTAS_TERMVECTOR = MtasSolrSearchComponent.PARAM_MTAS
      + ".termvector";

  /** The Constant NAME_MTAS_TERMVECTOR_FIELD. */
  public static final String NAME_MTAS_TERMVECTOR_FIELD = "field";

  /** The Constant NAME_MTAS_TERMVECTOR_KEY. */
  public static final String NAME_MTAS_TERMVECTOR_KEY = "key";

  /** The Constant NAME_MTAS_TERMVECTOR_PREFIX. */
  public static final String NAME_MTAS_TERMVECTOR_PREFIX = "prefix";

  /** The Constant NAME_MTAS_TERMVECTOR_REGEXP. */
  public static final String NAME_MTAS_TERMVECTOR_REGEXP = "regexp";

  /** The Constant NAME_MTAS_TERMVECTOR_FULL. */
  public static final String NAME_MTAS_TERMVECTOR_FULL = "full";

  /** The Constant NAME_MTAS_TERMVECTOR_TYPE. */
  public static final String NAME_MTAS_TERMVECTOR_TYPE = "type";

  /** The Constant NAME_MTAS_TERMVECTOR_SORT_TYPE. */
  public static final String NAME_MTAS_TERMVECTOR_SORT_TYPE = "sort.type";

  /** The Constant NAME_MTAS_TERMVECTOR_SORT_DIRECTION. */
  public static final String NAME_MTAS_TERMVECTOR_SORT_DIRECTION = "sort.direction";

  public static final String NAME_MTAS_TERMVECTOR_START = "start";

  /** The Constant NAME_MTAS_TERMVECTOR_NUMBER. */
  public static final String NAME_MTAS_TERMVECTOR_NUMBER = "number";

  /** The Constant NAME_MTAS_TERMVECTOR_NUMBER_SHARDS. */
  public static final String NAME_MTAS_TERMVECTOR_NUMBER_SHARDS = "number.shards";

  /** The Constant NAME_MTAS_TERMVECTOR_FUNCTION. */
  public static final String NAME_MTAS_TERMVECTOR_FUNCTION = "function";

  /** The Constant NAME_MTAS_TERMVECTOR_FUNCTION_EXPRESSION. */
  public static final String NAME_MTAS_TERMVECTOR_FUNCTION_EXPRESSION = "expression";

  /** The Constant NAME_MTAS_TERMVECTOR_FUNCTION_KEY. */
  public static final String NAME_MTAS_TERMVECTOR_FUNCTION_KEY = "key";

  /** The Constant NAME_MTAS_TERMVECTOR_FUNCTION_TYPE. */
  public static final String NAME_MTAS_TERMVECTOR_FUNCTION_TYPE = "type";

  /** The Constant NAME_MTAS_TERMVECTOR_BOUNDARY. */
  public static final String NAME_MTAS_TERMVECTOR_BOUNDARY = "boundary";

  /** The Constant NAME_MTAS_TERMVECTOR_LIST. */
  public static final String NAME_MTAS_TERMVECTOR_LIST = "list";

  public static final String NAME_MTAS_TERMVECTOR_LIST_REGEXP = "listRegexp";

  public static final String NAME_MTAS_TERMVECTOR_IGNORE_REGEXP = "ignoreRegexp";

  public static final String NAME_MTAS_TERMVECTOR_IGNORE_LIST = "ignoreList";

  public static final String NAME_MTAS_TERMVECTOR_IGNORE_LIST_REGEXP = "ignoreListRegexp";

  /** The Constant SHARD_NUMBER_MULTIPLIER. */
  private static final int SHARD_NUMBER_MULTIPLIER = 2;

  /** The Constant DEFAULT_NUMBER. */
  private static final int DEFAULT_NUMBER = 10;

  /**
   * Instantiates a new mtas solr component termvector.
   *
   * @param searchComponent
   *          the search component
   */
  public MtasSolrComponentTermvector(MtasSolrSearchComponent searchComponent) {
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
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_TERMVECTOR);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] prefixes = new String[ids.size()];
      String[] regexps = new String[ids.size()];
      String[] fulls = new String[ids.size()];
      String[] sortTypes = new String[ids.size()];
      String[] sortDirections = new String[ids.size()];
      String[] types = new String[ids.size()];
      String[] startValues = new String[ids.size()];
      String[] numbers = new String[ids.size()];
      String[] numberShards = new String[ids.size()];
      String[][] functionExpressions = new String[ids.size()][];
      String[][] functionKeys = new String[ids.size()][];
      String[][] functionTypes = new String[ids.size()][];
      String[] boundaries = new String[ids.size()];
      String[] lists = new String[ids.size()];
      Boolean[] listRegexps = new Boolean[ids.size()];
      String[] ignoreRegexps = new String[ids.size()];
      String[] ignoreLists = new String[ids.size()];
      Boolean[] ignoreListRegexps = new Boolean[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_TERMVECTOR + "." + id + "." + NAME_MTAS_TERMVECTOR_FIELD,
            null);
        keys[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_TERMVECTOR + "." + id + "." + NAME_MTAS_TERMVECTOR_KEY,
            String.valueOf(tmpCounter)).trim();
        prefixes[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_TERMVECTOR
            + "." + id + "." + NAME_MTAS_TERMVECTOR_PREFIX, null);
        regexps[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_TERMVECTOR + "."
            + id + "." + NAME_MTAS_TERMVECTOR_REGEXP, null);
        fulls[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_TERMVECTOR + "." + id + "." + NAME_MTAS_TERMVECTOR_FULL,
            null);
        sortTypes[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_TERMVECTOR
            + "." + id + "." + NAME_MTAS_TERMVECTOR_SORT_TYPE, null);
        sortDirections[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                + NAME_MTAS_TERMVECTOR_SORT_DIRECTION, null);
        startValues[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_TERMVECTOR + "." + id + "." + NAME_MTAS_TERMVECTOR_START,
            null);
        numbers[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_TERMVECTOR + "."
            + id + "." + NAME_MTAS_TERMVECTOR_NUMBER, null);
        numberShards[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_TERMVECTOR
            + "." + id + "." + NAME_MTAS_TERMVECTOR_NUMBER_SHARDS, null);
        types[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_TERMVECTOR + "." + id + "." + NAME_MTAS_TERMVECTOR_TYPE,
            null);
        Set<String> functionIds = MtasSolrResultUtil
            .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_TERMVECTOR
                + "." + id + "." + NAME_MTAS_TERMVECTOR_FUNCTION);
        functionExpressions[tmpCounter] = new String[functionIds.size()];
        functionKeys[tmpCounter] = new String[functionIds.size()];
        functionTypes[tmpCounter] = new String[functionIds.size()];
        int tmpSubCounter = 0;
        for (String functionId : functionIds) {
          functionKeys[tmpCounter][tmpSubCounter] = rb.req.getParams()
              .get(
                  PARAM_MTAS_TERMVECTOR + "." + id + "."
                      + NAME_MTAS_TERMVECTOR_FUNCTION + "." + functionId + "."
                      + NAME_MTAS_TERMVECTOR_FUNCTION_KEY,
                  String.valueOf(tmpSubCounter))
              .trim();
          functionExpressions[tmpCounter][tmpSubCounter] = rb.req.getParams()
              .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION + "." + functionId + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION_EXPRESSION, null);
          functionTypes[tmpCounter][tmpSubCounter] = rb.req.getParams()
              .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION + "." + functionId + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION_TYPE, null);
          tmpSubCounter++;
        }
        boundaries[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_TERMVECTOR
            + "." + id + "." + NAME_MTAS_TERMVECTOR_BOUNDARY, null);
        lists[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_TERMVECTOR + "." + id + "." + NAME_MTAS_TERMVECTOR_LIST);
        listRegexps[tmpCounter] = rb.req.getParams()
            .getBool(PARAM_MTAS_TERMVECTOR + "." + id + "."
                + NAME_MTAS_TERMVECTOR_LIST_REGEXP, false);
        ignoreRegexps[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_TERMVECTOR
            + "." + id + "." + NAME_MTAS_TERMVECTOR_IGNORE_REGEXP, null);
        ignoreLists[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_TERMVECTOR
            + "." + id + "." + NAME_MTAS_TERMVECTOR_IGNORE_LIST, null);
        ignoreListRegexps[tmpCounter] = rb.req.getParams()
            .getBool(PARAM_MTAS_TERMVECTOR + "." + id + "."
                + NAME_MTAS_TERMVECTOR_IGNORE_LIST_REGEXP, false);
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doTermVector = true;
      rb.setNeedDocSet(true);
      // init and checks
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas termvector");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      MtasSolrResultUtil.compareAndCheck(keys, fields, NAME_MTAS_TERMVECTOR_KEY,
          NAME_MTAS_TERMVECTOR_FIELD, true);
      MtasSolrResultUtil.compareAndCheck(prefixes, fields,
          NAME_MTAS_TERMVECTOR_PREFIX, NAME_MTAS_TERMVECTOR_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(regexps, fields,
          NAME_MTAS_TERMVECTOR_REGEXP, NAME_MTAS_TERMVECTOR_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(types, fields,
          NAME_MTAS_TERMVECTOR_TYPE, NAME_MTAS_TERMVECTOR_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(sortTypes, fields,
          NAME_MTAS_TERMVECTOR_SORT_TYPE, NAME_MTAS_TERMVECTOR_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(sortDirections, fields,
          NAME_MTAS_TERMVECTOR_SORT_DIRECTION, NAME_MTAS_TERMVECTOR_FIELD,
          false);
      MtasSolrResultUtil.compareAndCheck(numbers, fields,
          NAME_MTAS_TERMVECTOR_NUMBER, NAME_MTAS_TERMVECTOR_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(boundaries, fields,
          NAME_MTAS_TERMVECTOR_BOUNDARY, NAME_MTAS_TERMVECTOR_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(lists, fields,
          NAME_MTAS_TERMVECTOR_LIST, NAME_MTAS_TERMVECTOR_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(ignoreRegexps, fields,
          NAME_MTAS_TERMVECTOR_IGNORE_REGEXP, NAME_MTAS_TERMVECTOR_FIELD,
          false);
      MtasSolrResultUtil.compareAndCheck(ignoreLists, fields,
          NAME_MTAS_TERMVECTOR_IGNORE_LIST, NAME_MTAS_TERMVECTOR_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        String field = fields[i];
        String prefix = (prefixes[i] == null) || (prefixes[i].isEmpty()) ? null
            : prefixes[i].trim();
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + field + ":" + prefix : keys[i].trim();
        String regexp = (regexps[i] == null) || (regexps[i].isEmpty()) ? null
            : regexps[i].trim();
        Boolean full = (fulls[i] == null) || (!fulls[i].equals("true")) ? false
            : true;
        String startValue = (startValues[i] == null)
            || (startValues[i].isEmpty()) ? null : startValues[i].trim();
        int listNumber = (numbers[i] == null) || (numbers[i].isEmpty())
            ? DEFAULT_NUMBER : Integer.parseInt(numbers[i]);
        int numberFinal = (numberShards[i] == null)
            || (numberShards[i].isEmpty()) ? listNumber
                : Integer.parseInt(numberShards[i]);
        String type = (types[i] == null) || (types[i].isEmpty()) ? null
            : types[i].trim();
        String sortType = (sortTypes[i] == null) || (sortTypes[i].isEmpty())
            ? null : sortTypes[i].trim();
        String sortDirection = (sortDirections[i] == null)
            || (sortDirections[i].isEmpty()) ? null : sortDirections[i].trim();
        String[] functionKey = functionKeys[i];
        String[] functionExpression = functionExpressions[i];
        String[] functionType = functionTypes[i];
        String boundary = boundaries[i];
        String[] list = null;
        Boolean listRegexp = listRegexps[i];
        if (lists[i] != null) {
          ArrayList<String> tmpList = new ArrayList<String>();
          String[] subList = lists[i].split("(?<!\\\\),");
          for (int j = 0; j < subList.length; j++) {
            tmpList.add(subList[j].replace("\\,", ",").replace("\\\\", "\\"));
          }
          list = tmpList.toArray(new String[tmpList.size()]);
        }
        String ignoreRegexp = ignoreRegexps[i];
        String[] ignoreList = null;
        Boolean ignoreListRegexp = ignoreListRegexps[i];
        if (ignoreLists[i] != null) {
          ArrayList<String> tmpList = new ArrayList<String>();
          String[] subList = ignoreLists[i].split("(?<!\\\\),");
          for (int j = 0; j < subList.length; j++) {
            tmpList.add(subList[j].replace("\\,", ",").replace("\\\\", "\\"));
          }
          ignoreList = tmpList.toArray(new String[tmpList.size()]);
        }

        if (prefix == null || prefix.isEmpty()) {
          throw new IOException("no (valid) prefix in mtas termvector");
        } else {
          try {
            mtasFields.list.get(field).termVectorList
                .add(new ComponentTermVector(key, prefix, regexp, full, type,
                    sortType, sortDirection, startValue, numberFinal,
                    functionKey, functionExpression, functionType, boundary,
                    list, listNumber, listRegexp, ignoreRegexp, ignoreList,
                    ignoreListRegexp));
          } catch (ParseException e) {
            throw new IOException(e.getMessage());
          }
        }
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
    if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
      if (sreq.params.getBool(PARAM_MTAS_TERMVECTOR, false)) {
        // compute keys
        Set<String> keys = MtasSolrResultUtil
            .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_TERMVECTOR);
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
          for (String key : keys) {
            String oldNumber = sreq.params.get(PARAM_MTAS_TERMVECTOR + "." + key
                + "." + NAME_MTAS_TERMVECTOR_NUMBER);
            int number;
            if (oldNumber != null) {
              int oldNumberValue = Integer.valueOf(oldNumber);
              number = (oldNumberValue >= 0)
                  ? oldNumberValue * SHARD_NUMBER_MULTIPLIER : oldNumberValue;
            } else {
              number = DEFAULT_NUMBER * SHARD_NUMBER_MULTIPLIER;
            }
            sreq.params.add(
                PARAM_MTAS_TERMVECTOR + "." + key + "."
                    + NAME_MTAS_TERMVECTOR_NUMBER_SHARDS,
                String.valueOf(number));
          }
        } else {
          sreq.params.remove(PARAM_MTAS_TERMVECTOR);
          for (String key : keys) {
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_FIELD);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_KEY);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_PREFIX);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_REGEXP);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_FULL);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_SORT_TYPE);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_SORT_DIRECTION);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_NUMBER);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_NUMBER_SHARDS);
            Set<String> functionKeys = MtasSolrResultUtil
                .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_TERMVECTOR
                    + "." + key + "." + NAME_MTAS_TERMVECTOR_FUNCTION);
            for (String functionKey : functionKeys) {
              sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION + "." + functionKey + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION_EXPRESSION);
              sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION + "." + functionKey + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION_KEY);
              sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION + "." + functionKey + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION_TYPE);
            }
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_BOUNDARY);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_LIST);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_LIST_REGEXP);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_IGNORE_REGEXP);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_IGNORE_LIST);
            sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                + NAME_MTAS_TERMVECTOR_IGNORE_REGEXP);
          }
        }
      }
    }
  }

  /**
   * Creates the.
   *
   * @param termVector
   *          the term vector
   * @param encode
   *          the encode
   * @return the simple ordered map
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unchecked")
  public SimpleOrderedMap<Object> create(ComponentTermVector termVector,
      Boolean encode) throws IOException {
    SimpleOrderedMap<Object> mtasTermVectorResponse = new SimpleOrderedMap<>();
    mtasTermVectorResponse.add("key", termVector.key);

    termVector.subComponentFunction.dataCollector.close();

    HashMap<MtasDataCollector<?, ?>, HashMap<String, MtasSolrResult>> functionData = new HashMap<MtasDataCollector<?, ?>, HashMap<String, MtasSolrResult>>();
    HashMap<String, MtasSolrResult> functionDataItem = new HashMap<String, MtasSolrResult>();
    functionData.put(termVector.subComponentFunction.dataCollector,
        functionDataItem);
    if (termVector.functions != null) {
      for (SubComponentFunction function : termVector.functions) {
        function.dataCollector.reduceToKeys(
            termVector.subComponentFunction.dataCollector.getKeyList());
        function.dataCollector.close();
        functionDataItem.put(function.key,
            new MtasSolrResult(function.dataCollector,
                new String[] { function.dataType },
                new String[] { function.statsType },
                new TreeSet[] { function.statsItems }, new String[] { null },
                new String[] { null }, new Integer[] { 0 },
                new Integer[] { Integer.MAX_VALUE }, null));
      }
    }
    MtasSolrResult data = new MtasSolrResult(
        termVector.subComponentFunction.dataCollector,
        new String[] { termVector.subComponentFunction.dataType },
        new String[] { termVector.subComponentFunction.statsType },
        new TreeSet[] { termVector.subComponentFunction.statsItems },
        new String[] { termVector.subComponentFunction.sortType },
        new String[] { termVector.subComponentFunction.sortDirection },
        new Integer[] { 0 }, new Integer[] { termVector.number }, functionData);
    if (encode) {
      mtasTermVectorResponse.add("_encoded_list",
          MtasSolrResultUtil.encode(data));
    } else {
      mtasTermVectorResponse.add("list", data);
      MtasSolrResultUtil.rewrite(mtasTermVectorResponse);
    }
    return mtasTermVectorResponse;
  }

  /**
   * Finish stage.
   *
   * @param rb
   *          the rb
   */
  @SuppressWarnings("unchecked")
  public void finishStage(ResponseBuilder rb) {
    if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
      if (rb.stage >= ResponseBuilder.STAGE_EXECUTE_QUERY
          && rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
        for (ShardRequest sreq : rb.finished) {
          if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
              && sreq.params.getBool(PARAM_MTAS_TERMVECTOR, false)) {
            for (ShardResponse shardResponse : sreq.responses) {
              NamedList<Object> response = shardResponse.getSolrResponse()
                  .getResponse();
              try {
                ArrayList<NamedList<Object>> data = (ArrayList<NamedList<Object>>) response
                    .findRecursive("mtas", "termvector");
                if (data != null) {
                  MtasSolrResultUtil.decode(data);
                }
              } catch (ClassCastException e) {
                // shouldnt happen
              }
            }
          }
        }
        if (rb.stage == MtasSolrSearchComponent.STAGE_TERMVECTOR_FINISH) {
          if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS,
              false)
              && rb.req.getParams().getBool(PARAM_MTAS_TERMVECTOR, false)) {
            Set<String> ids = MtasSolrResultUtil.getIdsFromParameters(
                rb.req.getParams(), PARAM_MTAS_TERMVECTOR);
            if (ids.size() > 0) {
              int tmpCounter = 0;
              String[] keys = new String[ids.size()];
              String[] numbers = new String[ids.size()];
              for (String id : ids) {
                keys[tmpCounter] = rb.req.getParams()
                    .get(
                        PARAM_MTAS_TERMVECTOR + "." + id + "."
                            + NAME_MTAS_TERMVECTOR_KEY,
                        String.valueOf(tmpCounter))
                    .trim();
                numbers[tmpCounter] = rb.req.getParams()
                    .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                        + NAME_MTAS_TERMVECTOR_NUMBER, null);
                tmpCounter++;
              }
              // mtas response
              NamedList<Object> mtasResponse = null;
              try {
                mtasResponse = (NamedList<Object>) rb.rsp.getValues()
                    .get("mtas");
              } catch (ClassCastException e) {
                mtasResponse = null;
              }
              if (mtasResponse == null) {
                mtasResponse = new SimpleOrderedMap<>();
                rb.rsp.add("mtas", mtasResponse);
              }
              Object o = mtasResponse.get("termvector");
              if (o != null && o instanceof ArrayList) {
                ArrayList<?> tvList = (ArrayList<?>) o;
                for (int i = 0; i < tmpCounter; i++) {
                  for (int j = 0; j < tvList.size(); j++) {
                    NamedList item = (NamedList) tvList.get(j);
                    if (item != null && item.get("key") != null
                        && item.get("key") instanceof String
                        && item.get("list") != null
                        && item.get("list") instanceof ArrayList) {
                      String key = (String) item.get("key");
                      ArrayList list = (ArrayList) item.get("list");
                      if (key.equals(keys[i])) {
                        int number;
                        if (numbers[i] != null) {
                          int numberValue = Integer.parseInt(numbers[i]);
                          number = numberValue >= 0 ? numberValue
                              : Integer.MAX_VALUE;
                        } else {
                          number = DEFAULT_NUMBER;
                        }
                        if (list.size() > number) {
                          item.add("list", list.subList(0, number));
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  }

  /**
   * Distributed process finish.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unchecked")
  public void distributedProcessFinish(ResponseBuilder rb,
      ComponentFields mtasFields) throws IOException {
    // rewrite
    NamedList<Object> mtasResponse = null;
    try {
      mtasResponse = (NamedList<Object>) rb.rsp.getValues().get("mtas");
      if (mtasResponse != null) {
        ArrayList<Object> mtasResponseTermvector;
        try {
          mtasResponseTermvector = (ArrayList<Object>) mtasResponse
              .get("termvector");
          if (mtasResponseTermvector != null) {
            MtasSolrResultUtil.rewrite(mtasResponseTermvector);
          }
        } catch (ClassCastException e) {
          mtasResponseTermvector = null;
        }
      }
    } catch (ClassCastException e) {
      mtasResponse = null;
    }
  }

  /**
   * Distributed process missing top.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void distributedProcessMissingTop(ResponseBuilder rb,
      ComponentFields mtasFields) throws IOException {
    // initialise
    HashMap<String, TreeMap<String, NumberComparator>> mergedComparatorLists = new HashMap<String, TreeMap<String, NumberComparator>>();
    HashMap<String, NumberComparator> mergedComparatorBoundaryList = new HashMap<String, NumberComparator>();
    HashMap<String, NumberComparator> summedComparatorBoundaryList = new HashMap<String, NumberComparator>();
    HashMap<String, HashMap<String, NumberComparator>> comparatorBoundariesList = new HashMap<String, HashMap<String, NumberComparator>>();
    // check all termvectors, and initialize
    for (String field : mtasFields.list.keySet()) {
      List<ComponentTermVector> tvList = mtasFields.list
          .get(field).termVectorList;
      if (tvList != null) {
        for (ComponentTermVector tv : tvList) {
          if (tv.subComponentFunction.sortType != CodecUtil.SORT_TERM) {
            mergedComparatorLists.put(tv.key,
                new TreeMap<String, NumberComparator>());
          }
        }
      }
    }
    // compute for each termvector the mergedComparatorList and the
    // summedBoundary
    for (ShardRequest sreq : rb.finished) {
      if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
          && sreq.params.getBool(PARAM_MTAS_TERMVECTOR, false)) {
        for (ShardResponse shardResponse : sreq.responses) {
          TreeMap<String, NumberComparator> mergedComparatorList;
          NamedList<Object> response = shardResponse.getSolrResponse()
              .getResponse();
          String key;
          MtasSolrResult list;
          NumberComparator comparatorLast;
          LinkedHashMap<String, NumberComparator> comparatorList;
          try {
            ArrayList<NamedList<Object>> data = (ArrayList<NamedList<Object>>) response
                .findRecursive("mtas", "termvector");
            if (data != null) {
              for (int i = 0; i < data.size(); i++) {
                NamedList<Object> dataItem = data.get(i);
                try {
                  key = (String) dataItem.get("key");
                  list = (MtasSolrResult) dataItem.get("list");
                  if (list != null) {
                    comparatorLast = list.getResult().getLastSortValue();
                    comparatorList = list.getResult().getComparatorList();
                    if (key == null) {
                      dataItem.clear();
                    } else if (comparatorLast == null || comparatorList == null
                        || !mergedComparatorLists.containsKey(key)) {
                      // do nothing
                    } else {
                      mergedComparatorList = mergedComparatorLists.get(key);
                      for (String value : comparatorList.keySet()) {
                        if (mergedComparatorList.containsKey(value)) {
                          mergedComparatorList.get(value)
                              .add(comparatorList.get(value).getValue());
                        } else {
                          mergedComparatorList.put(value,
                              comparatorList.get(value).clone());
                        }
                      }
                      if (!comparatorBoundariesList.containsKey(key)) {
                        comparatorBoundariesList.put(key,
                            new HashMap<String, NumberComparator>());
                      }
                      comparatorBoundariesList.get(key)
                          .put(shardResponse.getShardAddress(), comparatorLast);
                      if (summedComparatorBoundaryList.containsKey(key)) {
                        summedComparatorBoundaryList.get(key)
                            .add(comparatorLast.getValue());
                      } else {
                        summedComparatorBoundaryList.put(key,
                            comparatorLast.clone());
                      }
                    }
                  } else {
                    throw new IOException("no data returned");
                  }
                } catch (ClassCastException e) {
                  dataItem.clear();
                }
              }
            }
          } catch (ClassCastException e) {
            // shouldnt happen
          }
          shardResponse.getSolrResponse().setResponse(response);
        }
      }
    }
    // compute for each relevant termvector the mergedComparatorBoundary
    HashMap<String, HashMap<String, HashMap<String, NumberComparator>>> recomputeFieldList = new HashMap<String, HashMap<String, HashMap<String, NumberComparator>>>();
    for (String field : mtasFields.list.keySet()) {
      List<ComponentTermVector> tvList = mtasFields.list
          .get(field).termVectorList;
      if (tvList != null) {
        for (ComponentTermVector tv : tvList) {
          TreeMap<String, NumberComparator> mergedComparatorList;
          if (mergedComparatorLists.containsKey(tv.key)) {
            mergedComparatorList = mergedComparatorLists.get(tv.key);
            if (mergedComparatorList.size() < tv.number || tv.number <= 0) {
              // do nothing
            } else {
              SortedSet<Map.Entry<String, NumberComparator>> sortedSet = new TreeSet<Map.Entry<String, NumberComparator>>(
                  new Comparator<Map.Entry<String, NumberComparator>>() {
                    @Override
                    public int compare(Map.Entry<String, NumberComparator> e1,
                        Map.Entry<String, NumberComparator> e2) {
                      int compare = e1.getValue()
                          .compareTo(e2.getValue().getValue());
                      if (compare == 0) {
                        compare = e1.getKey().compareTo(e2.getKey());
                      } else if (tv.subComponentFunction.sortDirection
                          .equals(CodecUtil.SORT_DESC)) {
                        compare *= -1;
                      }
                      return compare;
                    }
                  });
              sortedSet.addAll(mergedComparatorLists.get(tv.key).entrySet());
              Optional<Map.Entry<String, NumberComparator>> optionalItem = sortedSet
                  .stream().skip(tv.number - 1).findFirst();
              if (optionalItem.isPresent()) {
                mergedComparatorBoundaryList.put(tv.key,
                    optionalItem.get().getValue());
              }
            }
          }
        }
      }
      // System.out.println("BOUNDARIES: " + comparatorBoundariesList);
      // System.out.println("MERGED: " + mergedComparatorLists);
      // System.out.println("MERGED boundary : " +
      // mergedComparatorBoundaryList);
      // System.out.println("SUMMED boundary : " +
      // summedComparatorBoundaryList);
      // compute which termvectors to recompute
      HashMap<String, HashMap<String, NumberComparator>> recomputeList = new HashMap<String, HashMap<String, NumberComparator>>();
      for (ComponentTermVector tv : tvList) {
        String key = tv.key;
        if (mergedComparatorBoundaryList.containsKey(key)
            && summedComparatorBoundaryList.containsKey(key)) {
          // set termvector to recompute
          recomputeList.put(key, new HashMap<String, NumberComparator>());
          // sort
          List<Entry<String, NumberComparator>> list = new LinkedList<Entry<String, NumberComparator>>(
              comparatorBoundariesList.get(key).entrySet());
          Collections.sort(list,
              new Comparator<Entry<String, NumberComparator>>() {

                public int compare(Entry<String, NumberComparator> e1,
                    Entry<String, NumberComparator> e2) {
                  return e1.getValue().compareTo(e2.getValue().getValue());
                }
              });

          HashMap<String, NumberComparator> sortedHashMap = new LinkedHashMap<String, NumberComparator>();
          for (Iterator<Map.Entry<String, NumberComparator>> it = list
              .iterator(); it.hasNext();) {
            Map.Entry<String, NumberComparator> entry = it.next();
            sortedHashMap.put(entry.getKey(), entry.getValue());
          }

          NumberComparator mainNewBoundary = mergedComparatorBoundaryList
              .get(key).recomputeBoundary(sortedHashMap.size());
          // System.out.println(
          // "MAIN NEW BOUNDARY for '" + key + "' : " + mainNewBoundary);

          NumberComparator sum = null;
          int number = 0;
          for (String shardAddress : sortedHashMap.keySet()) {
            NumberComparator newBoundary = mainNewBoundary.clone();
            NumberComparator currentBoundary = sortedHashMap.get(shardAddress);
            int compare = currentBoundary.compareTo(newBoundary.getValue());
            if (tv.subComponentFunction.sortDirection
                .equals(CodecUtil.SORT_DESC)) {
              compare *= -1;
            }
            if (compare < 0) {
              HashMap<String, NumberComparator> recomputeSubList = new HashMap<String, NumberComparator>();
              if (number > 0 && tv.subComponentFunction.sortDirection
                  .equals(CodecUtil.SORT_DESC)) {
                NumberComparator tmpSumBoundary = mergedComparatorBoundaryList
                    .get(key);
                tmpSumBoundary.subtract(sum.getValue());
                NumberComparator alternativeNewBoundary = tmpSumBoundary
                    .recomputeBoundary(sortedHashMap.size() - number);
                compare = newBoundary
                    .compareTo(alternativeNewBoundary.getValue());
                if (compare < 0) {
                  newBoundary = alternativeNewBoundary;
                  compare = currentBoundary.compareTo(newBoundary.getValue());
                  if (tv.subComponentFunction.sortDirection
                      .equals(CodecUtil.SORT_DESC)) {
                    compare *= -1;
                  }
                  if (compare < 0) {
                    recomputeSubList.put(shardAddress, newBoundary);
                  }
                } else {
                  recomputeSubList.put(shardAddress, newBoundary);
                }
              } else {
                recomputeSubList.put(shardAddress, newBoundary);
              }
              if (recomputeSubList.size() > 0) {
                if (!recomputeList.containsKey(key)) {
                  recomputeList.put(key, recomputeSubList);
                } else {
                  recomputeList.get(key).putAll(recomputeSubList);
                }
              }
            } else {
              newBoundary = currentBoundary.clone();
            }
            if (sum == null) {
              sum = newBoundary.clone();
            } else {
              sum.add(newBoundary.getValue());
            }
            number++;
          }

        }
      }
      if (recomputeList.size() > 0) {
        recomputeFieldList.put(field, recomputeList);
      }
    }

    // finally, recompute
    if (recomputeFieldList.size() > 0) {

      // remove output for termvectors in recompute list and get list of shards
      HashSet<String> shards = new HashSet<String>();
      for (ShardRequest sreq : rb.finished) {
        if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
            && sreq.params.getBool(PARAM_MTAS_TERMVECTOR, false)) {
          for (ShardResponse shardResponse : sreq.responses) {
            NamedList<Object> response = shardResponse.getSolrResponse()
                .getResponse();
            String key, field;
            String shardAddress = shardResponse.getShardAddress();
            try {
              ArrayList<NamedList<Object>> data = (ArrayList<NamedList<Object>>) response
                  .findRecursive("mtas", "termvector");
              shards.add(shardAddress);
              if (data != null) {
                for (int i = 0; i < data.size(); i++) {
                  NamedList<Object> dataItem = data.get(i);
                  try {
                    key = (String) dataItem.get("key");
                    field = (String) dataItem.get("field");
                    if (field != null && key != null
                        && recomputeFieldList.containsKey(field)
                        && recomputeFieldList.get(field).containsKey(key)
                        && recomputeFieldList.get(field).get(key)
                            .containsKey(shardAddress)) {
                      dataItem.clear();
                      dataItem.add("key", key);
                    }
                  } catch (ClassCastException e) {
                    dataItem.clear();
                  }
                }
              }
            } catch (ClassCastException e) {
              // shouldnt happen
            }
            shardResponse.getSolrResponse().setResponse(response);
          }
        }
      }

      // parameter
      HashMap<String, ModifiableSolrParams> requestParamList = new HashMap<String, ModifiableSolrParams>();
      for (String shardAddress : shards) {
        ModifiableSolrParams paramsNewRequest = new ModifiableSolrParams();
        int termvectorCounter = 0;
        for (String field : mtasFields.list.keySet()) {
          List<ComponentTermVector> tvList = mtasFields.list
              .get(field).termVectorList;
          if (recomputeFieldList.containsKey(field)) {
            HashMap<String, HashMap<String, NumberComparator>> recomputeList = recomputeFieldList
                .get(field);
            if (tvList != null) {
              for (ComponentTermVector tv : tvList) {
                if (recomputeList.containsKey(tv.key)
                    && recomputeList.get(tv.key).containsKey(shardAddress)) {
                  paramsNewRequest.add(
                      PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                          + NAME_MTAS_TERMVECTOR_BOUNDARY,
                      String.valueOf(recomputeList.get(tv.key).get(shardAddress)
                          .getValue()));
                  paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                      + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_FIELD,
                      field);
                  paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                      + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_PREFIX,
                      tv.prefix);
                  paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                      + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_KEY,
                      tv.key);
                  paramsNewRequest.add(
                      PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                          + NAME_MTAS_TERMVECTOR_NUMBER,
                      String.valueOf(tv.number));
                  if (tv.subComponentFunction.sortType != null) {
                    paramsNewRequest.add(
                        PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                            + NAME_MTAS_TERMVECTOR_SORT_TYPE,
                        tv.subComponentFunction.sortType);
                  }
                  if (tv.subComponentFunction.sortDirection != null) {
                    paramsNewRequest.add(
                        PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                            + NAME_MTAS_TERMVECTOR_SORT_DIRECTION,
                        tv.subComponentFunction.sortDirection);
                  }
                  if (tv.subComponentFunction.type != null) {
                    paramsNewRequest.add(
                        PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                            + NAME_MTAS_TERMVECTOR_TYPE,
                        tv.subComponentFunction.type);
                  }
                  if (tv.functions != null) {
                    int functionCounter = 0;
                    for (SubComponentFunction function : tv.functions) {
                      paramsNewRequest.add(
                          PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                              + NAME_MTAS_TERMVECTOR_FUNCTION + "."
                              + functionCounter + "."
                              + NAME_MTAS_TERMVECTOR_FUNCTION_EXPRESSION,
                          function.expression);
                      paramsNewRequest.add(
                          PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                              + NAME_MTAS_TERMVECTOR_FUNCTION + "."
                              + functionCounter + "."
                              + NAME_MTAS_TERMVECTOR_FUNCTION_KEY,
                          function.key);
                      if (function.type != null) {
                        paramsNewRequest.add(
                            PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                + "." + NAME_MTAS_TERMVECTOR_FUNCTION + "."
                                + functionCounter + "."
                                + NAME_MTAS_TERMVECTOR_FUNCTION_TYPE,
                            function.type);
                      }
                      functionCounter++;
                    }
                  }
                  if (tv.regexp != null) {
                    paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                        + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_REGEXP,
                        tv.regexp);
                  }
                  termvectorCounter++;
                }
              }
            }
          }
        }
        if (paramsNewRequest.getParameterNames().size() > 0) {
          requestParamList.put(shardAddress, paramsNewRequest);
        }
      }

      // new requests
      for (String shardAddress : requestParamList.keySet()) {
        ShardRequest sreq = new ShardRequest();
        sreq.shards = new String[] { shardAddress };
        sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
        sreq.params = requestParamList.get(shardAddress);
        sreq.params.add("fq", rb.req.getParams().getParams("fq"));
        sreq.params.add("q", rb.req.getParams().getParams("q"));
        sreq.params.add("cache", rb.req.getParams().getParams("cache"));
        sreq.params.add("rows", "0");
        sreq.params.add(MtasSolrSearchComponent.PARAM_MTAS, rb.req
            .getOriginalParams().getParams(MtasSolrSearchComponent.PARAM_MTAS));
        sreq.params.add(PARAM_MTAS_TERMVECTOR,
            rb.req.getOriginalParams().getParams(PARAM_MTAS_TERMVECTOR));
        rb.addRequest(searchComponent, sreq);
      }
    }

  }

  /**
   * Distributed process missing key.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void distributedProcessMissingKey(ResponseBuilder rb,
      ComponentFields mtasFields) throws IOException {
    HashMap<String, HashMap<String, HashSet<String>>> missingTermvectorKeys = computeMissingTermvectorItemsPerShard(
        rb.finished, "mtas", "termvector");
    for (String shardName : missingTermvectorKeys.keySet()) {
      HashMap<String, HashSet<String>> missingTermvectorKeysShard = missingTermvectorKeys
          .get(shardName);
      ModifiableSolrParams paramsNewRequest = new ModifiableSolrParams();
      int termvectorCounter = 0;
      for (String field : mtasFields.list.keySet()) {
        List<ComponentTermVector> tvList = mtasFields.list
            .get(field).termVectorList;
        if (tvList != null) {
          for (ComponentTermVector tv : tvList) {
            if (!tv.full) {
              if (missingTermvectorKeysShard.containsKey(tv.key)) {
                HashSet<String> list = missingTermvectorKeysShard.get(tv.key);
                if (list.size() > 0) {
                  paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                      + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_FIELD,
                      field);
                  paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                      + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_PREFIX,
                      tv.prefix);
                  paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                      + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_KEY,
                      tv.key);
                  if (tv.subComponentFunction.type != null) {
                    paramsNewRequest.add(
                        PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                            + NAME_MTAS_TERMVECTOR_TYPE,
                        tv.subComponentFunction.type);
                  }
                  if (tv.functions != null) {
                    int functionCounter = 0;
                    for (SubComponentFunction function : tv.functions) {
                      paramsNewRequest.add(
                          PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                              + NAME_MTAS_TERMVECTOR_FUNCTION + "."
                              + functionCounter + "."
                              + NAME_MTAS_TERMVECTOR_FUNCTION_EXPRESSION,
                          function.expression);
                      paramsNewRequest.add(
                          PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                              + NAME_MTAS_TERMVECTOR_FUNCTION + "."
                              + functionCounter + "."
                              + NAME_MTAS_TERMVECTOR_FUNCTION_KEY,
                          function.key);
                      if (function.type != null) {
                        paramsNewRequest.add(
                            PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                + "." + NAME_MTAS_TERMVECTOR_FUNCTION + "."
                                + functionCounter + "."
                                + NAME_MTAS_TERMVECTOR_FUNCTION_TYPE,
                            function.type);
                      }
                      functionCounter++;
                    }
                  }
                  if (tv.regexp != null) {
                    paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                        + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_REGEXP,
                        tv.regexp);
                  }
                  if (list.size() > 0) {
                    String listValue = "";
                    String[] listList = list.toArray(new String[list.size()]);
                    for (int i = 0; i < listList.length; i++) {
                      if (i > 0) {
                        listValue += ",";
                      }
                      listValue += listList[i].replace("\\", "\\\\")
                          .replace(",", "\\\\");
                    }
                    paramsNewRequest.add(
                        PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                            + NAME_MTAS_TERMVECTOR_FULL,
                        tv.full ? "true" : "false");
                    paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                        + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_LIST,
                        listValue);
                  }
                  termvectorCounter++;
                }
              }
            }
          }
          if (termvectorCounter > 0) {
            ShardRequest nsreq = new ShardRequest();
            nsreq.shards = new String[] { shardName };
            nsreq.purpose = ShardRequest.PURPOSE_PRIVATE;
            nsreq.params = new ModifiableSolrParams();
            nsreq.params.add("fq", rb.req.getParams().getParams("fq"));
            nsreq.params.add("q", rb.req.getParams().getParams("q"));
            nsreq.params.add("cache", rb.req.getParams().getParams("cache"));
            nsreq.params.add("rows", "0");
            nsreq.params.add(MtasSolrSearchComponent.PARAM_MTAS,
                rb.req.getOriginalParams()
                    .getParams(MtasSolrSearchComponent.PARAM_MTAS));
            nsreq.params.add(PARAM_MTAS_TERMVECTOR,
                rb.req.getOriginalParams().getParams(PARAM_MTAS_TERMVECTOR));
            nsreq.params.add(paramsNewRequest);
            rb.addRequest(searchComponent, nsreq);
          }
        }
      }
    }
  }

  /**
   * Compute missing termvector items per shard.
   *
   * @param requests
   *          the requests
   * @param args
   *          the args
   * @return the hash map
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unchecked")
  private HashMap<String, HashMap<String, HashSet<String>>> computeMissingTermvectorItemsPerShard(
      List<ShardRequest> requests, String... args) throws IOException {
    HashMap<String, HashMap<String, HashSet<String>>> result = new HashMap<String, HashMap<String, HashSet<String>>>();
    HashMap<String, HashMap<String, HashSet<String>>> itemsPerShardSets = new HashMap<String, HashMap<String, HashSet<String>>>();
    HashMap<String, HashSet<String>> itemSets = new HashMap<String, HashSet<String>>();
    // loop over responses different shards
    for (ShardRequest sreq : requests) {
      if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
          && sreq.params.getBool(PARAM_MTAS_TERMVECTOR, false)) {
        for (ShardResponse shardResponse : sreq.responses) {
          NamedList<Object> response = shardResponse.getSolrResponse()
              .getResponse();
          try {
            ArrayList<NamedList<Object>> data = (ArrayList<NamedList<Object>>) response
                .findRecursive(args);
            if (data != null) {
              for (int i = 0; i < data.size(); i++) {
                NamedList<Object> dataItem = data.get(i);
                try {
                  String termvectorKey = (String) dataItem.get("key");
                  MtasSolrResult list = (MtasSolrResult) dataItem.get("list");
                  if (termvectorKey != null && list != null) {
                    Set<String> keyList = list.getKeyList();
                    HashMap<String, HashSet<String>> itemsPerShardSet;
                    HashSet<String> itemSet, tmpItemSet = new HashSet<String>();
                    if (itemsPerShardSets.containsKey(termvectorKey)) {
                      itemsPerShardSet = itemsPerShardSets.get(termvectorKey);
                      itemSet = itemSets.get(termvectorKey);
                    } else {
                      itemsPerShardSet = new HashMap<String, HashSet<String>>();
                      itemSet = new HashSet<String>();
                      itemsPerShardSets.put(termvectorKey, itemsPerShardSet);
                      itemSets.put(termvectorKey, itemSet);
                    }
                    itemsPerShardSet.put(shardResponse.getShardAddress(),
                        tmpItemSet);
                    tmpItemSet.addAll(keyList);
                    itemSet.addAll(keyList);
                  }
                } catch (ClassCastException e) {

                }
              }

            }
          } catch (ClassCastException e) {
          }
        }
      }
    }

    // construct result
    for (String key : itemSets.keySet()) {
      if (itemsPerShardSets.containsKey(key)) {
        HashMap<String, HashSet<String>> itemsPerShardSet = itemsPerShardSets
            .get(key);
        for (String shardAddress : itemsPerShardSet.keySet()) {
          HashMap<String, HashSet<String>> tmpShardKeySet;
          if (result.containsKey(shardAddress)) {
            tmpShardKeySet = result.get(shardAddress);
          } else {
            tmpShardKeySet = new HashMap<String, HashSet<String>>();
            result.put(shardAddress, tmpShardKeySet);
          }
          HashSet<String> tmpResult = new HashSet<String>();
          tmpShardKeySet.put(key, tmpResult);
          HashSet<String> itemsSet = itemsPerShardSet.get(shardAddress);
          for (String item : itemSets.get(key)) {
            if (!itemsSet.contains(item)) {
              tmpResult.add(item);
            }
          }
        }
      }
    }
    return result;
  }

}
