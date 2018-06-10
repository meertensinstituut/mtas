package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
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
import mtas.codec.util.CodecComponent.SubComponentDistance;
import mtas.codec.util.CodecComponent.SubComponentFunction;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.codec.util.collector.MtasDataItemNumberComparator;
import mtas.parser.function.ParseException;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentTermvector.
 */
public class MtasSolrComponentTermvector
    implements MtasSolrComponent<ComponentTermVector> {

  /** The Constant log. */
  private static final Log log = LogFactory
      .getLog(MtasSolrComponentTermvector.class);

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant NAME. */
  public static final String NAME = "termvector";

  /** The Constant PARAM_MTAS_TERMVECTOR. */
  public static final String PARAM_MTAS_TERMVECTOR = MtasSolrSearchComponent.PARAM_MTAS
      + "." + NAME;

  /** The Constant NAME_MTAS_TERMVECTOR_FIELD. */
  public static final String NAME_MTAS_TERMVECTOR_FIELD = "field";

  /** The Constant NAME_MTAS_TERMVECTOR_KEY. */
  public static final String NAME_MTAS_TERMVECTOR_KEY = "key";

  /** The Constant NAME_MTAS_TERMVECTOR_PREFIX. */
  public static final String NAME_MTAS_TERMVECTOR_PREFIX = "prefix";

  /** The Constant NAME_MTAS_TERMVECTOR_DISTANCE. */
  public static final String NAME_MTAS_TERMVECTOR_DISTANCE = "distance";

  /** The Constant NAME_MTAS_TERMVECTOR_DISTANCE_KEY. */
  public static final String NAME_MTAS_TERMVECTOR_DISTANCE_KEY = "key";

  /** The Constant NAME_MTAS_TERMVECTOR_DISTANCE_TYPE. */
  public static final String NAME_MTAS_TERMVECTOR_DISTANCE_TYPE = "type";

  /** The Constant NAME_MTAS_TERMVECTOR_DISTANCE_BASE. */
  public static final String NAME_MTAS_TERMVECTOR_DISTANCE_BASE = "base";

  /** The Constant NAME_MTAS_TERMVECTOR_DISTANCE_PARAMETER. */
  public static final String NAME_MTAS_TERMVECTOR_DISTANCE_PARAMETER = "parameter";

  /** The Constant NAME_MTAS_TERMVECTOR_DISTANCE_MINIMUM. */
  public static final String NAME_MTAS_TERMVECTOR_DISTANCE_MINIMUM = "minimum";

  /** The Constant NAME_MTAS_TERMVECTOR_DISTANCE_MAXIMUM. */
  public static final String NAME_MTAS_TERMVECTOR_DISTANCE_MAXIMUM = "maximum";

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

  /** The Constant NAME_MTAS_TERMVECTOR_START. */
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

  /** The Constant NAME_MTAS_TERMVECTOR_LIST_REGEXP. */
  public static final String NAME_MTAS_TERMVECTOR_LIST_REGEXP = "listRegexp";

  /** The Constant NAME_MTAS_TERMVECTOR_IGNORE_REGEXP. */
  public static final String NAME_MTAS_TERMVECTOR_IGNORE_REGEXP = "ignoreRegexp";

  /** The Constant NAME_MTAS_TERMVECTOR_IGNORE_LIST. */
  public static final String NAME_MTAS_TERMVECTOR_IGNORE_LIST = "ignoreList";

  /** The Constant NAME_MTAS_TERMVECTOR_IGNORE_LIST_REGEXP. */
  public static final String NAME_MTAS_TERMVECTOR_IGNORE_LIST_REGEXP = "ignoreListRegexp";

  /** The Constant SHARD_NUMBER_MULTIPLIER. */
  private static final int SHARD_NUMBER_MULTIPLIER = 2;

  /** The Constant DEFAULT_NUMBER. */
  private static final int DEFAULT_NUMBER = 10;

  /**
   * Instantiates a new mtas solr component termvector.
   *
   * @param searchComponent the search component
   */
  public MtasSolrComponentTermvector(MtasSolrSearchComponent searchComponent) {
    this.searchComponent = searchComponent;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#prepare(org.apache.solr.
   * handler.component.ResponseBuilder,
   * mtas.codec.util.CodecComponent.ComponentFields)
   */
  public void prepare(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = MtasSolrResultUtil
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_TERMVECTOR);
    if (!ids.isEmpty()) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] prefixes = new String[ids.size()];
      String[][] distanceKeys = new String[ids.size()][];
      String[][] distanceTypes = new String[ids.size()][];
      String[][] distanceBases = new String[ids.size()][];
      Map<String, String>[][] distanceParameters = new Map[ids.size()][];
      String[][] distanceMinimums = new String[ids.size()][];
      String[][] distanceMaximums = new String[ids.size()][];
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
        Set<String> distanceIds = MtasSolrResultUtil
            .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_TERMVECTOR
                + "." + id + "." + NAME_MTAS_TERMVECTOR_DISTANCE);
        distanceKeys[tmpCounter] = new String[distanceIds.size()];
        distanceTypes[tmpCounter] = new String[distanceIds.size()];
        distanceBases[tmpCounter] = new String[distanceIds.size()];
        distanceParameters[tmpCounter] = new Map[distanceIds.size()];
        distanceMinimums[tmpCounter] = new String[distanceIds.size()];
        distanceMaximums[tmpCounter] = new String[distanceIds.size()];
        int tmpSubDistanceCounter = 0;
        for (String distanceId : distanceIds) {
          distanceKeys[tmpCounter][tmpSubDistanceCounter] = rb.req.getParams()
              .get(
                  PARAM_MTAS_TERMVECTOR + "." + id + "."
                      + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceId + "."
                      + NAME_MTAS_TERMVECTOR_DISTANCE_KEY,
                  String.valueOf(tmpSubDistanceCounter))
              .trim();
          distanceTypes[tmpCounter][tmpSubDistanceCounter] = rb.req.getParams()
              .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceId + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE_TYPE, null);
          distanceBases[tmpCounter][tmpSubDistanceCounter] = rb.req.getParams()
              .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceId + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE_BASE, null);
          distanceParameters[tmpCounter][tmpSubDistanceCounter] = new HashMap<>();
          Set<String> parameters = MtasSolrResultUtil.getIdsFromParameters(
              rb.req.getParams(),
              PARAM_MTAS_TERMVECTOR + "." + id + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceId + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE_PARAMETER);
          for (String parameter : parameters) {
            distanceParameters[tmpCounter][tmpSubDistanceCounter].put(parameter,
                rb.req.getParams()
                    .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                        + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceId + "."
                        + NAME_MTAS_TERMVECTOR_DISTANCE_PARAMETER + "."
                        + parameter));
          }
          distanceMinimums[tmpCounter][tmpSubDistanceCounter] = rb.req
              .getParams()
              .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceId + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE_MINIMUM, null);
          distanceMaximums[tmpCounter][tmpSubDistanceCounter] = rb.req
              .getParams()
              .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceId + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE_MAXIMUM, null);
          tmpSubDistanceCounter++;
        }
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
        int tmpSubFunctionCounter = 0;
        for (String functionId : functionIds) {
          functionKeys[tmpCounter][tmpSubFunctionCounter] = rb.req.getParams()
              .get(
                  PARAM_MTAS_TERMVECTOR + "." + id + "."
                      + NAME_MTAS_TERMVECTOR_FUNCTION + "." + functionId + "."
                      + NAME_MTAS_TERMVECTOR_FUNCTION_KEY,
                  String.valueOf(tmpSubFunctionCounter))
              .trim();
          functionExpressions[tmpCounter][tmpSubFunctionCounter] = rb.req
              .getParams()
              .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION + "." + functionId + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION_EXPRESSION, null);
          functionTypes[tmpCounter][tmpSubFunctionCounter] = rb.req.getParams()
              .get(PARAM_MTAS_TERMVECTOR + "." + id + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION + "." + functionId + "."
                  + NAME_MTAS_TERMVECTOR_FUNCTION_TYPE, null);
          tmpSubFunctionCounter++;
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
          mtasFields.list.put(field, new ComponentField(uniqueKeyField));
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
        if (!rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
          numberShards[i] = null;
          boundaries[i] = null;
        }
        String field = fields[i];
        String prefix = (prefixes[i] == null) || (prefixes[i].isEmpty()) ? null
            : prefixes[i].trim();
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + field + ":" + prefix : keys[i].trim();
        String[] distanceKey = distanceKeys[i];
        String[] distanceType = distanceTypes[i];
        String[] distanceBase = distanceBases[i];
        Map<String, String>[] distanceParameter = distanceParameters[i];
        String[] distanceMinimum = distanceMinimums[i];
        String[] distanceMaximum = distanceMaximums[i];
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
          ArrayList<String> tmpList = new ArrayList<>();
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
          ArrayList<String> tmpList = new ArrayList<>();
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
            mtasFields.list.get(field).termVectorList.add(
                new ComponentTermVector(key, prefix, distanceKey, distanceType,
                    distanceBase, distanceParameter, distanceMinimum,
                    distanceMaximum, regexp, full, type, sortType,
                    sortDirection, startValue, numberFinal, functionKey,
                    functionExpression, functionType, boundary, list,
                    listRegexp, ignoreRegexp, ignoreList, ignoreListRegexp));
          } catch (ParseException e) {
            throw new IOException(e);
          }
        }
      }

    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#modifyRequest(org.apache
   * .solr.handler.component.ResponseBuilder,
   * org.apache.solr.handler.component.SearchComponent,
   * org.apache.solr.handler.component.ShardRequest)
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
              int oldNumberValue = Integer.parseInt(oldNumber);
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
            Set<String> distanceKeys = MtasSolrResultUtil
                .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_TERMVECTOR
                    + "." + key + "." + NAME_MTAS_TERMVECTOR_DISTANCE);
            for (String distanceKey : distanceKeys) {
              sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceKey + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE_KEY);
              sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceKey + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE_TYPE);
              sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceKey + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE_BASE);
              sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceKey + "."
                  + NAME_MTAS_TERMVECTOR_DISTANCE_MAXIMUM);
              Set<String> distanceParameters = MtasSolrResultUtil
                  .getIdsFromParameters(rb.req.getParams(),
                      PARAM_MTAS_TERMVECTOR + "." + key + "."
                          + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceKey
                          + "." + NAME_MTAS_TERMVECTOR_DISTANCE_PARAMETER);
              for (String distanceParameter : distanceParameters) {
                sreq.params.remove(PARAM_MTAS_TERMVECTOR + "." + key + "."
                    + NAME_MTAS_TERMVECTOR_DISTANCE + "." + distanceKey + "."
                    + NAME_MTAS_TERMVECTOR_DISTANCE_PARAMETER + "."
                    + distanceParameter);
              }
            }
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

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#create(mtas.codec.util.
   * CodecComponent.BasicComponent, java.lang.Boolean)
   */
  @SuppressWarnings("unchecked")
  public SimpleOrderedMap<Object> create(ComponentTermVector termVector,
      Boolean encode) throws IOException {
    SimpleOrderedMap<Object> mtasTermVectorResponse = new SimpleOrderedMap<>();
    mtasTermVectorResponse.add("key", termVector.key);

    termVector.subComponentFunction.dataCollector.close();

    HashMap<MtasDataCollector<?, ?>, HashMap<String, MtasSolrMtasResult>> functionData = new HashMap<>();
    HashMap<String, MtasSolrMtasResult> functionDataItem = new HashMap<>();
    functionData.put(termVector.subComponentFunction.dataCollector,
        functionDataItem);
    if (termVector.functions != null) {
      for (SubComponentFunction function : termVector.functions) {
        function.dataCollector.reduceToKeys(
            termVector.subComponentFunction.dataCollector.getKeyList());
        function.dataCollector.close();
        functionDataItem.put(function.key, new MtasSolrMtasResult(
            function.dataCollector, new String[] { function.dataType },
            new String[] { function.statsType },
            new SortedSet[] { function.statsItems }, new List[] { null },
            new String[] { null }, new String[] { null }, new Integer[] { 0 },
            new Integer[] { Integer.MAX_VALUE }, null));
      }
    }
    MtasSolrMtasResult data = new MtasSolrMtasResult(
        termVector.subComponentFunction.dataCollector,
        new String[] { termVector.subComponentFunction.dataType },
        new String[] { termVector.subComponentFunction.statsType },
        new SortedSet[] { termVector.subComponentFunction.statsItems },
        new List[] { termVector.distances },
        new String[] { termVector.subComponentFunction.sortType },
        new String[] { termVector.subComponentFunction.sortDirection },
        new Integer[] { 0 }, new Integer[] { termVector.number }, functionData);
    if (encode) {
      mtasTermVectorResponse.add("_encoded_list",
          MtasSolrResultUtil.encode(data));
    } else {
      mtasTermVectorResponse.add("list", data);
      MtasSolrResultUtil.rewrite(mtasTermVectorResponse, searchComponent);
    }
    return mtasTermVectorResponse;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#finishStage(org.apache.
   * solr.handler.component.ResponseBuilder)
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
                    .findRecursive("mtas", NAME);
                if (data != null) {
                  MtasSolrResultUtil.decode(data);
                }
              } catch (ClassCastException e) {
                log.debug(e);
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
            if (!ids.isEmpty()) {
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
                log.debug(e);
                mtasResponse = null;
              }
              if (mtasResponse == null) {
                mtasResponse = new SimpleOrderedMap<>();
                rb.rsp.add("mtas", mtasResponse);
              }
              Object o = mtasResponse.get(NAME);
              if (o != null && o instanceof ArrayList) {
                ArrayList<?> tvList = (ArrayList<?>) o;
                for (int i = 0; i < tmpCounter; i++) {
                  for (int j = 0; j < tvList.size(); j++) {
                    NamedList<Object> item = (NamedList<Object>) tvList.get(j);
                    boolean condition;
                    condition = item != null;
                    condition &= item.get("key") != null;
                    condition &= item.get("key") instanceof String;
                    condition &= item.get("list") != null;
                    condition &= item.get("list") instanceof ArrayList;
                    if (condition) {
                      String key = (String) item.get("key");
                      ArrayList<Object> list = (ArrayList<Object>) item
                          .get("list");
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
                          item.removeAll("list");
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

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#distributedProcess(org.
   * apache.solr.handler.component.ResponseBuilder,
   * mtas.codec.util.CodecComponent.ComponentFields)
   */
  @Override
  public void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    if (rb.stage == MtasSolrSearchComponent.STAGE_TERMVECTOR_MISSING_TOP) {
      distributedProcessMissingTop(rb, mtasFields);
    } else if (rb.stage == MtasSolrSearchComponent.STAGE_TERMVECTOR_MISSING_KEY) {
      distributedProcessMissingKey(rb, mtasFields);
    } else if (rb.stage == MtasSolrSearchComponent.STAGE_TERMVECTOR_FINISH) {
      distributedProcessFinish(rb, mtasFields);
    }
  }

  /**
   * Distributed process finish.
   *
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void distributedProcessFinish(ResponseBuilder rb,
      ComponentFields mtasFields) throws IOException {
    // rewrite

    Object mtasResponseRaw;
    if ((mtasResponseRaw = rb.rsp.getValues().get("mtas")) != null
        && mtasResponseRaw instanceof NamedList) {
      NamedList<Object> mtasResponse = (NamedList<Object>) mtasResponseRaw;
      Object mtasResponseTermvectorRaw;
      if ((mtasResponseTermvectorRaw = mtasResponse.get(NAME)) != null
          && mtasResponseTermvectorRaw instanceof ArrayList) {
        MtasSolrResultUtil.rewrite(
            (ArrayList<Object>) mtasResponseTermvectorRaw, searchComponent);
      }
    }
  }

  /**
   * Distributed process missing top.
   *
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void distributedProcessMissingTop(ResponseBuilder rb,
      ComponentFields mtasFields) throws IOException {
    // initialise
    Map<String, SortedMap<String, MtasDataItemNumberComparator>> mergedComparatorLists = new HashMap<>();
    Map<String, MtasDataItemNumberComparator> mergedComparatorBoundaryList = new HashMap<>();
    Map<String, MtasDataItemNumberComparator> summedComparatorBoundaryList = new HashMap<>();
    Map<String, Map<String, MtasDataItemNumberComparator>> comparatorBoundariesList = new HashMap<>();
    // check all termvectors, and initialize
    for (String field : mtasFields.list.keySet()) {
      List<ComponentTermVector> tvList = mtasFields.list
          .get(field).termVectorList;
      if (tvList != null) {
        for (ComponentTermVector tv : tvList) {
          if (!tv.subComponentFunction.sortType.equals(CodecUtil.SORT_TERM)) {
            mergedComparatorLists.put(tv.key,
                new TreeMap<String, MtasDataItemNumberComparator>());
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
          SortedMap<String, MtasDataItemNumberComparator> mergedComparatorList;
          NamedList<Object> response = shardResponse.getSolrResponse()
              .getResponse();
          String key;
          MtasSolrMtasResult list;
          MtasDataItemNumberComparator comparatorLast;
          Map<String, MtasDataItemNumberComparator> comparatorList;
          try {
            List<NamedList<Object>> data = (List<NamedList<Object>>) response
                .findRecursive("mtas", NAME);
            if (data != null) {
              for (int i = 0; i < data.size(); i++) {
                NamedList<Object> dataItem = data.get(i);
                try {
                  key = (String) dataItem.get("key");
                  list = (MtasSolrMtasResult) dataItem.get("list");
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
                      for (Entry<String, MtasDataItemNumberComparator> entry : comparatorList
                          .entrySet()) {
                        if (mergedComparatorList.containsKey(entry.getKey())) {
                          mergedComparatorList.get(entry.getKey())
                              .add(entry.getValue().getValue());
                        } else {
                          mergedComparatorList.put(entry.getKey(),
                              entry.getValue().clone());
                        }
                      }
                      if (!comparatorBoundariesList.containsKey(key)) {
                        comparatorBoundariesList.put(key,
                            new HashMap<String, MtasDataItemNumberComparator>());
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
                  log.debug(e);
                  dataItem.clear();
                }
              }
            }
          } catch (ClassCastException e) {
            log.debug(e);
            // shouldnt happen
          }
          shardResponse.getSolrResponse().setResponse(response);
        }
      }
    }
    // compute for each relevant termvector the mergedComparatorBoundary
    HashMap<String, HashMap<String, HashMap<String, MtasDataItemNumberComparator>>> recomputeFieldList = new HashMap<>();
    for (String field : mtasFields.list.keySet()) {
      List<ComponentTermVector> tvList = mtasFields.list
          .get(field).termVectorList;
      if (tvList != null) {
        for (ComponentTermVector tv : tvList) {
          SortedMap<String, MtasDataItemNumberComparator> mergedComparatorList;
          if (mergedComparatorLists.containsKey(tv.key)) {
            mergedComparatorList = mergedComparatorLists.get(tv.key);
            if (mergedComparatorList.size() < tv.number || tv.number <= 0) {
              // do nothing
            } else {
              final int sortDirection = tv.subComponentFunction.sortDirection
                  .equals(CodecUtil.SORT_DESC) ? -1 : 1;
              SortedSet<Map.Entry<String, MtasDataItemNumberComparator>> sortedSet = new TreeSet<>(
                  (Map.Entry<String, MtasDataItemNumberComparator> e1,
                      Map.Entry<String, MtasDataItemNumberComparator> e2) -> (e1
                          .getValue().compareTo(e2.getValue().getValue()) == 0)
                              ? e1.getKey().compareTo(e2.getKey())
                              : e1.getValue().compareTo(
                                  e2.getValue().getValue()) * sortDirection);
              sortedSet.addAll(mergedComparatorLists.get(tv.key).entrySet());
              Optional<Map.Entry<String, MtasDataItemNumberComparator>> optionalItem = sortedSet
                  .stream().skip(tv.number - 1L).findFirst();
              if (optionalItem.isPresent()) {
                mergedComparatorBoundaryList.put(tv.key,
                    optionalItem.get().getValue());
              }
            }
          }
        }
      }
      HashMap<String, HashMap<String, MtasDataItemNumberComparator>> recomputeList = new HashMap<>();
      if (tvList != null) {
        for (ComponentTermVector tv : tvList) {
          String key = tv.key;
          if (mergedComparatorBoundaryList.containsKey(key)
              && summedComparatorBoundaryList.containsKey(key)) {
            // set termvector to recompute
            recomputeList.put(key,
                new HashMap<String, MtasDataItemNumberComparator>());
            // sort
            List<Entry<String, MtasDataItemNumberComparator>> list = new LinkedList<>(
                comparatorBoundariesList.get(key).entrySet());
            Collections.sort(list,
                (Entry<String, MtasDataItemNumberComparator> e1,
                    Entry<String, MtasDataItemNumberComparator> e2) -> e1
                        .getValue().compareTo(e2.getValue().getValue()));
            HashMap<String, MtasDataItemNumberComparator> sortedHashMap = new LinkedHashMap<>();
            for (Iterator<Map.Entry<String, MtasDataItemNumberComparator>> it = list
                .iterator(); it.hasNext();) {
              Map.Entry<String, MtasDataItemNumberComparator> entry = it.next();
              sortedHashMap.put(entry.getKey(), entry.getValue());
            }

            MtasDataItemNumberComparator mainNewBoundary = mergedComparatorBoundaryList
                .get(key).recomputeBoundary(sortedHashMap.size());
            // System.out.println(
            // "MAIN NEW BOUNDARY for '" + key + "' : " + mainNewBoundary);

            MtasDataItemNumberComparator sum = null;
            int number = 0;
            for (Entry<String, MtasDataItemNumberComparator> entry : sortedHashMap
                .entrySet()) {
              MtasDataItemNumberComparator newBoundary = mainNewBoundary
                  .clone();
              MtasDataItemNumberComparator currentBoundary = entry.getValue();
              int compare = currentBoundary.compareTo(newBoundary.getValue());
              if (tv.subComponentFunction.sortDirection
                  .equals(CodecUtil.SORT_DESC)) {
                compare *= -1;
              }
              if (compare < 0) {
                HashMap<String, MtasDataItemNumberComparator> recomputeSubList = new HashMap<>();
                // sum not null if number>0, but do check
                if (number > 0 && sum != null
                    && tv.subComponentFunction.sortDirection
                        .equals(CodecUtil.SORT_DESC)) {
                  MtasDataItemNumberComparator tmpSumBoundary = mergedComparatorBoundaryList
                      .get(key);
                  tmpSumBoundary.subtract(sum.getValue());
                  MtasDataItemNumberComparator alternativeNewBoundary = tmpSumBoundary
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
                      recomputeSubList.put(entry.getKey(), newBoundary);
                    }
                  } else {
                    recomputeSubList.put(entry.getKey(), newBoundary);
                  }
                } else {
                  recomputeSubList.put(entry.getKey(), newBoundary);
                }
                if (!recomputeSubList.isEmpty()) {
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
      }
      if (!recomputeList.isEmpty()) {
        recomputeFieldList.put(field, recomputeList);
      }
    }

    // finally, recompute
    if (recomputeFieldList.size() > 0) {

      // remove output for termvectors in recompute list and get list of shards
      HashSet<String> shards = new HashSet<>();
      for (ShardRequest sreq : rb.finished) {
        if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
            && sreq.params.getBool(PARAM_MTAS_TERMVECTOR, false)) {
          for (ShardResponse shardResponse : sreq.responses) {
            NamedList<Object> response = shardResponse.getSolrResponse()
                .getResponse();
            String key;
            String field;
            String shardAddress = shardResponse.getShardAddress();
            try {
              ArrayList<NamedList<Object>> data = (ArrayList<NamedList<Object>>) response
                  .findRecursive("mtas", NAME);
              shards.add(shardAddress);
              if (data != null) {
                for (int i = 0; i < data.size(); i++) {
                  NamedList<Object> dataItem = data.get(i);
                  try {
                    key = (String) dataItem.get("key");
                    field = (String) dataItem.get("field");
                    boolean doClear;
                    doClear = field != null && key != null;
                    doClear = doClear ? recomputeFieldList.get(field) != null
                        : false;
                    doClear = doClear
                        ? recomputeFieldList.get(field).containsKey(key)
                        : false;
                    doClear = doClear ? recomputeFieldList.get(field).get(key)
                        .containsKey(shardAddress) : false;
                    if (doClear) {
                      dataItem.clear();
                      dataItem.add("key", key);
                    }
                  } catch (ClassCastException e) {
                    log.debug(e);
                    dataItem.clear();
                  }
                }
              }
            } catch (ClassCastException e) {
              log.debug(e);
              // shouldnt happen
            }
            shardResponse.getSolrResponse().setResponse(response);
          }
        }
      }

      // parameter
      HashMap<String, ModifiableSolrParams> requestParamList = new HashMap<>();
      for (String shardAddress : shards) {
        ModifiableSolrParams paramsNewRequest = new ModifiableSolrParams();
        int termvectorCounter = 0;
        for (String field : mtasFields.list.keySet()) {
          List<ComponentTermVector> tvList = mtasFields.list
              .get(field).termVectorList;
          if (recomputeFieldList.containsKey(field)) {
            HashMap<String, HashMap<String, MtasDataItemNumberComparator>> recomputeList = recomputeFieldList
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
                  if (tv.distances != null) {
                    int distanceCounter = 0;
                    for (SubComponentDistance distance : tv.distances) {
                      paramsNewRequest.add(
                          PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                              + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                              + distanceCounter + "."
                              + NAME_MTAS_TERMVECTOR_DISTANCE_TYPE,
                          distance.type);
                      paramsNewRequest.add(
                          PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                              + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                              + distanceCounter + "."
                              + NAME_MTAS_TERMVECTOR_DISTANCE_BASE,
                          distance.base);
                      if (distance.key != null) {
                        paramsNewRequest.add(
                            PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                + "." + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                                + distanceCounter + "."
                                + NAME_MTAS_TERMVECTOR_DISTANCE_KEY,
                            distance.key);
                      }
                      if (distance.minimum != null) {
                        paramsNewRequest.add(
                            PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                + "." + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                                + distanceCounter + "."
                                + NAME_MTAS_TERMVECTOR_DISTANCE_MINIMUM,
                            String.valueOf(distance.minimum));
                      }
                      if (distance.maximum != null) {
                        paramsNewRequest.add(
                            PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                + "." + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                                + distanceCounter + "."
                                + NAME_MTAS_TERMVECTOR_DISTANCE_MAXIMUM,
                            String.valueOf(distance.maximum));
                      }
                      if (distance.parameters != null) {
                        for (Entry<String, String> parameter : distance.parameters
                            .entrySet()) {
                          paramsNewRequest.add(
                              PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                  + "." + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                                  + distanceCounter + "."
                                  + NAME_MTAS_TERMVECTOR_DISTANCE_PARAMETER
                                  + "." + parameter.getKey(),
                              parameter.getValue());
                        }
                      }
                      distanceCounter++;
                    }
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
        if (!paramsNewRequest.getParameterNames().isEmpty()) {
          requestParamList.put(shardAddress, paramsNewRequest);
        }
      }

      // new requests
      for (Entry<String, ModifiableSolrParams> entry : requestParamList
          .entrySet()) {
        ShardRequest sreq = new ShardRequest();
        sreq.shards = new String[] { entry.getKey() };
        sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
        sreq.params = entry.getValue();
        sreq.params.add(CommonParams.FQ,
            rb.req.getParams().getParams(CommonParams.FQ));
        sreq.params.add(CommonParams.Q,
            rb.req.getParams().getParams(CommonParams.Q));
        sreq.params.add(CommonParams.CACHE,
            rb.req.getParams().getParams(CommonParams.CACHE));
        sreq.params.add(CommonParams.ROWS, "0");
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
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void distributedProcessMissingKey(ResponseBuilder rb,
      ComponentFields mtasFields) throws IOException {
    HashMap<String, HashMap<String, HashSet<String>>> missingTermvectorKeys = computeMissingTermvectorItemsPerShard(
        rb.finished, "mtas", NAME);
    for (Entry<String, HashMap<String, HashSet<String>>> entry : missingTermvectorKeys
        .entrySet()) {
      HashMap<String, HashSet<String>> missingTermvectorKeysShard = entry
          .getValue();
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
                if (!list.isEmpty()) {
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
                  if (tv.distances != null) {
                    int distanceCounter = 0;
                    for (SubComponentDistance distance : tv.distances) {
                      paramsNewRequest.add(
                          PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                              + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                              + distanceCounter + "."
                              + NAME_MTAS_TERMVECTOR_DISTANCE_TYPE,
                          distance.type);
                      paramsNewRequest.add(
                          PARAM_MTAS_TERMVECTOR + "." + termvectorCounter + "."
                              + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                              + distanceCounter + "."
                              + NAME_MTAS_TERMVECTOR_DISTANCE_BASE,
                          distance.base);
                      if (distance.key != null) {
                        paramsNewRequest.add(
                            PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                + "." + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                                + distanceCounter + "."
                                + NAME_MTAS_TERMVECTOR_DISTANCE_KEY,
                            distance.key);
                      }
                      if (distance.minimum != null) {
                        paramsNewRequest.add(
                            PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                + "." + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                                + distanceCounter + "."
                                + NAME_MTAS_TERMVECTOR_DISTANCE_MINIMUM,
                            String.valueOf(distance.minimum));
                      }
                      if (distance.maximum != null) {
                        paramsNewRequest.add(
                            PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                + "." + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                                + distanceCounter + "."
                                + NAME_MTAS_TERMVECTOR_DISTANCE_MAXIMUM,
                            String.valueOf(distance.maximum));
                      }
                      if (distance.parameters != null) {
                        for (Entry<String, String> parameter : distance.parameters
                            .entrySet()) {
                          paramsNewRequest.add(
                              PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                  + "." + NAME_MTAS_TERMVECTOR_DISTANCE + "."
                                  + distanceCounter + "."
                                  + NAME_MTAS_TERMVECTOR_DISTANCE_PARAMETER
                                  + "." + parameter.getKey(),
                              parameter.getValue());
                        }
                      }
                      distanceCounter++;
                    }
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
                  if (!list.isEmpty()) {
                    StringBuilder listValue = new StringBuilder();
                    String[] listList = list.toArray(new String[list.size()]);
                    for (int i = 0; i < listList.length; i++) {
                      if (i > 0) {
                        listValue.append(",");
                      }
                      listValue.append(listList[i].replace("\\", "\\\\")
                          .replace(",", "\\\\"));
                    }
                    paramsNewRequest.add(PARAM_MTAS_TERMVECTOR + "."
                        + termvectorCounter + "." + NAME_MTAS_TERMVECTOR_FULL,
                        "false");
                    paramsNewRequest
                        .add(
                            PARAM_MTAS_TERMVECTOR + "." + termvectorCounter
                                + "." + NAME_MTAS_TERMVECTOR_LIST,
                            listValue.toString());
                  }
                  termvectorCounter++;
                }
              }
            }
          }
          if (termvectorCounter > 0) {
            ShardRequest nsreq = new ShardRequest();
            nsreq.shards = new String[] { entry.getKey() };
            nsreq.purpose = ShardRequest.PURPOSE_PRIVATE;
            nsreq.params = new ModifiableSolrParams();
            nsreq.params.add(CommonParams.FQ,
                rb.req.getParams().getParams(CommonParams.FQ));
            nsreq.params.add(CommonParams.Q,
                rb.req.getParams().getParams(CommonParams.Q));
            nsreq.params.add(CommonParams.CACHE,
                rb.req.getParams().getParams(CommonParams.CACHE));
            nsreq.params.add(CommonParams.ROWS, "0");
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
   * @param requests the requests
   * @param args the args
   * @return the hash map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unchecked")
  private HashMap<String, HashMap<String, HashSet<String>>> computeMissingTermvectorItemsPerShard(
      List<ShardRequest> requests, String... args) throws IOException {
    HashMap<String, HashMap<String, HashSet<String>>> result = new HashMap<>();
    HashMap<String, HashMap<String, HashSet<String>>> itemsPerShardSets = new HashMap<>();
    HashMap<String, HashSet<String>> itemSets = new HashMap<>();
    // loop over responses different shards
    for (ShardRequest sreq : requests) {
      if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
          && sreq.params.getBool(PARAM_MTAS_TERMVECTOR, false)) {
        for (ShardResponse shardResponse : sreq.responses) {
          NamedList<Object> response = shardResponse.getSolrResponse()
              .getResponse();
          try {
            // get termvector data
            ArrayList<NamedList<Object>> data = (ArrayList<NamedList<Object>>) response
                .findRecursive(args);
            if (data != null) {
              // loop over temvector results
              for (int i = 0; i < data.size(); i++) {
                NamedList<Object> dataItem = data.get(i);
                try {
                  // get termvector result
                  String termvectorKey = (String) dataItem.get("key");
                  MtasSolrMtasResult list = (MtasSolrMtasResult) dataItem
                      .get("list");
                  if (termvectorKey != null && list != null) {
                    // get keys
                    Set<String> keyList = list.getKeyList();
                    HashMap<String, HashSet<String>> itemsPerShardSet;
                    HashSet<String> itemSet;
                    HashSet<String> tmpItemSet = new HashSet<>();
                    if (itemsPerShardSets.containsKey(termvectorKey)) {
                      itemsPerShardSet = itemsPerShardSets.get(termvectorKey);
                      itemSet = itemSets.get(termvectorKey);
                    } else {
                      itemsPerShardSet = new HashMap<>();
                      itemSet = new HashSet<>();
                      itemsPerShardSets.put(termvectorKey, itemsPerShardSet);
                      itemSets.put(termvectorKey, itemSet);
                    }
                    itemsPerShardSet.put(shardResponse.getShardAddress(),
                        tmpItemSet);
                    tmpItemSet.addAll(keyList);
                    itemSet.addAll(keyList);
                  }
                } catch (ClassCastException e) {
                  log.debug(e);
                }
              }
            }
          } catch (ClassCastException e) {
            log.debug(e);
          }
        }
      }
    }

    // construct result
    for (Entry<String, HashSet<String>> entry : itemSets.entrySet()) {
      String termvectorKey = entry.getKey();
      HashSet<String> termvectorKeyList = entry.getValue();
      if (itemsPerShardSets.containsKey(termvectorKey)) {
        HashMap<String, HashSet<String>> itemsPerShardSet = itemsPerShardSets
            .get(termvectorKey);
        for (Entry<String, HashSet<String>> subEntry : itemsPerShardSet
            .entrySet()) {
          String shardName = subEntry.getKey();
          HashMap<String, HashSet<String>> tmpShardKeySet;
          if (result.containsKey(shardName)) {
            tmpShardKeySet = result.get(shardName);
          } else {
            tmpShardKeySet = new HashMap<>();
            result.put(shardName, tmpShardKeySet);
          }
          HashSet<String> tmpResult = new HashSet<>();
          HashSet<String> shardItemsSet = subEntry.getValue();
          for (String termvectorKeyListItem : termvectorKeyList) {
            if (!shardItemsSet.contains(termvectorKeyListItem)) {
              tmpResult.add(termvectorKeyListItem);
            }
          }
          tmpShardKeySet.put(termvectorKey, tmpResult);
        }
      }
    }
    return result;
  }

}
