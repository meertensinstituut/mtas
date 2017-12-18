package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;

import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentPosition;
import mtas.codec.util.CodecComponent.ComponentSpan;
import mtas.codec.util.CodecComponent.ComponentStats;
import mtas.codec.util.CodecComponent.ComponentToken;
import mtas.codec.util.CodecComponent.SubComponentFunction;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.parser.function.ParseException;
import mtas.search.spans.util.MtasSpanQuery;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentStats.
 */
public class MtasSolrComponentStats
    implements MtasSolrComponent<ComponentStats> {

  /** The Constant log. */
  private static final Log log = LogFactory
      .getLog(MtasSolrComponentStats.class);

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant PARAM_MTAS_STATS. */
  public static final String PARAM_MTAS_STATS = MtasSolrSearchComponent.PARAM_MTAS
      + ".stats";

  /** The Constant PARAM_MTAS_STATS_POSITIONS. */
  public static final String PARAM_MTAS_STATS_POSITIONS = PARAM_MTAS_STATS
      + ".positions";

  /** The Constant NAME_MTAS_STATS_POSITIONS_FIELD. */
  public static final String NAME_MTAS_STATS_POSITIONS_FIELD = "field";

  /** The Constant NAME_MTAS_STATS_POSITIONS_KEY. */
  public static final String NAME_MTAS_STATS_POSITIONS_KEY = "key";

  /** The Constant NAME_MTAS_STATS_POSITIONS_TYPE. */
  public static final String NAME_MTAS_STATS_POSITIONS_TYPE = "type";

  /** The Constant NAME_MTAS_STATS_POSITIONS_MINIMUM. */
  public static final String NAME_MTAS_STATS_POSITIONS_MINIMUM = "minimum";

  /** The Constant NAME_MTAS_STATS_POSITIONS_MAXIMUM. */
  public static final String NAME_MTAS_STATS_POSITIONS_MAXIMUM = "maximum";

  /** The Constant PARAM_MTAS_STATS_TOKENS. */
  public static final String PARAM_MTAS_STATS_TOKENS = PARAM_MTAS_STATS
      + ".tokens";

  /** The Constant NAME_MTAS_STATS_TOKENS_FIELD. */
  public static final String NAME_MTAS_STATS_TOKENS_FIELD = "field";

  /** The Constant NAME_MTAS_STATS_TOKENS_KEY. */
  public static final String NAME_MTAS_STATS_TOKENS_KEY = "key";

  /** The Constant NAME_MTAS_STATS_TOKENS_TYPE. */
  public static final String NAME_MTAS_STATS_TOKENS_TYPE = "type";

  /** The Constant NAME_MTAS_STATS_TOKENS_MINIMUM. */
  public static final String NAME_MTAS_STATS_TOKENS_MINIMUM = "minimum";

  /** The Constant NAME_MTAS_STATS_TOKENS_MAXIMUM. */
  public static final String NAME_MTAS_STATS_TOKENS_MAXIMUM = "maximum";

  /** The Constant PARAM_MTAS_STATS_SPANS. */
  public static final String PARAM_MTAS_STATS_SPANS = PARAM_MTAS_STATS
      + ".spans";

  /** The Constant NAME_MTAS_STATS_SPANS_FIELD. */
  public static final String NAME_MTAS_STATS_SPANS_FIELD = "field";

  /** The Constant NAME_MTAS_STATS_SPANS_QUERY. */
  public static final String NAME_MTAS_STATS_SPANS_QUERY = "query";

  /** The Constant NAME_MTAS_STATS_SPANS_EXPAND. */
  public static final String NAME_MTAS_STATS_SPANS_EXPAND = "expand";

  /** The Constant NAME_MTAS_STATS_SPANS_KEY. */
  public static final String NAME_MTAS_STATS_SPANS_KEY = "key";

  /** The Constant NAME_MTAS_STATS_SPANS_TYPE. */
  public static final String NAME_MTAS_STATS_SPANS_TYPE = "type";

  /** The Constant NAME_MTAS_STATS_SPANS_MINIMUM. */
  public static final String NAME_MTAS_STATS_SPANS_MINIMUM = "minimum";

  /** The Constant NAME_MTAS_STATS_SPANS_MAXIMUM. */
  public static final String NAME_MTAS_STATS_SPANS_MAXIMUM = "maximum";

  /** The Constant NAME_MTAS_STATS_SPANS_FUNCTION. */
  public static final String NAME_MTAS_STATS_SPANS_FUNCTION = "function";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_FUNCTION_EXPRESSION. */
  public static final String SUBNAME_MTAS_STATS_SPANS_FUNCTION_EXPRESSION = "expression";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_FUNCTION_KEY. */
  public static final String SUBNAME_MTAS_STATS_SPANS_FUNCTION_KEY = "key";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_FUNCTION_TYPE. */
  public static final String SUBNAME_MTAS_STATS_SPANS_FUNCTION_TYPE = "type";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_TYPE. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_TYPE = "type";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_VALUE. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_VALUE = "value";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_IGNORE. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_IGNORE = "ignore";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_MAXIMUM_IGNORE_LENGTH. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_MAXIMUM_IGNORE_LENGTH = "maximumIgnoreLength";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_PREFIX. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_PREFIX = "prefix";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE = "variable";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE_NAME. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE_NAME = "name";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE_VALUE. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE_VALUE = "value";

  /**
   * Instantiates a new mtas solr component stats.
   *
   * @param searchComponent the search component
   */
  public MtasSolrComponentStats(MtasSolrSearchComponent searchComponent) {
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
    if (rb.req.getParams().getBool(PARAM_MTAS_STATS_POSITIONS, false)) {
      preparePositions(rb, mtasFields);
    }
    if (rb.req.getParams().getBool(PARAM_MTAS_STATS_TOKENS, false)) {
      prepareTokens(rb, mtasFields);
    }
    if (rb.req.getParams().getBool(PARAM_MTAS_STATS_SPANS, false)) {
      prepareSpans(rb, mtasFields);
    }
  }

  /**
   * Prepare positions.
   *
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void preparePositions(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = MtasSolrResultUtil
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_STATS_POSITIONS);
    if (!ids.isEmpty()) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] minima = new String[ids.size()];
      String[] maxima = new String[ids.size()];
      String[] types = new String[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_POSITIONS
            + "." + id + "." + NAME_MTAS_STATS_POSITIONS_FIELD, null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_STATS_POSITIONS + "." + id + "."
                + NAME_MTAS_STATS_POSITIONS_KEY, String.valueOf(tmpCounter))
            .trim();
        minima[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_POSITIONS
            + "." + id + "." + NAME_MTAS_STATS_POSITIONS_MINIMUM, null);
        maxima[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_POSITIONS
            + "." + id + "." + NAME_MTAS_STATS_POSITIONS_MAXIMUM, null);
        types[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_POSITIONS
            + "." + id + "." + NAME_MTAS_STATS_POSITIONS_TYPE, null);
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doStats = true;
      mtasFields.doStatsPositions = true;
      rb.setNeedDocSet(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas stats positions");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(uniqueKeyField));
        }
      }
      MtasSolrResultUtil.compareAndCheck(keys, fields,
          NAME_MTAS_STATS_POSITIONS_KEY, NAME_MTAS_STATS_POSITIONS_FIELD, true);
      MtasSolrResultUtil.compareAndCheck(minima, fields,
          NAME_MTAS_STATS_POSITIONS_MINIMUM, NAME_MTAS_STATS_POSITIONS_FIELD,
          false);
      MtasSolrResultUtil.compareAndCheck(maxima, fields,
          NAME_MTAS_STATS_POSITIONS_MAXIMUM, NAME_MTAS_STATS_POSITIONS_FIELD,
          false);
      MtasSolrResultUtil.compareAndCheck(types, fields,
          NAME_MTAS_STATS_POSITIONS_TYPE, NAME_MTAS_STATS_POSITIONS_FIELD,
          false);
      for (int i = 0; i < fields.length; i++) {
        String field = fields[i];
        String key = keys[i];
        String type = (types[i] == null) || (types[i].isEmpty()) ? null
            : types[i].trim();
        Double minimum = (minima[i] == null) || (minima[i].isEmpty()) ? null
            : Double.parseDouble(minima[i]);
        Double maximum = (maxima[i] == null) || (maxima[i].isEmpty()) ? null
            : Double.parseDouble(maxima[i]);
        try {
          mtasFields.list.get(field).statsPositionList
              .add(new ComponentPosition(key, minimum, maximum, type));
        } catch (ParseException e) {
          throw new IOException(e.getMessage());
        }
      }
    }
  }

  /**
   * Prepare tokens.
   *
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void prepareTokens(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = MtasSolrResultUtil
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_STATS_TOKENS);
    if (!ids.isEmpty()) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] minima = new String[ids.size()];
      String[] maxima = new String[ids.size()];
      String[] types = new String[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_TOKENS
            + "." + id + "." + NAME_MTAS_STATS_TOKENS_FIELD, null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_STATS_TOKENS + "." + id + "."
                + NAME_MTAS_STATS_TOKENS_KEY, String.valueOf(tmpCounter))
            .trim();
        minima[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_TOKENS
            + "." + id + "." + NAME_MTAS_STATS_TOKENS_MINIMUM, null);
        maxima[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_TOKENS
            + "." + id + "." + NAME_MTAS_STATS_TOKENS_MAXIMUM, null);
        types[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_TOKENS + "."
            + id + "." + NAME_MTAS_STATS_TOKENS_TYPE, null);
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doStats = true;
      mtasFields.doStatsTokens = true;
      rb.setNeedDocSet(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas stats tokens");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(uniqueKeyField));
        }
      }
      MtasSolrResultUtil.compareAndCheck(keys, fields,
          NAME_MTAS_STATS_TOKENS_KEY, NAME_MTAS_STATS_TOKENS_FIELD, true);
      MtasSolrResultUtil.compareAndCheck(minima, fields,
          NAME_MTAS_STATS_TOKENS_MINIMUM, NAME_MTAS_STATS_TOKENS_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(maxima, fields,
          NAME_MTAS_STATS_TOKENS_MAXIMUM, NAME_MTAS_STATS_TOKENS_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(types, fields,
          NAME_MTAS_STATS_TOKENS_TYPE, NAME_MTAS_STATS_TOKENS_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        String field = fields[i];
        String key = keys[i];
        String type = (types[i] == null) || (types[i].isEmpty()) ? null
            : types[i].trim();
        Double minimum = (minima[i] == null) || (minima[i].isEmpty()) ? null
            : Double.parseDouble(minima[i]);
        Double maximum = (maxima[i] == null) || (maxima[i].isEmpty()) ? null
            : Double.parseDouble(maxima[i]);
        try {
          mtasFields.list.get(field).statsTokenList
              .add(new ComponentToken(key, minimum, maximum, type));
        } catch (ParseException e) {
          throw new IOException(e.getMessage());
        }
      }
    }
  }

  /**
   * Prepare spans.
   *
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void prepareSpans(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    SortedSet<String> ids = MtasSolrResultUtil
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_STATS_SPANS);
    if (!ids.isEmpty()) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] minima = new String[ids.size()];
      String[] maxima = new String[ids.size()];
      String[] types = new String[ids.size()];
      String[][] functionExpressions = new String[ids.size()][];
      String[][] functionKeys = new String[ids.size()][];
      String[][] functionTypes = new String[ids.size()][];
      String[][] queryTypes = new String[ids.size()][];
      String[][] queryValues = new String[ids.size()][];
      String[][] queryIgnores = new String[ids.size()][];
      String[][] queryMaximumIgnoreLengths = new String[ids.size()][];
      String[][] queryPrefixes = new String[ids.size()][];
      HashMap<String, String[]>[][] queryVariables = new HashMap[ids.size()][];
      Boolean[] expand = new Boolean[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_SPANS + "."
            + id + "." + NAME_MTAS_STATS_SPANS_FIELD, null);
        keys[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_STATS_SPANS + "." + id + "." + NAME_MTAS_STATS_SPANS_KEY,
            String.valueOf(tmpCounter)).trim();
        minima[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_SPANS + "."
            + id + "." + NAME_MTAS_STATS_SPANS_MINIMUM, null);
        maxima[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_SPANS + "."
            + id + "." + NAME_MTAS_STATS_SPANS_MAXIMUM, null);
        types[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_SPANS + "."
            + id + "." + NAME_MTAS_STATS_SPANS_TYPE, null);
        Set<String> functionIds = MtasSolrResultUtil
            .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_STATS_SPANS
                + "." + id + "." + NAME_MTAS_STATS_SPANS_FUNCTION);
        functionExpressions[tmpCounter] = new String[functionIds.size()];
        functionKeys[tmpCounter] = new String[functionIds.size()];
        functionTypes[tmpCounter] = new String[functionIds.size()];
        int tmpSubCounter = 0;
        for (String functionId : functionIds) {
          functionKeys[tmpCounter][tmpSubCounter] = rb.req.getParams()
              .get(
                  PARAM_MTAS_STATS_SPANS + "." + id + "."
                      + NAME_MTAS_STATS_SPANS_FUNCTION + "." + functionId + "."
                      + SUBNAME_MTAS_STATS_SPANS_FUNCTION_KEY,
                  String.valueOf(tmpSubCounter))
              .trim();
          functionExpressions[tmpCounter][tmpSubCounter] = rb.req.getParams()
              .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                  + NAME_MTAS_STATS_SPANS_FUNCTION + "." + functionId + "."
                  + SUBNAME_MTAS_STATS_SPANS_FUNCTION_EXPRESSION, null);
          functionTypes[tmpCounter][tmpSubCounter] = rb.req.getParams()
              .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                  + NAME_MTAS_STATS_SPANS_FUNCTION + "." + functionId + "."
                  + SUBNAME_MTAS_STATS_SPANS_FUNCTION_TYPE, null);
          tmpSubCounter++;
        }

        Set<String> qIds = MtasSolrResultUtil
            .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_STATS_SPANS
                + "." + id + "." + NAME_MTAS_STATS_SPANS_QUERY);
        if (!qIds.isEmpty()) {
          int tmpQCounter = 0;
          queryTypes[tmpCounter] = new String[qIds.size()];
          queryValues[tmpCounter] = new String[qIds.size()];
          queryIgnores[tmpCounter] = new String[qIds.size()];
          queryMaximumIgnoreLengths[tmpCounter] = new String[qIds.size()];
          queryPrefixes[tmpCounter] = new String[qIds.size()];
          queryVariables[tmpCounter] = new HashMap[qIds.size()];
          for (String qId : qIds) {
            queryTypes[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                    + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                    + SUBNAME_MTAS_STATS_SPANS_QUERY_TYPE, null);
            queryValues[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                    + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                    + SUBNAME_MTAS_STATS_SPANS_QUERY_VALUE, null);
            queryIgnores[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                    + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                    + SUBNAME_MTAS_STATS_SPANS_QUERY_IGNORE, null);
            queryMaximumIgnoreLengths[tmpCounter][tmpQCounter] = rb.req
                .getParams().get(
                    PARAM_MTAS_STATS_SPANS + "." + id + "."
                        + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                        + SUBNAME_MTAS_STATS_SPANS_QUERY_MAXIMUM_IGNORE_LENGTH,
                    null);
            queryPrefixes[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                    + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                    + SUBNAME_MTAS_STATS_SPANS_QUERY_PREFIX, null);
            Set<String> vIds = MtasSolrResultUtil.getIdsFromParameters(
                rb.req.getParams(),
                PARAM_MTAS_STATS_SPANS + "." + id + "."
                    + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                    + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE);
            queryVariables[tmpCounter][tmpQCounter] = new HashMap<>();
            if (!vIds.isEmpty()) {
              HashMap<String, ArrayList<String>> tmpVariables = new HashMap<>();
              for (String vId : vIds) {
                String name = rb.req.getParams()
                    .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                        + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                        + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE + "." + vId
                        + "." + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE_NAME,
                        null);
                if (name != null) {
                  if (!tmpVariables.containsKey(name)) {
                    tmpVariables.put(name, new ArrayList<String>());
                  }
                  String value = rb.req.getParams()
                      .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                          + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                          + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE + "." + vId
                          + "." + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE_VALUE,
                          null);
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
                queryVariables[tmpCounter][tmpQCounter].put(entry.getKey(),
                    entry.getValue()
                        .toArray(new String[entry.getValue().size()]));
              }
            }
            tmpQCounter++;
          }
        } else {
          throw new IOException("no " + NAME_MTAS_STATS_SPANS_QUERY
              + " for mtas stats span " + id);
        }
        if (rb.req.getParams().getBool(PARAM_MTAS_STATS_SPANS + "." + id + "."
            + NAME_MTAS_STATS_SPANS_EXPAND, false)) {
          expand[tmpCounter] = true;
        } else {
          expand[tmpCounter] = false;
        }
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doStats = true;
      mtasFields.doStatsSpans = true;
      rb.setNeedDocSet(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas stats spans");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(uniqueKeyField));
        }
      }
      MtasSolrResultUtil.compareAndCheck(keys, fields,
          NAME_MTAS_STATS_SPANS_KEY, NAME_MTAS_STATS_SPANS_FIELD, true);
      MtasSolrResultUtil.compareAndCheck(minima, fields,
          NAME_MTAS_STATS_SPANS_MINIMUM, NAME_MTAS_STATS_SPANS_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(maxima, fields,
          NAME_MTAS_STATS_SPANS_MAXIMUM, NAME_MTAS_STATS_SPANS_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(types, fields,
          NAME_MTAS_STATS_SPANS_TYPE, NAME_MTAS_STATS_SPANS_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(types, fields,
          NAME_MTAS_STATS_SPANS_FUNCTION, NAME_MTAS_STATS_SPANS_FIELD, false);

      for (int i = 0; i < fields.length; i++) {
        ComponentField cf = mtasFields.list.get(fields[i]);
        int queryNumber = queryValues[i].length;
        MtasSpanQuery[] ql = new MtasSpanQuery[queryNumber];
        for (int j = 0; j < queryNumber; j++) {
          Integer maximumIgnoreLength = (queryMaximumIgnoreLengths[i][j] == null)
              ? null : Integer.parseInt(queryMaximumIgnoreLengths[i][j]);
          MtasSpanQuery q = MtasSolrResultUtil.constructQuery(queryValues[i][j],
              queryTypes[i][j], queryPrefixes[i][j], queryVariables[i][j],
              fields[i], queryIgnores[i][j], maximumIgnoreLength);
          // minimize number of queries
          if (cf.spanQueryList.contains(q)) {
            q = cf.spanQueryList.get(cf.spanQueryList.indexOf(q));
          } else {
            cf.spanQueryList.add(q);
          }
          ql[j] = q;
        }
        Double minimum = (minima[i] == null) || (minima[i].isEmpty()) ? null
            : Double.parseDouble(minima[i]);
        Double maximum = (maxima[i] == null) || (maxima[i].isEmpty()) ? null
            : Double.parseDouble(maxima[i]);
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + fields[i] + ":" + queryNumber
            : keys[i].trim();
        String type = (types[i] == null) || (types[i].isEmpty()) ? null
            : types[i].trim();
        String[] functionKey = functionKeys[i];
        String[] functionExpression = functionExpressions[i];
        String[] functionType = functionTypes[i];
        try {
          mtasFields.list.get(fields[i]).statsSpanList
              .add(new ComponentSpan(ql, key, minimum, maximum, type,
                  functionKey, functionExpression, functionType));
        } catch (ParseException e) {
          throw new IOException(e.getMessage());
        }
        if (expand[i]) {
          HashMap<String, String[]>[][] expandedQueryVariables = expandedQueryVariables(
              queryVariables[i]);
          for (int e = 0; e < expandedQueryVariables.length; e++) {
            MtasSpanQuery[] eql = new MtasSpanQuery[queryNumber];
            for (int j = 0; j < queryNumber; j++) {
              Integer maximumIgnoreLength = (queryMaximumIgnoreLengths[i][j] == null)
                  ? null : Integer.parseInt(queryMaximumIgnoreLengths[i][j]);
              MtasSpanQuery q = MtasSolrResultUtil.constructQuery(
                  queryValues[i][j], queryTypes[i][j], queryPrefixes[i][j],
                  expandedQueryVariables[e][j], fields[i], queryIgnores[i][j],
                  maximumIgnoreLength);
              // minimize number of queries
              if (cf.spanQueryList.contains(q)) {
                q = cf.spanQueryList.get(cf.spanQueryList.indexOf(q));
              } else {
                cf.spanQueryList.add(q);
              }
              eql[j] = q;
            }
            String newKey = generateKey(key + " (" + e + ")",
                expandedQueryVariables[e]);
            try {
              mtasFields.list.get(fields[i]).statsSpanList
                  .add(new ComponentSpan(eql, newKey, minimum, maximum, type,
                      functionKey, functionExpression, functionType));
            } catch (ParseException ee) {
              throw new IOException(ee.getMessage());
            }
          }
        }
      }
    } else {
      throw new IOException("missing parameters stats spans");
    }
  }

  /**
   * Generate key.
   *
   * @param key the key
   * @param queryVariables the query variables
   * @return the string
   */
  private String generateKey(String key,
      HashMap<String, String[]>[] queryVariables) {
    StringBuilder newKey = new StringBuilder(key);
    newKey.append(" -");
    for (int q = 0; q < queryVariables.length; q++) {
      if (queryVariables[q] != null && queryVariables[q].size() > 0) {
        for (String name : queryVariables[q].keySet()) {
          newKey.append(" q" + q + ":$" + name + "=");
          if (queryVariables[q].get(name) != null
              && queryVariables[q].get(name).length == 1) {
            newKey.append("'" + queryVariables[q].get(name)[0]
                .replace("\\", "\\\\").replace(",", "\\,") + "'");
          } else {
            newKey.append("-");
          }
        }
      }
    }
    return newKey.toString();
  }

  /**
   * Expanded query variables.
   *
   * @param queryVariables the query variables
   * @return the hash map[][]
   */
  private HashMap<String, String[]>[][] expandedQueryVariables(
      HashMap<String, String[]>[] queryVariables) {
    HashMap<String, String[]>[][] subResult = new HashMap[queryVariables.length][];
    for (int q = 0; q < queryVariables.length; q++) {
      subResult[q] = expandedQueryVariables(queryVariables[q]);
    }
    ArrayList<HashMap<String, String[]>[]> result = new ArrayList<>();
    generatePermutations(result, 0, subResult);
    return result.toArray(new HashMap[result.size()][]);
  }

  /**
   * Generate permutations.
   *
   * @param result the result
   * @param index the index
   * @param subResult the sub result
   */
  private void generatePermutations(
      ArrayList<HashMap<String, String[]>[]> result, int index,
      HashMap<String, String[]>[][] subResult) {
    int localIndex = index;
    HashMap<String, String[]>[] value = subResult[localIndex];
    if (localIndex == 0) {
      for (int i = 0; i < value.length; i++) {
        HashMap<String, String[]>[] resultItem = new HashMap[subResult.length];
        resultItem[localIndex] = value[i];
        result.add(resultItem);
      }
    } else {
      ArrayList<HashMap<String, String[]>[]> newResult = new ArrayList<>();
      for (int e = 0; e < result.size(); e++) {
        for (int i = 0; i < value.length; i++) {
          HashMap<String, String[]>[] resultItem = result.get(e);
          resultItem[localIndex] = value[i];
          newResult.add(resultItem);
        }
      }
      result.clear();
      result.addAll(newResult);
    }
    localIndex++;
    if (localIndex < subResult.length) {
      generatePermutations(result, localIndex, subResult);
    }
  }

  /**
   * Expanded query variables.
   *
   * @param queryVariables the query variables
   * @return the hash map[]
   */
  private HashMap<String, String[]>[] expandedQueryVariables(
      HashMap<String, String[]> queryVariables) {
    ArrayList<HashMap<String, String[]>> result = new ArrayList<>();
    Set<String> keys = queryVariables.keySet();
    generatePermutationsQueryVariables(result, keys, queryVariables);
    return result.toArray(new HashMap[result.size()]);
  }

  /**
   * Generate permutations query variables.
   *
   * @param result the result
   * @param keys the keys
   * @param queryVariables the query variables
   */
  private void generatePermutationsQueryVariables(
      ArrayList<HashMap<String, String[]>> result, Set<String> keys,
      HashMap<String, String[]> queryVariables) {
    if (keys != null && !keys.isEmpty()) {
      Set<String> newKeys = new HashSet<>();
      Iterator<String> it = keys.iterator();
      String key = it.next();
      String[] value = queryVariables.get(key);
      if (result.isEmpty()) {
        HashMap<String, String[]> newItem;
        if (value == null || value.length == 0) {
          newItem = new HashMap<>();
          newItem.put(key, value);
          result.add(newItem);
        } else {
          for (int j = 0; j < value.length; j++) {
            newItem = new HashMap<>();
            newItem.put(key, new String[] { value[j] });
            result.add(newItem);
          }
        }
      } else {
        ArrayList<HashMap<String, String[]>> newResult = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {
          HashMap<String, String[]> newItem;
          if (value == null || value.length == 0) {
            newItem = (HashMap<String, String[]>) result.get(i).clone();
            newItem.put(key, value);
            newResult.add(newItem);
          } else {
            for (int j = 0; j < value.length; j++) {
              newItem = (HashMap<String, String[]>) result.get(i).clone();
              newItem.put(key, new String[] { value[j] });
              newResult.add(newItem);
            }
          }
        }
        result.clear();
        result.addAll(newResult);
      }
      while (it.hasNext()) {
        newKeys.add(it.next());
      }
      generatePermutationsQueryVariables(result, newKeys, queryVariables);
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
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      // do nothing
    } else {
      // remove stats for other requests
      sreq.params.remove(PARAM_MTAS_STATS);
      sreq.params.remove(PARAM_MTAS_STATS_POSITIONS);
      Set<String> keys = MtasSolrResultUtil
          .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_STATS_POSITIONS);
      for (String key : keys) {
        sreq.params.remove(
            PARAM_MTAS_STATS + "." + key + "." + NAME_MTAS_STATS_POSITIONS_KEY);
        sreq.params.remove(PARAM_MTAS_STATS + "." + key + "."
            + NAME_MTAS_STATS_POSITIONS_FIELD);
        sreq.params.remove(PARAM_MTAS_STATS + "." + key + "."
            + NAME_MTAS_STATS_POSITIONS_TYPE);
        sreq.params.remove(PARAM_MTAS_STATS + "." + key + "."
            + NAME_MTAS_STATS_POSITIONS_MAXIMUM);
        sreq.params.remove(PARAM_MTAS_STATS + "." + key + "."
            + NAME_MTAS_STATS_POSITIONS_MINIMUM);
      }
      sreq.params.remove(PARAM_MTAS_STATS_TOKENS);
      keys = MtasSolrResultUtil.getIdsFromParameters(rb.req.getParams(),
          PARAM_MTAS_STATS_TOKENS);
      for (String key : keys) {
        sreq.params.remove(
            PARAM_MTAS_STATS + "." + key + "." + NAME_MTAS_STATS_TOKENS_KEY);
        sreq.params.remove(
            PARAM_MTAS_STATS + "." + key + "." + NAME_MTAS_STATS_TOKENS_FIELD);
        sreq.params.remove(
            PARAM_MTAS_STATS + "." + key + "." + NAME_MTAS_STATS_TOKENS_TYPE);
        sreq.params.remove(PARAM_MTAS_STATS + "." + key + "."
            + NAME_MTAS_STATS_TOKENS_MAXIMUM);
        sreq.params.remove(PARAM_MTAS_STATS + "." + key + "."
            + NAME_MTAS_STATS_TOKENS_MINIMUM);
      }
      sreq.params.remove(PARAM_MTAS_STATS_SPANS);
      keys = MtasSolrResultUtil.getIdsFromParameters(rb.req.getParams(),
          PARAM_MTAS_STATS_SPANS);
      for (String key : keys) {
        sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
            + NAME_MTAS_STATS_SPANS_KEY);
        sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
            + NAME_MTAS_STATS_SPANS_FIELD);
        sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
            + NAME_MTAS_STATS_SPANS_TYPE);
        sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
            + NAME_MTAS_STATS_SPANS_MAXIMUM);
        sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
            + NAME_MTAS_STATS_SPANS_MINIMUM);
        Set<String> subKeys = MtasSolrResultUtil
            .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_STATS_SPANS
                + "." + key + "." + NAME_MTAS_STATS_SPANS_FUNCTION);
        for (String subKey : subKeys) {
          sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
              + NAME_MTAS_STATS_SPANS_FUNCTION + "." + subKey + "."
              + SUBNAME_MTAS_STATS_SPANS_FUNCTION_EXPRESSION);
          sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
              + NAME_MTAS_STATS_SPANS_FUNCTION + "." + subKey + "."
              + SUBNAME_MTAS_STATS_SPANS_FUNCTION_KEY);
          sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
              + NAME_MTAS_STATS_SPANS_FUNCTION + "." + subKey + "."
              + SUBNAME_MTAS_STATS_SPANS_FUNCTION_TYPE);
        }
        subKeys = MtasSolrResultUtil.getIdsFromParameters(rb.req.getParams(),
            PARAM_MTAS_STATS_SPANS + "." + key + "."
                + NAME_MTAS_STATS_SPANS_QUERY);
        for (String subKey : subKeys) {
          sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
              + NAME_MTAS_STATS_SPANS_QUERY + "." + subKey + "."
              + SUBNAME_MTAS_STATS_SPANS_QUERY_IGNORE);
          sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
              + NAME_MTAS_STATS_SPANS_QUERY + "." + subKey + "."
              + SUBNAME_MTAS_STATS_SPANS_QUERY_MAXIMUM_IGNORE_LENGTH);
          sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
              + NAME_MTAS_STATS_SPANS_QUERY + "." + subKey + "."
              + SUBNAME_MTAS_STATS_SPANS_QUERY_PREFIX);
          sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
              + NAME_MTAS_STATS_SPANS_QUERY + "." + subKey + "."
              + SUBNAME_MTAS_STATS_SPANS_QUERY_TYPE);
          sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
              + NAME_MTAS_STATS_SPANS_QUERY + "." + subKey + "."
              + SUBNAME_MTAS_STATS_SPANS_QUERY_VALUE);
          Set<String> subSubKeys = MtasSolrResultUtil.getIdsFromParameters(
              rb.req.getParams(),
              PARAM_MTAS_STATS_SPANS + "." + key + "."
                  + NAME_MTAS_STATS_SPANS_QUERY + "." + subKey + "."
                  + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE);
          for (String subSubKey : subSubKeys) {
            sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
                + NAME_MTAS_STATS_SPANS_QUERY + "." + subKey + "."
                + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE + "." + subSubKey
                + "." + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE_NAME);
            sreq.params.remove(PARAM_MTAS_STATS_SPANS + "." + key + "."
                + NAME_MTAS_STATS_SPANS_QUERY + "." + subKey + "."
                + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE + "." + subSubKey
                + "." + SUBNAME_MTAS_STATS_SPANS_QUERY_VARIABLE_VALUE);
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
  @Override
  public SimpleOrderedMap<Object> create(ComponentStats response,
      Boolean encode) throws IOException {
    if (response instanceof ComponentPosition) {
      return createPosition((ComponentPosition) response, encode);
    } else if (response instanceof ComponentToken) {
      return createToken((ComponentToken) response, encode);
    } else if (response instanceof ComponentSpan) {
      return createSpan((ComponentSpan) response, encode);
    } else {
      throw new IOException("incorrect type " + response.getClass());
    }
  }

  /**
   * Creates the position.
   *
   * @param position the position
   * @param encode the encode
   * @return the simple ordered map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private SimpleOrderedMap<Object> createPosition(ComponentPosition position,
      Boolean encode) throws IOException {
    // System.out.println("Create stats position " + position.dataType + " "
    // + position.statsType + " " + position.statsItems + " --- " + encode);
    SimpleOrderedMap<Object> mtasPositionResponse = new SimpleOrderedMap<>();
    mtasPositionResponse.add("key", position.key);
    MtasSolrMtasResult data = new MtasSolrMtasResult(position.dataCollector,
        position.dataType, position.statsType, position.statsItems, null, null);
    if (encode) {
      mtasPositionResponse.add("_encoded_data",
          MtasSolrResultUtil.encode(data));
    } else {
      mtasPositionResponse.add(position.dataCollector.getCollectorType(), data);
      MtasSolrResultUtil.rewrite(mtasPositionResponse, searchComponent);
    }
    return mtasPositionResponse;
  }

  /**
   * Creates the token.
   *
   * @param token the token
   * @param encode the encode
   * @return the simple ordered map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private SimpleOrderedMap<Object> createToken(ComponentToken token,
      Boolean encode) throws IOException {
    // System.out.println("Create stats position " + position.dataType + " "
    // + position.statsType + " " + position.statsItems + " --- " + encode);
    SimpleOrderedMap<Object> mtasTokenResponse = new SimpleOrderedMap<>();
    mtasTokenResponse.add("key", token.key);
    MtasSolrMtasResult data = new MtasSolrMtasResult(token.dataCollector,
        token.dataType, token.statsType, token.statsItems, null, null);
    if (encode) {
      mtasTokenResponse.add("_encoded_data", MtasSolrResultUtil.encode(data));
    } else {
      mtasTokenResponse.add(token.dataCollector.getCollectorType(), data);
      MtasSolrResultUtil.rewrite(mtasTokenResponse, searchComponent);
    }
    return mtasTokenResponse;
  }

  /**
   * Creates the span.
   *
   * @param span the span
   * @param encode the encode
   * @return the simple ordered map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unchecked")
  private SimpleOrderedMap<Object> createSpan(ComponentSpan span,
      Boolean encode) throws IOException {
    // System.out.println("Create stats span " + span.dataType + " "
    // + span.statsType + " " + span.statsItems + " --- " + encode);
    SimpleOrderedMap<Object> mtasSpanResponse = new SimpleOrderedMap<>();
    mtasSpanResponse.add("key", span.key);
    HashMap<MtasDataCollector<?, ?>, HashMap<String, MtasSolrMtasResult>> functionData = new HashMap<>();
    HashMap<String, MtasSolrMtasResult> functionDataItem = new HashMap<>();
    functionData.put(span.dataCollector, functionDataItem);
    if (span.functions != null) {
      for (SubComponentFunction function : span.functions) {
        function.dataCollector.close();
        functionDataItem.put(function.key,
            new MtasSolrMtasResult(function.dataCollector,
                new String[] { function.dataType },
                new String[] { function.statsType },
                new SortedSet[] { function.statsItems }, new List[] {null}, new String[] { null },
                new String[] { null }, new Integer[] { 0 },
                new Integer[] { Integer.MAX_VALUE }, null));
      }
    }
    MtasSolrMtasResult data = new MtasSolrMtasResult(span.dataCollector,
        span.dataType, span.statsType, span.statsItems, null, functionData);
    if (encode) {
      mtasSpanResponse.add("_encoded_data", MtasSolrResultUtil.encode(data));
    } else {
      mtasSpanResponse.add(span.dataCollector.getCollectorType(), data);
      MtasSolrResultUtil.rewrite(mtasSpanResponse, searchComponent);
    }
    return mtasSpanResponse;
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
    if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
        && rb.stage >= ResponseBuilder.STAGE_EXECUTE_QUERY
        && rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
      for (ShardRequest sreq : rb.finished) {
        if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
            && sreq.params.getBool(PARAM_MTAS_STATS, false)) {
          for (ShardResponse shardResponse : sreq.responses) {
            NamedList<Object> response = shardResponse.getSolrResponse()
                .getResponse();
            try {
              ArrayList<NamedList<Object>> data = (ArrayList<NamedList<Object>>) response
                  .findRecursive("mtas", "stats");
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
  @SuppressWarnings("unchecked")
  public void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    // rewrite
    NamedList<Object> mtasResponse = null;
    try {
      mtasResponse = (NamedList<Object>) rb.rsp.getValues().get("mtas");
    } catch (ClassCastException e) {
      log.debug(e);
      mtasResponse = null;
    }
    if (mtasResponse != null) {
      NamedList<Object> mtasResponseStats;
      try {
        mtasResponseStats = (NamedList<Object>) mtasResponse.get("stats");
        if (mtasResponseStats != null) {
          MtasSolrResultUtil.rewrite(mtasResponseStats, searchComponent);
        }
      } catch (ClassCastException e) {
        log.debug(e);
        mtasResponse.remove("stats");
      }
    }
  }

}
