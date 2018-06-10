package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;

import mtas.codec.util.CodecComponent.ComponentFacet;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.SubComponentFunction;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.parser.function.ParseException;
import mtas.search.spans.util.MtasSpanQuery;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentFacet.
 */
public class MtasSolrComponentFacet
    implements MtasSolrComponent<ComponentFacet> {

  /** The Constant log. */
  private static final Log log = LogFactory
      .getLog(MtasSolrComponentFacet.class);

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant NAME. */
  public static final String NAME = "facet";

  /** The Constant PARAM_MTAS_FACET. */
  public static final String PARAM_MTAS_FACET = MtasSolrSearchComponent.PARAM_MTAS
      + "." + NAME;

  /** The Constant NAME_MTAS_FACET_KEY. */
  public static final String NAME_MTAS_FACET_KEY = "key";

  /** The Constant NAME_MTAS_FACET_FIELD. */
  public static final String NAME_MTAS_FACET_FIELD = "field";

  /** The Constant NAME_MTAS_FACET_QUERY. */
  private static final String NAME_MTAS_FACET_QUERY = "query";

  /** The Constant NAME_MTAS_FACET_BASE. */
  private static final String NAME_MTAS_FACET_BASE = "base";

  /** The Constant SUBNAME_MTAS_FACET_QUERY_TYPE. */
  public static final String SUBNAME_MTAS_FACET_QUERY_TYPE = "type";

  /** The Constant SUBNAME_MTAS_FACET_QUERY_VALUE. */
  public static final String SUBNAME_MTAS_FACET_QUERY_VALUE = "value";

  /** The Constant SUBNAME_MTAS_FACET_QUERY_PREFIX. */
  public static final String SUBNAME_MTAS_FACET_QUERY_PREFIX = "prefix";

  /** The Constant SUBNAME_MTAS_FACET_QUERY_IGNORE. */
  public static final String SUBNAME_MTAS_FACET_QUERY_IGNORE = "ignore";

  /** The Constant SUBNAME_MTAS_FACET_QUERY_MAXIMUM_IGNORE_LENGTH. */
  public static final String SUBNAME_MTAS_FACET_QUERY_MAXIMUM_IGNORE_LENGTH = "maximumIgnoreLength";

  /** The Constant SUBNAME_MTAS_FACET_QUERY_VARIABLE. */
  public static final String SUBNAME_MTAS_FACET_QUERY_VARIABLE = "variable";

  /** The Constant SUBNAME_MTAS_FACET_QUERY_VARIABLE_NAME. */
  public static final String SUBNAME_MTAS_FACET_QUERY_VARIABLE_NAME = "name";

  /** The Constant SUBNAME_MTAS_FACET_QUERY_VARIABLE_VALUE. */
  public static final String SUBNAME_MTAS_FACET_QUERY_VARIABLE_VALUE = "value";

  /** The Constant SUBNAME_MTAS_FACET_BASE_FIELD. */
  public static final String SUBNAME_MTAS_FACET_BASE_FIELD = "field";

  /** The Constant SUBNAME_MTAS_FACET_BASE_TYPE. */
  public static final String SUBNAME_MTAS_FACET_BASE_TYPE = "type";

  /** The Constant SUBNAME_MTAS_FACET_BASE_SORT_TYPE. */
  public static final String SUBNAME_MTAS_FACET_BASE_SORT_TYPE = "sort.type";

  /** The Constant SUBNAME_MTAS_FACET_BASE_SORT_DIRECTION. */
  public static final String SUBNAME_MTAS_FACET_BASE_SORT_DIRECTION = "sort.direction";

  /** The Constant SUBNAME_MTAS_FACET_BASE_NUMBER. */
  public static final String SUBNAME_MTAS_FACET_BASE_NUMBER = "number";

  /** The Constant SUBNAME_MTAS_FACET_BASE_MINIMUM. */
  public static final String SUBNAME_MTAS_FACET_BASE_MINIMUM = "minimum";

  /** The Constant SUBNAME_MTAS_FACET_BASE_MAXIMUM. */
  public static final String SUBNAME_MTAS_FACET_BASE_MAXIMUM = "maximum";

  /** The Constant SUBNAME_MTAS_FACET_BASE_FUNCTION. */
  public static final String SUBNAME_MTAS_FACET_BASE_FUNCTION = "function";

  /** The Constant SUBNAME_MTAS_FACET_BASE_FUNCTION_KEY. */
  public static final String SUBNAME_MTAS_FACET_BASE_FUNCTION_KEY = "key";

  /** The Constant SUBNAME_MTAS_FACET_BASE_FUNCTION_EXPRESSION. */
  public static final String SUBNAME_MTAS_FACET_BASE_FUNCTION_EXPRESSION = "expression";

  /** The Constant SUBNAME_MTAS_FACET_BASE_FUNCTION_TYPE. */
  public static final String SUBNAME_MTAS_FACET_BASE_FUNCTION_TYPE = "type";

  /** The Constant SUBNAME_MTAS_FACET_BASE_RANGE. */
  public static final String SUBNAME_MTAS_FACET_BASE_RANGE = "range";

  /** The Constant SUBNAME_MTAS_FACET_BASE_RANGE_SIZE. */
  public static final String SUBNAME_MTAS_FACET_BASE_RANGE_SIZE = "size";

  /** The Constant SUBNAME_MTAS_FACET_BASE_RANGE_BASE. */
  public static final String SUBNAME_MTAS_FACET_BASE_RANGE_BASE = "base";

  /**
   * Instantiates a new mtas solr component facet.
   *
   * @param searchComponent the search component
   */
  public MtasSolrComponentFacet(MtasSolrSearchComponent searchComponent) {
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
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_FACET);
    if (!ids.isEmpty()) {
      int tmpCounter = 0;
      String tmpValue;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[][] queryTypes = new String[ids.size()][];
      String[][] queryValues = new String[ids.size()][];
      String[][] queryPrefixes = new String[ids.size()][];
      String[][] queryIgnores = new String[ids.size()][];
      String[][] queryMaximumIgnoreLengths = new String[ids.size()][];
      HashMap<String, String[]>[][] queryVariables = new HashMap[ids.size()][];
      String[][] baseFields = new String[ids.size()][];
      String[][] baseFieldTypes = new String[ids.size()][];
      String[][] baseTypes = new String[ids.size()][];
      Double[][] baseRangeSizes = new Double[ids.size()][];
      Double[][] baseRangeBases = new Double[ids.size()][];
      String[][] baseSortTypes = new String[ids.size()][];
      String[][] baseSortDirections = new String[ids.size()][];
      Integer[][] baseNumbers = new Integer[ids.size()][];
      Double[][] baseMinima = new Double[ids.size()][];
      Double[][] baseMaxima = new Double[ids.size()][];
      String[][][] baseFunctionExpressions = new String[ids.size()][][];
      String[][][] baseFunctionKeys = new String[ids.size()][][];
      String[][][] baseFunctionTypes = new String[ids.size()][][];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_FIELD, null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_KEY,
                String.valueOf(tmpCounter))
            .trim();
        Set<String> qIds = MtasSolrResultUtil.getIdsFromParameters(
            rb.req.getParams(),
            PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_QUERY);
        if (!qIds.isEmpty()) {
          int tmpQCounter = 0;
          queryTypes[tmpCounter] = new String[qIds.size()];
          queryValues[tmpCounter] = new String[qIds.size()];
          queryPrefixes[tmpCounter] = new String[qIds.size()];
          queryIgnores[tmpCounter] = new String[qIds.size()];
          queryMaximumIgnoreLengths[tmpCounter] = new String[qIds.size()];
          queryVariables[tmpCounter] = new HashMap[qIds.size()];
          for (String qId : qIds) {
            queryTypes[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_QUERY
                        + "." + qId + "." + SUBNAME_MTAS_FACET_QUERY_TYPE,
                    null);
            queryValues[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_QUERY
                        + "." + qId + "." + SUBNAME_MTAS_FACET_QUERY_VALUE,
                    null);
            queryPrefixes[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_QUERY
                        + "." + qId + "." + SUBNAME_MTAS_FACET_QUERY_PREFIX,
                    null);
            queryIgnores[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_QUERY
                        + "." + qId + "." + SUBNAME_MTAS_FACET_QUERY_IGNORE,
                    null);
            queryMaximumIgnoreLengths[tmpCounter][tmpQCounter] = rb.req
                .getParams()
                .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_QUERY
                    + "." + qId + "."
                    + SUBNAME_MTAS_FACET_QUERY_MAXIMUM_IGNORE_LENGTH, null);
            Set<String> vIds = MtasSolrResultUtil.getIdsFromParameters(
                rb.req.getParams(),
                PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_QUERY + "."
                    + qId + "." + SUBNAME_MTAS_FACET_QUERY_VARIABLE);
            queryVariables[tmpCounter][tmpQCounter] = new HashMap<>();
            if (!vIds.isEmpty()) {
              HashMap<String, ArrayList<String>> tmpVariables = new HashMap<>();
              for (String vId : vIds) {
                String name = rb.req.getParams().get(PARAM_MTAS_FACET + "." + id
                    + "." + NAME_MTAS_FACET_QUERY + "." + qId + "."
                    + SUBNAME_MTAS_FACET_QUERY_VARIABLE + "." + vId + "."
                    + SUBNAME_MTAS_FACET_QUERY_VARIABLE_NAME, null);
                if (name != null) {
                  if (!tmpVariables.containsKey(name)) {
                    tmpVariables.put(name, new ArrayList<String>());
                  }
                  String value = rb.req.getParams().get(PARAM_MTAS_FACET + "."
                      + id + "." + NAME_MTAS_FACET_QUERY + "." + qId + "."
                      + SUBNAME_MTAS_FACET_QUERY_VARIABLE + "." + vId + "."
                      + SUBNAME_MTAS_FACET_QUERY_VARIABLE_VALUE, null);
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
          throw new IOException(
              "no " + NAME_MTAS_FACET_QUERY + " for mtas facet " + id);
        }
        Set<String> bIds = MtasSolrResultUtil.getIdsFromParameters(
            rb.req.getParams(),
            PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE);
        if (!bIds.isEmpty()) {
          int tmpBCounter = 0;
          baseFields[tmpCounter] = new String[bIds.size()];
          baseFieldTypes[tmpCounter] = new String[bIds.size()];
          baseTypes[tmpCounter] = new String[bIds.size()];
          baseRangeSizes[tmpCounter] = new Double[bIds.size()];
          baseRangeBases[tmpCounter] = new Double[bIds.size()];
          baseSortTypes[tmpCounter] = new String[bIds.size()];
          baseSortDirections[tmpCounter] = new String[bIds.size()];
          baseNumbers[tmpCounter] = new Integer[bIds.size()];
          baseMinima[tmpCounter] = new Double[bIds.size()];
          baseMaxima[tmpCounter] = new Double[bIds.size()];
          baseFunctionKeys[tmpCounter] = new String[bIds.size()][];
          baseFunctionExpressions[tmpCounter] = new String[bIds.size()][];
          baseFunctionTypes[tmpCounter] = new String[bIds.size()][];
          for (String bId : bIds) {
            baseFields[tmpCounter][tmpBCounter] = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                        + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_FIELD,
                    null);
            baseFieldTypes[tmpCounter][tmpBCounter] = getFieldType(
                rb.req.getSchema(), baseFields[tmpCounter][tmpBCounter]);
            baseTypes[tmpCounter][tmpBCounter] = rb.req.getParams()
                .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                    + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_TYPE, null);
            tmpValue = rb.req.getParams()
                .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                    + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_RANGE + "."
                    + SUBNAME_MTAS_FACET_BASE_RANGE_SIZE, null);
            baseRangeSizes[tmpCounter][tmpBCounter] = tmpValue == null ? null
                : Double.parseDouble(tmpValue);
            tmpValue = rb.req.getParams()
                .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                    + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_RANGE + "."
                    + SUBNAME_MTAS_FACET_BASE_RANGE_BASE, null);
            baseRangeBases[tmpCounter][tmpBCounter] = tmpValue == null ? null
                : Double.parseDouble(tmpValue);
            baseSortTypes[tmpCounter][tmpBCounter] = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                        + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_SORT_TYPE,
                    null);
            baseSortDirections[tmpCounter][tmpBCounter] = rb.req.getParams()
                .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                    + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_SORT_DIRECTION,
                    null);
            tmpValue = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                        + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_NUMBER,
                    null);
            baseNumbers[tmpCounter][tmpBCounter] = tmpValue != null
                ? getPositiveInteger(tmpValue) : null;
            tmpValue = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                        + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_MINIMUM,
                    null);
            baseMinima[tmpCounter][tmpBCounter] = tmpValue != null
                ? getDouble(tmpValue) : null;
            tmpValue = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                        + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_MAXIMUM,
                    null);
            baseMaxima[tmpCounter][tmpBCounter] = tmpValue != null
                ? getDouble(tmpValue) : null;
            Set<String> functionIds = MtasSolrResultUtil.getIdsFromParameters(
                rb.req.getParams(),
                PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE + "."
                    + bId + "." + SUBNAME_MTAS_FACET_BASE_FUNCTION);
            baseFunctionExpressions[tmpCounter][tmpBCounter] = new String[functionIds
                .size()];
            baseFunctionKeys[tmpCounter][tmpBCounter] = new String[functionIds
                .size()];
            baseFunctionTypes[tmpCounter][tmpBCounter] = new String[functionIds
                .size()];
            int tmpSubCounter = 0;
            for (String functionId : functionIds) {
              baseFunctionKeys[tmpCounter][tmpBCounter][tmpSubCounter] = rb.req
                  .getParams()
                  .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                      + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_FUNCTION + "."
                      + functionId + "." + SUBNAME_MTAS_FACET_BASE_FUNCTION_KEY,
                      String.valueOf(tmpSubCounter))
                  .trim();
              baseFunctionExpressions[tmpCounter][tmpBCounter][tmpSubCounter] = rb.req
                  .getParams()
                  .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                      + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_FUNCTION + "."
                      + functionId + "."
                      + SUBNAME_MTAS_FACET_BASE_FUNCTION_EXPRESSION, null);
              baseFunctionTypes[tmpCounter][tmpBCounter][tmpSubCounter] = rb.req
                  .getParams()
                  .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                      + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_FUNCTION + "."
                      + functionId + "."
                      + SUBNAME_MTAS_FACET_BASE_FUNCTION_TYPE, null);
              tmpSubCounter++;
            }
            tmpBCounter++;
          }
        } else {
          throw new IOException(
              "no " + NAME_MTAS_FACET_BASE + " for mtas facet " + id);
        }
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doFacet = true;
      rb.setNeedDocSet(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas facet");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(uniqueKeyField));
        }
      }
      MtasSolrResultUtil.compareAndCheck(keys, fields, NAME_MTAS_FACET_KEY,
          NAME_MTAS_FACET_FIELD, true);
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
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + fields[i] : keys[i].trim();
        try {
          mtasFields.list.get(fields[i]).facetList.add(new ComponentFacet(ql,
              fields[i], key, baseFields[i], baseFieldTypes[i], baseTypes[i],
              baseRangeSizes[i], baseRangeBases[i], baseSortTypes[i],
              baseSortDirections[i], baseNumbers[i], baseMinima[i],
              baseMaxima[i], baseFunctionKeys[i], baseFunctionExpressions[i],
              baseFunctionTypes[i]));
        } catch (ParseException e) {
          throw new IOException(e.getMessage());
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
    if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
        && sreq.params.getBool(PARAM_MTAS_FACET, false)) {
      if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
        // do nothing
      } else {
        // remove prefix for other requests
        Set<String> keys = MtasSolrResultUtil
            .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_FACET);
        sreq.params.remove(PARAM_MTAS_FACET);
        for (String key : keys) {
          sreq.params.remove(
              PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_FIELD);
          sreq.params
              .remove(PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_KEY);
          Set<String> subKeys = MtasSolrResultUtil.getIdsFromParameters(
              rb.req.getParams(),
              PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_QUERY);
          for (String subKey : subKeys) {
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_QUERY + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_QUERY_TYPE);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_QUERY + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_QUERY_VALUE);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_QUERY + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_QUERY_PREFIX);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_QUERY + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_QUERY_IGNORE);
            sreq.params.remove(PARAM_MTAS_FACET + "." + key + "."
                + NAME_MTAS_FACET_QUERY + "." + subKey + "."
                + SUBNAME_MTAS_FACET_QUERY_MAXIMUM_IGNORE_LENGTH);
            Set<String> subSubKeys = MtasSolrResultUtil
                .getIdsFromParameters(rb.req.getParams(),
                    PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_QUERY
                        + "." + subKey + "."
                        + SUBNAME_MTAS_FACET_QUERY_VARIABLE);
            for (String subSubKey : subSubKeys) {
              sreq.params.remove(PARAM_MTAS_FACET + "." + key + "."
                  + NAME_MTAS_FACET_QUERY + "." + subKey + "."
                  + SUBNAME_MTAS_FACET_QUERY_VARIABLE + "." + subSubKey + "."
                  + SUBNAME_MTAS_FACET_QUERY_VARIABLE_NAME);
              sreq.params.remove(PARAM_MTAS_FACET + "." + key + "."
                  + NAME_MTAS_FACET_QUERY + "." + subKey + "."
                  + SUBNAME_MTAS_FACET_QUERY_VARIABLE + "." + subSubKey + "."
                  + SUBNAME_MTAS_FACET_QUERY_VARIABLE_VALUE);
            }
          }
          subKeys = MtasSolrResultUtil.getIdsFromParameters(rb.req.getParams(),
              PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE);
          for (String subKey : subKeys) {
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_BASE_FIELD);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_BASE_TYPE);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_BASE_RANGE + "."
                    + SUBNAME_MTAS_FACET_BASE_RANGE_SIZE);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_BASE_RANGE + "."
                    + SUBNAME_MTAS_FACET_BASE_RANGE_BASE);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_BASE_MAXIMUM);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_BASE_MINIMUM);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_BASE_NUMBER);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_BASE_SORT_DIRECTION);
            sreq.params.remove(
                PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE + "."
                    + subKey + "." + SUBNAME_MTAS_FACET_BASE_SORT_TYPE);
            Set<String> subSubKeys = MtasSolrResultUtil
                .getIdsFromParameters(rb.req.getParams(),
                    PARAM_MTAS_FACET + "." + key + "." + NAME_MTAS_FACET_BASE
                        + "." + subKey + "."
                        + SUBNAME_MTAS_FACET_BASE_FUNCTION);
            for (String subSubKey : subSubKeys) {
              sreq.params.remove(PARAM_MTAS_FACET + "." + key + "."
                  + NAME_MTAS_FACET_BASE + "." + subKey + "."
                  + SUBNAME_MTAS_FACET_BASE_FUNCTION + "." + subSubKey + "."
                  + SUBNAME_MTAS_FACET_BASE_FUNCTION_EXPRESSION);
              sreq.params.remove(PARAM_MTAS_FACET + "." + key + "."
                  + NAME_MTAS_FACET_BASE + "." + subKey + "."
                  + SUBNAME_MTAS_FACET_BASE_FUNCTION + "." + subSubKey + "."
                  + SUBNAME_MTAS_FACET_BASE_FUNCTION_KEY);
              sreq.params.remove(PARAM_MTAS_FACET + "." + key + "."
                  + NAME_MTAS_FACET_BASE + "." + subKey + "."
                  + SUBNAME_MTAS_FACET_BASE_FUNCTION + "." + subSubKey + "."
                  + SUBNAME_MTAS_FACET_BASE_FUNCTION_TYPE);
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
   * mtas.solr.handler.component.util.MtasSolrComponent#create(mtas.codec.util.
   * CodecComponent.BasicComponent, java.lang.Boolean)
   */
  public SimpleOrderedMap<Object> create(ComponentFacet facet, Boolean encode)
      throws IOException {
    SimpleOrderedMap<Object> mtasFacetResponse = new SimpleOrderedMap<>();
    mtasFacetResponse.add("key", facet.key);
    HashMap<MtasDataCollector<?, ?>, HashMap<String, MtasSolrMtasResult>> functionData = new HashMap<>();
    for (int i = 0; i < facet.baseFields.length; i++) {
      if (facet.baseFunctionList[i] != null) {
        for (MtasDataCollector<?, ?> functionDataCollector : facet.baseFunctionList[i]
            .keySet()) {
          SubComponentFunction[] tmpSubComponentFunctionList = facet.baseFunctionList[i]
              .get(functionDataCollector);
          if (tmpSubComponentFunctionList != null) {
            HashMap<String, MtasSolrMtasResult> tmpList = new HashMap<>();
            for (SubComponentFunction tmpSubComponentFunction : tmpSubComponentFunctionList) {
              tmpList.put(tmpSubComponentFunction.key,
                  new MtasSolrMtasResult(tmpSubComponentFunction.dataCollector,
                      tmpSubComponentFunction.dataType,
                      tmpSubComponentFunction.statsType,
                      tmpSubComponentFunction.statsItems, null, null));
            }
            functionData.put(functionDataCollector, tmpList);
          }
        }
      }
    }
    MtasSolrMtasResult data = new MtasSolrMtasResult(facet.dataCollector,
        facet.baseDataTypes, facet.baseStatsTypes, facet.baseStatsItems, null,
        facet.baseSortTypes, facet.baseSortDirections, null, facet.baseNumbers,
        functionData);

    if (encode) {
      mtasFacetResponse.add("_encoded_list", MtasSolrResultUtil.encode(data));
    } else {
      mtasFacetResponse.add("list", data);
      MtasSolrResultUtil.rewrite(mtasFacetResponse, searchComponent);
    }
    return mtasFacetResponse;
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
            && sreq.params.getBool(PARAM_MTAS_FACET, false)) {
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
              // shouldn't happen
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
      ArrayList<Object> mtasResponseFacet;
      try {
        mtasResponseFacet = (ArrayList<Object>) mtasResponse.get(NAME);
        if (mtasResponseFacet != null) {
          MtasSolrResultUtil.rewrite(mtasResponseFacet, searchComponent);
        }
      } catch (ClassCastException e) {
        log.debug(e);
        mtasResponse.remove(NAME);
      }
    }
  }

  /**
   * Gets the field type.
   *
   * @param schema the schema
   * @param field the field
   * @return the field type
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private String getFieldType(IndexSchema schema, String field)
      throws IOException {
    SchemaField sf = schema.getField(field);
    FieldType ft = sf.getType();
    if (ft != null) {
      if (ft.isPointField() && !sf.hasDocValues()) {
        return ComponentFacet.TYPE_POINTFIELD_WITHOUT_DOCVALUES;
      }
      NumberType nt = ft.getNumberType();
      if (nt != null) {
        return nt.name();
      } else {
        return ComponentFacet.TYPE_STRING;
      }
    } else {
      // best guess
      return ComponentFacet.TYPE_STRING;
    }
  }

  /**
   * Gets the positive integer.
   *
   * @param number the number
   * @return the positive integer
   */
  private int getPositiveInteger(String number) {
    try {
      return Math.max(0, Integer.parseInt(number));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Gets the double.
   *
   * @param number the number
   * @return the double
   */
  private Double getDouble(String number) {
    try {
      return Double.parseDouble(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }

}
