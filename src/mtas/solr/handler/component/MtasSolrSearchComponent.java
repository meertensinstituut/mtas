package mtas.solr.handler.component;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mtas.analysis.token.MtasToken;
import mtas.codec.MtasCodecPostingsFormat;
import mtas.codec.util.DataCollector;
import mtas.codec.util.DataCollector.MtasDataCollector;
import mtas.codec.util.DataCollector.MtasDataItem;
import mtas.codec.util.CodecComponent.ComponentFacet;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentGroup;
import mtas.codec.util.CodecComponent.ComponentKwic;
import mtas.codec.util.CodecComponent.ComponentList;
import mtas.codec.util.CodecComponent.ComponentPosition;
import mtas.codec.util.CodecComponent.ComponentPrefix;
import mtas.codec.util.CodecComponent.ComponentSpan;
import mtas.codec.util.CodecComponent.ComponentTermVector;
import mtas.codec.util.CodecComponent.ComponentToken;
import mtas.codec.util.CodecComponent.KwicHit;
import mtas.codec.util.CodecComponent.KwicToken;
import mtas.codec.util.CodecComponent.ListHit;
import mtas.codec.util.CodecComponent.ListToken;
import mtas.codec.util.CodecUtil;
import mtas.parser.cql.MtasCQLParser;
import mtas.parser.function.ParseException;

import org.apache.lucene.document.FieldType.LegacyNumericType;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * The Class MtasSolrSearchComponent.
 */
public class MtasSolrSearchComponent extends SearchComponent {

  /** The Constant QUERY_TYPE_CQL. */
  public static final String QUERY_TYPE_CQL = "cql";

  /** The Constant PARAM_MTAS. */
  public static final String PARAM_MTAS = "mtas";

  /** The Constant PARAM_MTAS_FACET. */
  public static final String PARAM_MTAS_FACET = PARAM_MTAS + ".facet";

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

  /** The Constant PARAM_MTAS_KWIC. */
  public static final String PARAM_MTAS_KWIC = PARAM_MTAS + ".kwic";

  /** The Constant NAME_MTAS_KWIC_FIELD. */
  public static final String NAME_MTAS_KWIC_FIELD = "field";

  /** The Constant NAME_MTAS_KWIC_QUERY_TYPE. */
  public static final String NAME_MTAS_KWIC_QUERY_TYPE = "query.type";

  /** The Constant NAME_MTAS_KWIC_QUERY_VALUE. */
  public static final String NAME_MTAS_KWIC_QUERY_VALUE = "query.value";

  /** The Constant NAME_MTAS_KWIC_KEY. */
  public static final String NAME_MTAS_KWIC_KEY = "key";

  /** The Constant NAME_MTAS_KWIC_PREFIX. */
  public static final String NAME_MTAS_KWIC_PREFIX = "prefix";

  /** The Constant NAME_MTAS_KWIC_NUMBER. */
  public static final String NAME_MTAS_KWIC_NUMBER = "number";

  /** The Constant NAME_MTAS_KWIC_START. */
  public static final String NAME_MTAS_KWIC_START = "start";

  /** The Constant NAME_MTAS_KWIC_LEFT. */
  public static final String NAME_MTAS_KWIC_LEFT = "left";

  /** The Constant NAME_MTAS_KWIC_RIGHT. */
  public static final String NAME_MTAS_KWIC_RIGHT = "right";

  /** The Constant NAME_MTAS_KWIC_OUTPUT. */
  public static final String NAME_MTAS_KWIC_OUTPUT = "output";

  /** The Constant PARAM_MTAS_LIST. */
  public static final String PARAM_MTAS_LIST = PARAM_MTAS + ".list";

  /** The Constant NAME_MTAS_LIST_FIELD. */
  public static final String NAME_MTAS_LIST_FIELD = "field";

  /** The Constant NAME_MTAS_LIST_QUERY_TYPE. */
  public static final String NAME_MTAS_LIST_QUERY_TYPE = "query.type";

  /** The Constant NAME_MTAS_LIST_QUERY_VALUE. */
  public static final String NAME_MTAS_LIST_QUERY_VALUE = "query.value";

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

  /** The Constant PARAM_MTAS_GROUP. */
  public static final String PARAM_MTAS_GROUP = PARAM_MTAS + ".group";

  /** The Constant NAME_MTAS_GROUP_FIELD. */
  public static final String NAME_MTAS_GROUP_FIELD = "field";

  /** The Constant NAME_MTAS_GROUP_QUERY_TYPE. */
  public static final String NAME_MTAS_GROUP_QUERY_TYPE = "query.type";

  /** The Constant NAME_MTAS_GROUP_QUERY_VALUE. */
  public static final String NAME_MTAS_GROUP_QUERY_VALUE = "query.value";

  /** The Constant NAME_MTAS_GROUP_KEY. */
  public static final String NAME_MTAS_GROUP_KEY = "key";

  /** The Constant NAME_MTAS_GROUP_GROUPING_LEFT. */
  public static final String NAME_MTAS_GROUP_GROUPING_LEFT = "grouping.left";

  /** The Constant NAME_MTAS_GROUP_GROUPING_RIGHT. */
  public static final String NAME_MTAS_GROUP_GROUPING_RIGHT = "grouping.right";

  /** The Constant NAME_MTAS_GROUP_GROUPING_HIT_INSIDE. */
  public static final String NAME_MTAS_GROUP_GROUPING_HIT_INSIDE = "grouping.hit.inside";

  /** The Constant NAME_MTAS_GROUP_GROUPING_HIT_LEFT. */
  public static final String NAME_MTAS_GROUP_GROUPING_HIT_LEFT = "grouping.hit.left";

  /** The Constant NAME_MTAS_GROUP_GROUPING_HIT_RIGHT. */
  public static final String NAME_MTAS_GROUP_GROUPING_HIT_RIGHT = "grouping.hit.right";

  /** The Constant NAME_MTAS_GROUP_GROUPING_HIT_INSIDE_LEFT. */
  public static final String NAME_MTAS_GROUP_GROUPING_HIT_INSIDE_LEFT = "grouping.hit.insideLeft";

  /** The Constant NAME_MTAS_GROUP_GROUPING_HIT_INSIDE_RIGHT. */
  public static final String NAME_MTAS_GROUP_GROUPING_HIT_INSIDE_RIGHT = "grouping.hit.insideRight";

  /** The Constant NAME_MTAS_GROUP_GROUPING_POSITION. */
  public static final String NAME_MTAS_GROUP_GROUPING_POSITION = "position";

  /** The Constant NAME_MTAS_GROUP_GROUPING_PREFIXES. */
  public static final String NAME_MTAS_GROUP_GROUPING_PREFIXES = "prefixes";

  /** The Constant PARAM_MTAS_TERMVECTOR. */
  public static final String PARAM_MTAS_TERMVECTOR = PARAM_MTAS + ".termvector";

  /** The Constant NAME_MTAS_TERMVECTOR_FIELD. */
  public static final String NAME_MTAS_TERMVECTOR_FIELD = "field";

  /** The Constant NAME_MTAS_TERMVECTOR_KEY. */
  public static final String NAME_MTAS_TERMVECTOR_KEY = "key";

  /** The Constant NAME_MTAS_TERMVECTOR_PREFIX. */
  public static final String NAME_MTAS_TERMVECTOR_PREFIX = "prefix";

  /** The Constant NAME_MTAS_TERMVECTOR_REGEXP. */
  public static final String NAME_MTAS_TERMVECTOR_REGEXP = "regexp";

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

  /** The Constant NAME_MTAS_TERMVECTOR_MINIMUM. */
  public static final String NAME_MTAS_TERMVECTOR_MINIMUM = "minimum";

  /** The Constant NAME_MTAS_TERMVECTOR_MAXIMUM. */
  public static final String NAME_MTAS_TERMVECTOR_MAXIMUM = "maximum";

  /** The Constant NAME_MTAS_TERMVECTOR_FUNCTION. */
  public static final String NAME_MTAS_TERMVECTOR_FUNCTION = "function";

  /** The Constant PARAM_MTAS_PREFIX. */
  public static final String PARAM_MTAS_PREFIX = PARAM_MTAS + ".prefix";

  /** The Constant NAME_MTAS_PREFIX_FIELD. */
  public static final String NAME_MTAS_PREFIX_FIELD = "field";

  /** The Constant NAME_MTAS_PREFIX_KEY. */
  public static final String NAME_MTAS_PREFIX_KEY = "key";

  /** The Constant PARAM_MTAS_STATS. */
  public static final String PARAM_MTAS_STATS = PARAM_MTAS + ".stats";

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

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_TYPE. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_TYPE = "type";

  /** The Constant SUBNAME_MTAS_STATS_SPANS_QUERY_VALUE. */
  public static final String SUBNAME_MTAS_STATS_SPANS_QUERY_VALUE = "value";

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.component.SearchComponent#getVersion()
   */
  @Override
  public String getVersion() {
    return String.valueOf(MtasCodecPostingsFormat.VERSION_CURRENT);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.component.SearchComponent#getDescription()
   */
  @Override
  public String getDescription() {
    return "Mtas";
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#prepare(org.apache.solr.
   * handler.component.ResponseBuilder)
   */
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    // System.out.println(System.nanoTime()+" - "+Thread.currentThread().getId()
    // + " - "
    // + rb.req.getParams().getBool("isShard", false) + " PREPARE " + rb.stage
    // + " " + rb.req.getParamString());
    if (rb.req.getParams().getBool(PARAM_MTAS, false)) {
      ComponentFields mtasFields = new ComponentFields();
      // get settings kwic
      if (rb.req.getParams().getBool(PARAM_MTAS_KWIC, false)) {
        prepareKwic(rb, mtasFields);
      }
      // get settings list
      if (rb.req.getParams().getBool(PARAM_MTAS_LIST, false)) {
        prepareList(rb, mtasFields);
      }
      // get settings group
      if (rb.req.getParams().getBool(PARAM_MTAS_GROUP, false)) {
        prepareGroup(rb, mtasFields);
      }
      // get settings termvector
      if (rb.req.getParams().getBool(PARAM_MTAS_TERMVECTOR, false)) {
        prepareTermVector(rb, mtasFields);
      }
      // get settings prefix
      if (rb.req.getParams().getBool(PARAM_MTAS_PREFIX, false)) {
        preparePrefix(rb, mtasFields);
      }
      // get settings stats
      if (rb.req.getParams().getBool(PARAM_MTAS_STATS, false)) {
        prepareStats(rb, mtasFields);
      }
      // get settings facet
      if (rb.req.getParams().getBool(PARAM_MTAS_FACET, false)) {
        prepareFacet(rb, mtasFields);
      }
      rb.req.getContext().put(ComponentFields.class, mtasFields);
    }
  }

  /**
   * Gets the field type.
   *
   * @param schema
   *          the schema
   * @param field
   *          the field
   * @return the field type
   */
  private String getFieldType(IndexSchema schema, String field) {
    SchemaField sf = schema.getField(field);
    FieldType ft = sf.getType();
    if (ft != null) {
      if (ft.getNumericType() != null) {
        LegacyNumericType nt = ft.getNumericType();
        if (nt.equals(LegacyNumericType.INT)) {
          return ComponentFacet.TYPE_INTEGER;
        } else if (nt.equals(LegacyNumericType.DOUBLE)) {
          return ComponentFacet.TYPE_DOUBLE;
        } else if (nt.equals(LegacyNumericType.LONG)) {
          return ComponentFacet.TYPE_LONG;
        } else if (nt.equals(LegacyNumericType.FLOAT)) {
          return ComponentFacet.TYPE_FLOAT;
        }
      }
    }
    return ComponentFacet.TYPE_STRING;
  }

  /**
   * Prepare kwic.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void prepareKwic(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_KWIC);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] queryTypes = new String[ids.size()];
      String[] queryValues = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] prefixes = new String[ids.size()];
      String[] numbers = new String[ids.size()];
      String[] starts = new String[ids.size()];
      String[] lefts = new String[ids.size()];
      String[] rights = new String[ids.size()];
      String[] outputs = new String[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_FIELD, null);
        queryTypes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_QUERY_TYPE, null);
        queryValues[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_QUERY_VALUE,
            null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_KEY,
                String.valueOf(tmpCounter))
            .trim();
        prefixes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_PREFIX, null);
        numbers[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_NUMBER, null);
        starts[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_START, null);
        lefts[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_LEFT, null);
        rights[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_RIGHT, null);
        starts[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_START, null);
        outputs[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_OUTPUT, null);
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doKwic = true;
      rb.setNeedDocList(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas kwic");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      compareAndCheck(keys, fields, NAME_MTAS_KWIC_KEY, NAME_MTAS_KWIC_FIELD,
          true);
      compareAndCheck(queryValues, fields, NAME_MTAS_KWIC_QUERY_VALUE,
          NAME_MTAS_KWIC_FIELD, false);
      compareAndCheck(queryTypes, fields, NAME_MTAS_KWIC_QUERY_TYPE,
          NAME_MTAS_KWIC_FIELD, false);
      compareAndCheck(prefixes, fields, NAME_MTAS_KWIC_PREFIX,
          NAME_MTAS_KWIC_FIELD, false);
      compareAndCheck(numbers, fields, NAME_MTAS_KWIC_NUMBER,
          NAME_MTAS_KWIC_FIELD, false);
      compareAndCheck(starts, fields, NAME_MTAS_KWIC_START,
          NAME_MTAS_KWIC_FIELD, false);
      compareAndCheck(lefts, fields, NAME_MTAS_KWIC_LEFT, NAME_MTAS_KWIC_FIELD,
          false);
      compareAndCheck(rights, fields, NAME_MTAS_KWIC_RIGHT,
          NAME_MTAS_KWIC_FIELD, false);
      compareAndCheck(outputs, fields, NAME_MTAS_KWIC_OUTPUT,
          NAME_MTAS_KWIC_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        ComponentField cf = mtasFields.list.get(fields[i]);
        SpanQuery q = constructQuery(queryValues[i], queryTypes[i], fields[i]);
        // minimize number of queries
        if (cf.spanQueryList.contains(q)) {
          q = cf.spanQueryList.get(cf.spanQueryList.indexOf(q));
        } else {
          cf.spanQueryList.add(q);
        }
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + fields[i] + ":" + queryValues[i]
            : keys[i].trim();
        String prefix = prefixes[i];
        Integer number = (numbers[i] != null) ? getPositiveInteger(numbers[i])
            : null;
        int start = getPositiveInteger(starts[i]);
        int left = getPositiveInteger(lefts[i]);
        int right = getPositiveInteger(rights[i]);
        String output = outputs[i];
        mtasFields.list.get(fields[i]).kwicList.add(new ComponentKwic(q, key,
            prefix, number, start, left, right, output));
      }
    }
  }

  /**
   * Prepare facet.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void prepareFacet(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = getIdsFromParameters(rb.req.getParams(),
        PARAM_MTAS_FACET);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String tmpValue;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[][] queryTypes = new String[ids.size()][];
      String[][] queryValues = new String[ids.size()][];
      String[][] baseFields = new String[ids.size()][];
      String[][] baseFieldTypes = new String[ids.size()][];
      String[][] baseTypes = new String[ids.size()][];
      String[][] baseSortTypes = new String[ids.size()][];
      String[][] baseSortDirections = new String[ids.size()][];
      Integer[][] baseNumbers = new Integer[ids.size()][];
      Double[][] baseMinima = new Double[ids.size()][];
      Double[][] baseMaxima = new Double[ids.size()][];
      String[][] baseFunctions = new String[ids.size()][];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_FIELD, null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_KEY,
                String.valueOf(tmpCounter))
            .trim();
        Set<String> qIds = getIdsFromParameters(rb.req.getParams(),
            PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_QUERY);
        if (qIds.size() > 0) {
          int tmpQCounter = 0;
          queryTypes[tmpCounter] = new String[qIds.size()];
          queryValues[tmpCounter] = new String[qIds.size()];
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
            tmpQCounter++;
          }
        } else {
          throw new IOException(
              "no " + NAME_MTAS_FACET_QUERY + " for mtas facet " + id);
        }
        Set<String> bIds = getIdsFromParameters(rb.req.getParams(),
            PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE);
        if (bIds.size() > 0) {
          int tmpBCounter = 0;
          baseFields[tmpCounter] = new String[bIds.size()];
          baseFieldTypes[tmpCounter] = new String[bIds.size()];
          baseTypes[tmpCounter] = new String[bIds.size()];
          baseSortTypes[tmpCounter] = new String[bIds.size()];
          baseSortDirections[tmpCounter] = new String[bIds.size()];
          baseNumbers[tmpCounter] = new Integer[bIds.size()];
          baseMinima[tmpCounter] = new Double[bIds.size()];
          baseMaxima[tmpCounter] = new Double[bIds.size()];
          baseFunctions[tmpCounter] = new String[bIds.size()];
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
            baseFunctions[tmpCounter][tmpBCounter] = rb.req.getParams()
                .get(
                    PARAM_MTAS_FACET + "." + id + "." + NAME_MTAS_FACET_BASE
                        + "." + bId + "." + SUBNAME_MTAS_FACET_BASE_FUNCTION,
                    null);
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
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      compareAndCheck(keys, fields, NAME_MTAS_FACET_KEY, NAME_MTAS_FACET_FIELD,
          true);
      for (int i = 0; i < fields.length; i++) {
        ComponentField cf = mtasFields.list.get(fields[i]);
        int queryNumber = queryValues[i].length;
        SpanQuery ql[] = new SpanQuery[queryNumber];
        for (int j = 0; j < queryNumber; j++) {
          SpanQuery q = constructQuery(queryValues[i][j], queryTypes[i][j],
              fields[i]);
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
              baseSortTypes[i], baseSortDirections[i], baseNumbers[i],
              baseMinima[i], baseMaxima[i], baseFunctions[i]));
        } catch (ParseException e) {
          throw new IOException(e.getMessage());
        }
      }
    }
  }

  /**
   * Prepare list.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void prepareList(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_LIST);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] queryTypes = new String[ids.size()];
      String[] queryValues = new String[ids.size()];
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
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      compareAndCheck(keys, fields, NAME_MTAS_LIST_KEY, NAME_MTAS_LIST_FIELD,
          true);
      compareAndCheck(prefixes, queryValues, NAME_MTAS_LIST_QUERY_VALUE,
          NAME_MTAS_LIST_FIELD, false);
      compareAndCheck(prefixes, queryTypes, NAME_MTAS_LIST_QUERY_TYPE,
          NAME_MTAS_LIST_FIELD, false);
      compareAndCheck(prefixes, fields, NAME_MTAS_LIST_PREFIX,
          NAME_MTAS_LIST_FIELD, false);
      compareAndCheck(starts, fields, NAME_MTAS_LIST_START,
          NAME_MTAS_LIST_FIELD, false);
      compareAndCheck(numbers, fields, NAME_MTAS_LIST_NUMBER,
          NAME_MTAS_LIST_FIELD, false);
      compareAndCheck(lefts, fields, NAME_MTAS_LIST_LEFT, NAME_MTAS_LIST_FIELD,
          false);
      compareAndCheck(rights, fields, NAME_MTAS_LIST_RIGHT,
          NAME_MTAS_LIST_FIELD, false);
      compareAndCheck(outputs, fields, NAME_MTAS_LIST_OUTPUT,
          NAME_MTAS_LIST_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        ComponentField cf = mtasFields.list.get(fields[i]);
        SpanQuery q = constructQuery(queryValues[i], queryTypes[i], fields[i]);
        // minimize number of queries
        if (cf.spanQueryList.contains(q)) {
          q = cf.spanQueryList.get(cf.spanQueryList.indexOf(q));
        } else {
          cf.spanQueryList.add(q);
        }
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + fields[i] + ":" + queryValues[i]
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
        mtasFields.list.get(fields[i]).listList
            .add(new ComponentList(q, fields[i], queryValues[i], queryTypes[i],
                key, prefix, start, number, left, right, output));
      }
    }
  }

  /**
   * Prepare group.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void prepareGroup(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = getIdsFromParameters(rb.req.getParams(),
        PARAM_MTAS_GROUP);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] queryTypes = new String[ids.size()];
      String[] queryValues = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[][] groupingLeftPosition = new String[ids.size()][];
      String[][] groupingLeftPrefixes = new String[ids.size()][];
      String[][] groupingRightPosition = new String[ids.size()][];
      String[][] groupingRightPrefixes = new String[ids.size()][];
      String[] groupingHitInsidePrefixes = new String[ids.size()];
      String[][] groupingHitLeftPosition = new String[ids.size()][];
      String[][] groupingHitLeftPrefixes = new String[ids.size()][];
      String[][] groupingHitRightPosition = new String[ids.size()][];
      String[][] groupingHitRightPrefixes = new String[ids.size()][];
      String[][] groupingHitInsideLeftPosition = new String[ids.size()][];
      String[][] groupingHitInsideLeftPrefixes = new String[ids.size()][];
      String[][] groupingHitInsideRightPosition = new String[ids.size()][];
      String[][] groupingHitInsideRightPrefixes = new String[ids.size()][];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_GROUP + "." + id + "." + NAME_MTAS_GROUP_FIELD, null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_GROUP + "." + id + "." + NAME_MTAS_GROUP_KEY,
                String.valueOf(tmpCounter))
            .trim();
        queryTypes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_GROUP + "." + id + "." + NAME_MTAS_GROUP_QUERY_TYPE,
            null);
        queryValues[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_GROUP + "." + id + "." + NAME_MTAS_GROUP_QUERY_VALUE,
            null);
        groupingHitInsidePrefixes[tmpCounter] = null;
        // collect
        SortedSet<String> gids;
        String tmpName;
        // collect grouping inside
        tmpName = PARAM_MTAS_GROUP + "." + id + "."
            + NAME_MTAS_GROUP_GROUPING_HIT_INSIDE;
        groupingHitInsidePrefixes[tmpCounter] = rb.req.getParams()
            .get(tmpName + "." + NAME_MTAS_GROUP_GROUPING_PREFIXES);
        // collect grouping left
        tmpName = PARAM_MTAS_GROUP + "." + id + "."
            + NAME_MTAS_GROUP_GROUPING_LEFT;
        gids = getIdsFromParameters(rb.req.getParams(), tmpName);
        groupingLeftPosition[tmpCounter] = new String[gids.size()];
        groupingLeftPrefixes[tmpCounter] = new String[gids.size()];
        prepareGroup(rb.req.getParams(), gids, tmpName,
            groupingLeftPosition[tmpCounter], groupingLeftPrefixes[tmpCounter]);
        // collect grouping right
        tmpName = PARAM_MTAS_GROUP + "." + id + "."
            + NAME_MTAS_GROUP_GROUPING_RIGHT;
        gids = getIdsFromParameters(rb.req.getParams(), tmpName);
        groupingRightPosition[tmpCounter] = new String[gids.size()];
        groupingRightPrefixes[tmpCounter] = new String[gids.size()];
        prepareGroup(rb.req.getParams(), gids, tmpName,
            groupingRightPosition[tmpCounter],
            groupingRightPrefixes[tmpCounter]);
        // collect grouping hit left
        tmpName = PARAM_MTAS_GROUP + "." + id + "."
            + NAME_MTAS_GROUP_GROUPING_HIT_LEFT;
        gids = getIdsFromParameters(rb.req.getParams(), tmpName);
        groupingHitLeftPosition[tmpCounter] = new String[gids.size()];
        groupingHitLeftPrefixes[tmpCounter] = new String[gids.size()];
        prepareGroup(rb.req.getParams(), gids, tmpName,
            groupingHitLeftPosition[tmpCounter],
            groupingHitLeftPrefixes[tmpCounter]);
        // collect grouping hit right
        tmpName = PARAM_MTAS_GROUP + "." + id + "."
            + NAME_MTAS_GROUP_GROUPING_HIT_RIGHT;
        gids = getIdsFromParameters(rb.req.getParams(), tmpName);
        groupingHitRightPosition[tmpCounter] = new String[gids.size()];
        groupingHitRightPrefixes[tmpCounter] = new String[gids.size()];
        prepareGroup(rb.req.getParams(), gids, tmpName,
            groupingHitRightPosition[tmpCounter],
            groupingHitRightPrefixes[tmpCounter]);
        // collect grouping hit inside left
        tmpName = PARAM_MTAS_GROUP + "." + id + "."
            + NAME_MTAS_GROUP_GROUPING_HIT_INSIDE_LEFT;
        gids = getIdsFromParameters(rb.req.getParams(), tmpName);
        groupingHitInsideLeftPosition[tmpCounter] = new String[gids.size()];
        groupingHitInsideLeftPrefixes[tmpCounter] = new String[gids.size()];
        prepareGroup(rb.req.getParams(), gids, tmpName,
            groupingHitInsideLeftPosition[tmpCounter],
            groupingHitInsideLeftPrefixes[tmpCounter]);
        // collect grouping hit inside right
        tmpName = PARAM_MTAS_GROUP + "." + id + "."
            + NAME_MTAS_GROUP_GROUPING_HIT_INSIDE_RIGHT;
        gids = getIdsFromParameters(rb.req.getParams(), tmpName);
        groupingHitInsideRightPosition[tmpCounter] = new String[gids.size()];
        groupingHitInsideRightPrefixes[tmpCounter] = new String[gids.size()];
        prepareGroup(rb.req.getParams(), gids, tmpName,
            groupingHitInsideRightPosition[tmpCounter],
            groupingHitInsideRightPrefixes[tmpCounter]);

        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doGroup = true;
      rb.setNeedDocSet(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas group");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      compareAndCheck(keys, fields, NAME_MTAS_GROUP_KEY, NAME_MTAS_GROUP_FIELD,
          true);
      compareAndCheck(queryValues, fields, NAME_MTAS_GROUP_QUERY_VALUE,
          NAME_MTAS_GROUP_FIELD, false);
      compareAndCheck(queryTypes, fields, NAME_MTAS_GROUP_QUERY_TYPE,
          NAME_MTAS_GROUP_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        ComponentField cf = mtasFields.list.get(fields[i]);
        SpanQuery q = constructQuery(queryValues[i], queryTypes[i], fields[i]);
        // minimize number of queries
        if (cf.spanQueryList.contains(q)) {
          q = cf.spanQueryList.get(cf.spanQueryList.indexOf(q));
        } else {
          cf.spanQueryList.add(q);
        }
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + fields[i] + ":" + queryValues[i]
            : keys[i].trim();
        mtasFields.list.get(fields[i]).groupList.add(new ComponentGroup(q,
            fields[i], queryValues[i], queryTypes[i], key,
            groupingHitInsidePrefixes[i], groupingHitInsideLeftPosition[i],
            groupingHitInsideLeftPrefixes[i], groupingHitInsideRightPosition[i],
            groupingHitInsideRightPrefixes[i], groupingHitLeftPosition[i],
            groupingHitLeftPrefixes[i], groupingHitRightPosition[i],
            groupingHitRightPrefixes[i], groupingLeftPosition[i],
            groupingLeftPrefixes[i], groupingRightPosition[i],
            groupingRightPrefixes[i]));
      }
    }
  }

  /**
   * Prepare group.
   *
   * @param solrParams
   *          the solr params
   * @param gids
   *          the gids
   * @param name
   *          the name
   * @param positions
   *          the positions
   * @param prefixes
   *          the prefixes
   */
  private void prepareGroup(SolrParams solrParams, SortedSet<String> gids,
      String name, String[] positions, String[] prefixes) {
    SortedSet<String> sgids;
    if (gids.size() > 0) {
      int tmpSubCounter = 0;
      for (String gid : gids) {
        positions[tmpSubCounter] = solrParams.get(
            name + "." + gid + "." + NAME_MTAS_GROUP_GROUPING_POSITION, null);
        prefixes[tmpSubCounter] = solrParams.get(
            name + "." + gid + "." + NAME_MTAS_GROUP_GROUPING_PREFIXES, null);
        tmpSubCounter++;
      }
    }
  }

  /**
   * Prepare term vector.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void prepareTermVector(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = getIdsFromParameters(rb.req.getParams(),
        PARAM_MTAS_TERMVECTOR);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] prefixes = new String[ids.size()];
      String[] regexps = new String[ids.size()];
      String[] sortTypes = new String[ids.size()];
      String[] sortDirections = new String[ids.size()];
      String[] types = new String[ids.size()];
      String[] startValues = new String[ids.size()];
      String[] numbers = new String[ids.size()];
      String[] functions = new String[ids.size()];
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
        types[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_TERMVECTOR + "." + id + "." + NAME_MTAS_TERMVECTOR_TYPE,
            null);
        functions[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_TERMVECTOR
            + "." + id + "." + NAME_MTAS_TERMVECTOR_FUNCTION, null);
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
      compareAndCheck(keys, fields, NAME_MTAS_TERMVECTOR_KEY,
          NAME_MTAS_TERMVECTOR_FIELD, true);
      compareAndCheck(prefixes, fields, NAME_MTAS_TERMVECTOR_PREFIX,
          NAME_MTAS_TERMVECTOR_FIELD, false);
      compareAndCheck(regexps, fields, NAME_MTAS_TERMVECTOR_REGEXP,
          NAME_MTAS_TERMVECTOR_FIELD, false);
      compareAndCheck(types, fields, NAME_MTAS_TERMVECTOR_TYPE,
          NAME_MTAS_TERMVECTOR_FIELD, false);
      compareAndCheck(sortTypes, fields, NAME_MTAS_TERMVECTOR_SORT_TYPE,
          NAME_MTAS_TERMVECTOR_FIELD, false);
      compareAndCheck(sortDirections, fields,
          NAME_MTAS_TERMVECTOR_SORT_DIRECTION, NAME_MTAS_TERMVECTOR_FIELD,
          false);
      compareAndCheck(startValues, fields, NAME_MTAS_TERMVECTOR_START,
          NAME_MTAS_TERMVECTOR_FIELD, false);
      compareAndCheck(numbers, fields, NAME_MTAS_TERMVECTOR_NUMBER,
          NAME_MTAS_TERMVECTOR_FIELD, false);
      compareAndCheck(functions, fields, NAME_MTAS_TERMVECTOR_FUNCTION,
          NAME_MTAS_TERMVECTOR_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        String field = fields[i];
        String prefix = (prefixes[i] == null) || (prefixes[i].isEmpty()) ? null
            : prefixes[i].trim();
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + field + ":" + prefix : keys[i].trim();
        String regexp = (regexps[i] == null) || (regexps[i].isEmpty()) ? null
            : regexps[i].trim();
        String startValue = (startValues[i] == null)
            || (startValues[i].isEmpty()) ? null : startValues[i].trim();
        int number = (numbers[i] == null) || (numbers[i].isEmpty()) ? 0
            : Integer.parseInt(numbers[i]);
        String type = (types[i] == null) || (types[i].isEmpty()) ? null
            : types[i].trim();
        String sortType = (sortTypes[i] == null) || (sortTypes[i].isEmpty())
            ? CodecUtil.SORT_TERM : sortTypes[i].trim();
        String sortDirection = (sortDirections[i] == null)
            || (sortDirections[i].isEmpty()) ? null : sortDirections[i].trim();
        String function = functions[i];
        if (prefix == null || prefix.isEmpty()) {
          throw new IOException("no (valid) prefix in mtas termvector");
        } else {
          try {
            mtasFields.list.get(field).termVectorList
                .add(new ComponentTermVector(key, prefix, regexp, type,
                    sortType, sortDirection, startValue, number, function));
          } catch (ParseException e) {
            throw new IOException(e.getMessage());
          }
        }
      }

    }
  }

  /**
   * Prepare prefix.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void preparePrefix(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = getIdsFromParameters(rb.req.getParams(),
        PARAM_MTAS_PREFIX);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_PREFIX + "." + id + "." + NAME_MTAS_PREFIX_FIELD, null);
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_PREFIX + "." + id + "." + NAME_MTAS_PREFIX_KEY,
                String.valueOf(tmpCounter))
            .trim();
        String uniqueKeyField = rb.req.getSchema().getUniqueKeyField()
            .getName();
        mtasFields.doPrefix = true;
        // init and checks
        for (String field : fields) {
          if (field == null || field.isEmpty()) {
            throw new IOException("no (valid) field in mtas prefix");
          } else if (!mtasFields.list.containsKey(field)) {
            mtasFields.list.put(field,
                new ComponentField(field, uniqueKeyField));
          }
        }
        compareAndCheck(keys, fields, NAME_MTAS_PREFIX_KEY,
            NAME_MTAS_PREFIX_FIELD, true);
        for (int i = 0; i < fields.length; i++) {
          String field = fields[i];
          String key = ((keys == null) || (keys[i] == null)
              || (keys[i].isEmpty())) ? String.valueOf(i) + ":" + field
                  : keys[i].trim();
          mtasFields.list.get(field).prefix = new ComponentPrefix(key);
        }
      }
    }
  }

  /**
   * Prepare stats.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void prepareStats(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    if (rb.req.getParams().getBool(PARAM_MTAS_STATS_POSITIONS, false)) {
      prepareStatsPositions(rb, mtasFields);
    }
    if (rb.req.getParams().getBool(PARAM_MTAS_STATS_TOKENS, false)) {
      prepareStatsTokens(rb, mtasFields);
    }
    if (rb.req.getParams().getBool(PARAM_MTAS_STATS_SPANS, false)) {
      prepareStatsSpans(rb, mtasFields);
    }
  }

  /**
   * Prepare stats positions.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void prepareStatsPositions(ResponseBuilder rb,
      ComponentFields mtasFields) throws IOException {
    Set<String> ids = getIdsFromParameters(rb.req.getParams(),
        PARAM_MTAS_STATS_POSITIONS);
    if (ids.size() > 0) {
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
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      compareAndCheck(keys, fields, NAME_MTAS_STATS_POSITIONS_KEY,
          NAME_MTAS_STATS_POSITIONS_FIELD, true);
      compareAndCheck(minima, fields, NAME_MTAS_STATS_POSITIONS_MINIMUM,
          NAME_MTAS_STATS_POSITIONS_FIELD, false);
      compareAndCheck(maxima, fields, NAME_MTAS_STATS_POSITIONS_MAXIMUM,
          NAME_MTAS_STATS_POSITIONS_FIELD, false);
      compareAndCheck(types, fields, NAME_MTAS_STATS_POSITIONS_TYPE,
          NAME_MTAS_STATS_POSITIONS_FIELD, false);
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
              .add(new ComponentPosition(field, key, minimum, maximum, type));
        } catch (ParseException e) {
          throw new IOException(e.getMessage());
        }
      }
    }
  }

  /**
   * Prepare stats tokens.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void prepareStatsTokens(ResponseBuilder rb,
      ComponentFields mtasFields) throws IOException {
    Set<String> ids = getIdsFromParameters(rb.req.getParams(),
        PARAM_MTAS_STATS_TOKENS);
    if (ids.size() > 0) {
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
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      compareAndCheck(keys, fields, NAME_MTAS_STATS_TOKENS_KEY,
          NAME_MTAS_STATS_TOKENS_FIELD, true);
      compareAndCheck(minima, fields, NAME_MTAS_STATS_TOKENS_MINIMUM,
          NAME_MTAS_STATS_TOKENS_FIELD, false);
      compareAndCheck(maxima, fields, NAME_MTAS_STATS_TOKENS_MAXIMUM,
          NAME_MTAS_STATS_TOKENS_FIELD, false);
      compareAndCheck(types, fields, NAME_MTAS_STATS_TOKENS_TYPE,
          NAME_MTAS_STATS_TOKENS_FIELD, false);
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
              .add(new ComponentToken(field, key, minimum, maximum, type));
        } catch (ParseException e) {
          throw new IOException(e.getMessage());
        }
      }
    }
  }

  /**
   * Prepare stats spans.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void prepareStatsSpans(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    SortedSet<String> ids = getIdsFromParameters(rb.req.getParams(),
        PARAM_MTAS_STATS_SPANS);
    if (ids.size() > 0) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] keys = new String[ids.size()];
      String[] minima = new String[ids.size()];
      String[] maxima = new String[ids.size()];
      String[] types = new String[ids.size()];
      String[] functions = new String[ids.size()];
      String[][] queryTypes = new String[ids.size()][];
      String[][] queryValues = new String[ids.size()][];
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
        functions[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_STATS_SPANS
            + "." + id + "." + NAME_MTAS_STATS_SPANS_FUNCTION, null);

        Set<String> qIds = getIdsFromParameters(rb.req.getParams(),
            PARAM_MTAS_STATS_SPANS + "." + id + "."
                + NAME_MTAS_STATS_SPANS_QUERY);
        if (qIds.size() > 0) {
          int tmpQCounter = 0;
          queryTypes[tmpCounter] = new String[qIds.size()];
          queryValues[tmpCounter] = new String[qIds.size()];
          for (String qId : qIds) {
            queryTypes[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                    + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                    + SUBNAME_MTAS_STATS_SPANS_QUERY_TYPE, null);
            queryValues[tmpCounter][tmpQCounter] = rb.req.getParams()
                .get(PARAM_MTAS_STATS_SPANS + "." + id + "."
                    + NAME_MTAS_STATS_SPANS_QUERY + "." + qId + "."
                    + SUBNAME_MTAS_STATS_SPANS_QUERY_VALUE, null);
            tmpQCounter++;
          }
        } else {
          throw new IOException("no " + NAME_MTAS_STATS_SPANS_QUERY
              + " for mtas stats span " + id);
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
          mtasFields.list.put(field, new ComponentField(field, uniqueKeyField));
        }
      }
      compareAndCheck(keys, fields, NAME_MTAS_STATS_SPANS_KEY,
          NAME_MTAS_STATS_SPANS_FIELD, true);
      compareAndCheck(minima, fields, NAME_MTAS_STATS_SPANS_MINIMUM,
          NAME_MTAS_STATS_SPANS_FIELD, false);
      compareAndCheck(maxima, fields, NAME_MTAS_STATS_SPANS_MAXIMUM,
          NAME_MTAS_STATS_SPANS_FIELD, false);
      compareAndCheck(types, fields, NAME_MTAS_STATS_SPANS_TYPE,
          NAME_MTAS_STATS_SPANS_FIELD, false);
      compareAndCheck(types, fields, NAME_MTAS_STATS_SPANS_FUNCTION,
          NAME_MTAS_STATS_SPANS_FIELD, false);

      for (int i = 0; i < fields.length; i++) {
        ComponentField cf = mtasFields.list.get(fields[i]);
        int queryNumber = queryValues[i].length;
        SpanQuery ql[] = new SpanQuery[queryNumber];
        for (int j = 0; j < queryNumber; j++) {
          SpanQuery q = constructQuery(queryValues[i][j], queryTypes[i][j],
              fields[i]);
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
        String function = (functions[i] == null) ? null : functions[i];
        mtasFields.list.get(fields[i]).statsSpanList
            .add(new ComponentSpan(ql, key, minimum, maximum, type, function));
      }
    } else {
      throw new IOException("missing parameters stats spans");
    }
  }

  /**
   * Gets the ids from parameters.
   *
   * @param params
   *          the params
   * @param prefix
   *          the prefix
   * @return the ids from parameters
   */
  private SortedSet<String> getIdsFromParameters(SolrParams params,
      String prefix) {
    SortedSet<String> ids = new TreeSet<String>();
    Iterator<String> it = params.getParameterNamesIterator();
    Pattern pattern = Pattern
        .compile("^" + Pattern.quote(prefix) + "\\.([^\\.]+)(\\..*|$)");
    while (it.hasNext()) {
      String item = it.next();
      Matcher m = pattern.matcher(item);
      if (m.matches()) {
        ids.add(m.group(1));
      }
    }
    return ids;
  }

  /**
   * Construct query.
   *
   * @param queryValue
   *          the query value
   * @param queryType
   *          the query type
   * @param field
   *          the field
   * @return the span query
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private SpanQuery constructQuery(String queryValue, String queryType,
      String field) throws IOException {
    if (queryType == null || queryType.isEmpty()) {
      throw new IOException("no (valid) type for query " + queryValue);
    } else if (queryValue == null || queryValue.isEmpty()) {
      throw new IOException("no (valid) value for " + queryType + " query");
    }
    Reader reader = new BufferedReader(new StringReader(queryValue));
    if (queryType.equals(QUERY_TYPE_CQL)) {
      MtasCQLParser p = new MtasCQLParser(reader);
      try {
        return p.parse(field);
      } catch (mtas.parser.cql.ParseException e) {
        throw new IOException(
            "couldn't parse " + queryType + " query " + queryValue);
      }
    } else {
      throw new IOException(
          "unknown queryType " + queryType + " for query " + queryValue);
    }
  }

  /**
   * Gets the positive integer.
   *
   * @param number
   *          the number
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
   * @param number
   *          the number
   * @return the double
   */
  private Double getDouble(String number) {
    try {
      return Double.parseDouble(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Compare and check.
   *
   * @param list
   *          the list
   * @param original
   *          the original
   * @param nameNew
   *          the name new
   * @param nameOriginal
   *          the name original
   * @param unique
   *          the unique
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void compareAndCheck(String[] list, String[] original, String nameNew,
      String nameOriginal, Boolean unique) throws IOException {
    if (list != null) {
      if (list.length != original.length) {
        throw new IOException(
            "unequal size " + nameNew + " and " + nameOriginal);
      }
      if (unique) {
        Set<String> set = new HashSet<String>();
        for (int i = 0; i < list.length; i++) {
          set.add(list[i]);
        }
        if (set.size() < list.length) {
          throw new IOException("duplicate " + nameNew);
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#process(org.apache.solr.
   * handler.component.ResponseBuilder)
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException {
    // System.out.println(System.nanoTime() + " - "
    // + Thread.currentThread().getId() + " - "
    // + rb.req.getParams().getBool("isShard", false) + " PROCESS " + rb.stage
    // + " " + rb.req.getParamString());
    ComponentFields mtasFields = getMtasFields(rb);
    if (mtasFields != null) {
      DocSet docSet = rb.getResults().docSet;
      DocList docList = rb.getResults().docList;
      if (mtasFields.doStats || mtasFields.doKwic || mtasFields.doList
          || mtasFields.doGroup || mtasFields.doFacet || mtasFields.doTermVector
          || mtasFields.doPrefix) {
        SolrIndexSearcher searcher = rb.req.getSearcher();
        ArrayList<Integer> docSetList = null;
        ArrayList<Integer> docListList = null;
        if (docSet != null) {
          docSetList = new ArrayList<Integer>();
          Iterator<Integer> docSetIterator = docSet.iterator();
          while (docSetIterator.hasNext()) {
            docSetList.add(docSetIterator.next());
          }
          Collections.sort(docSetList);
        }
        if (docList != null) {
          docListList = new ArrayList<Integer>();
          Iterator<Integer> docListIterator = docList.iterator();
          while (docListIterator.hasNext()) {
            docListList.add(docListIterator.next());
          }
          Collections.sort(docListList);
        }
        for (String field : mtasFields.list.keySet()) {
          try {
            CodecUtil.collect(field, searcher, searcher.getRawReader(),
                docListList, docSetList, mtasFields.list.get(field));
          } catch (IllegalAccessException | IllegalArgumentException
              | InvocationTargetException e) {
            throw new IOException(e.getMessage());
          }
        }
        NamedList<Object> mtasResponse = new SimpleOrderedMap<>();
        if (mtasFields.doKwic) {
          ArrayList<NamedList> mtasKwicResponses = new ArrayList<NamedList>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentKwic kwic : mtasFields.list.get(field).kwicList) {
              mtasKwicResponses.add(createKwic(kwic));
            }
          }
          // add to response
          mtasResponse.add("kwic", mtasKwicResponses);
        }
        if (mtasFields.doFacet) {
          ArrayList<NamedList> mtasFacetResponses = new ArrayList<NamedList>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentFacet facet : mtasFields.list.get(field).facetList) {
              if (rb.req.getParams().getBool("isShard", false)) {
                mtasFacetResponses.add(createFacet(facet, true));
              } else {
                mtasFacetResponses.add(createFacet(facet, false));
              }
            }
          }
          // add to response
          mtasResponse.add("facet", mtasFacetResponses);
        }
        if (mtasFields.doList) {
          ArrayList<NamedList> mtasListResponses = new ArrayList<NamedList>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentList list : mtasFields.list.get(field).listList) {
              mtasListResponses.add(createList(list));
            }
          }
          // add to response
          mtasResponse.add("list", mtasListResponses);
        }
        if (mtasFields.doGroup) {
          ArrayList<NamedList> mtasGroupResponses = new ArrayList<NamedList>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentGroup group : mtasFields.list.get(field).groupList) {
              if (rb.req.getParams().getBool("isShard", false)) {
                mtasGroupResponses.add(createGroup(group, true));
              } else {
                mtasGroupResponses.add(createGroup(group, false));
              }
            }
          }
          // add to response
          mtasResponse.add("group", mtasGroupResponses);
        }
        if (mtasFields.doTermVector) {
          ArrayList<NamedList> mtasTermVectorResponses = new ArrayList<NamedList>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentTermVector termVector : mtasFields.list
                .get(field).termVectorList) {
              if (rb.req.getParams().getBool("isShard", false)) {
                mtasTermVectorResponses.add(createTermVector(termVector, true));
              } else {
                mtasTermVectorResponses
                    .add(createTermVector(termVector, false));
              }
            }
          }
          // add to response
          mtasResponse.add("termvector", mtasTermVectorResponses);
        }
        if (mtasFields.doPrefix) {
          ArrayList<NamedList> mtasPrefixResponses = new ArrayList<NamedList>();
          for (String field : mtasFields.list.keySet()) {
            if (mtasFields.list.get(field).prefix != null) {
              if (rb.req.getParams().getBool("isShard", false)) {
                mtasPrefixResponses
                    .add(createPrefix(mtasFields.list.get(field).prefix, true));
              } else {
                mtasPrefixResponses.add(
                    createPrefix(mtasFields.list.get(field).prefix, false));
              }
            }
          }
          mtasResponse.add("prefix", mtasPrefixResponses);
        }
        if (mtasFields.doStats) {
          NamedList<Object> mtasStatsResponse = new SimpleOrderedMap<>();
          if (mtasFields.doStatsPositions || mtasFields.doStatsTokens
              || mtasFields.doStatsSpans) {
            if (mtasFields.doStatsTokens) {
              ArrayList<Object> mtasStatsTokensResponses = new ArrayList<Object>();
              for (String field : mtasFields.list.keySet()) {
                for (ComponentToken token : mtasFields.list
                    .get(field).statsTokenList) {
                  if (rb.req.getParams().getBool("isShard", false)) {
                    mtasStatsTokensResponses.add(createStatsToken(token, true));
                  } else {
                    mtasStatsTokensResponses
                        .add(createStatsToken(token, false));
                  }
                }
              }
              mtasStatsResponse.add("tokens", mtasStatsTokensResponses);
            }
            if (mtasFields.doStatsPositions) {
              ArrayList<Object> mtasStatsPositionsResponses = new ArrayList<Object>();
              for (String field : mtasFields.list.keySet()) {
                for (ComponentPosition position : mtasFields.list
                    .get(field).statsPositionList) {
                  if (rb.req.getParams().getBool("isShard", false)) {
                    mtasStatsPositionsResponses
                        .add(createStatsPosition(position, true));
                  } else {
                    mtasStatsPositionsResponses
                        .add(createStatsPosition(position, false));
                  }
                }
              }
              mtasStatsResponse.add("positions", mtasStatsPositionsResponses);
            }
            if (mtasFields.doStatsSpans) {
              ArrayList<Object> mtasStatsSpansResponses = new ArrayList<Object>();
              for (String field : mtasFields.list.keySet()) {
                for (ComponentSpan span : mtasFields.list
                    .get(field).statsSpanList) {
                  if (rb.req.getParams().getBool("isShard", false)) {
                    mtasStatsSpansResponses.add(createStatsSpan(span, true));
                  } else {
                    mtasStatsSpansResponses.add(createStatsSpan(span, false));
                  }
                }
              }
              mtasStatsResponse.add("spans", mtasStatsSpansResponses);
            }
            // add to response
            mtasResponse.add("stats", mtasStatsResponse);
          }
        }
        // add to response
        rb.rsp.add("mtas", mtasResponse);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#modifyRequest(org.apache.
   * solr.handler.component.ResponseBuilder,
   * org.apache.solr.handler.component.SearchComponent,
   * org.apache.solr.handler.component.ShardRequest)
   */
  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq) {
    // System.out.println(Thread.currentThread().getId() + " - "
    // + rb.req.getParams().getBool("isShard", false) + " MODIFY REQUEST "
    // + rb.stage + " " + rb.req.getParamString());
    if (sreq.params.getBool(PARAM_MTAS, false)) {
      if (sreq.params.getBool(PARAM_MTAS_STATS, false)) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
          // do nothing
        } else {
          // remove stats for other requests
          sreq.params.remove(PARAM_MTAS_STATS);
          sreq.params.remove(PARAM_MTAS_STATS_POSITIONS);
          sreq.params.remove(PARAM_MTAS_STATS_SPANS);
        }
      }
      if (sreq.params.getBool(PARAM_MTAS_KWIC, false)) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
          // do nothing
        } else {
          Set<String> keys = getIdsFromParameters(rb.req.getParams(),
              PARAM_MTAS_LIST);
          sreq.params.remove(PARAM_MTAS_KWIC);
          for (String key : keys) {
            sreq.params.remove(
                PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_FIELD);
            sreq.params.remove(
                PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_QUERY_VALUE);
            sreq.params
                .remove(PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_KEY);
            sreq.params.remove(
                PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_PREFIX);
            sreq.params.remove(
                PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_NUMBER);
            sreq.params.remove(
                PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_LEFT);
            sreq.params.remove(
                PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_RIGHT);
            sreq.params.remove(
                PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_OUTPUT);
          }
        }
      }
      if (sreq.params.getBool(PARAM_MTAS_LIST, false)) {
        // compute keys
        Set<String> keys = getIdsFromParameters(rb.req.getParams(),
            PARAM_MTAS_LIST);
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
          for (String key : keys) {
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_PREFIX);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_START);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_NUMBER);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_LEFT);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_RIGHT);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_OUTPUT);
            // don't get data
            sreq.params.add(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_NUMBER, "0");
          }
        } else {
          sreq.params.remove(PARAM_MTAS_LIST);
          for (String key : keys) {
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_FIELD);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_QUERY_VALUE);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_QUERY_TYPE);
            sreq.params
                .remove(PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_KEY);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_PREFIX);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_START);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_NUMBER);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_LEFT);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_RIGHT);
            sreq.params.remove(
                PARAM_MTAS_LIST + "." + key + "." + NAME_MTAS_LIST_OUTPUT);
          }
        }        
      }  
      if (sreq.params.getBool(PARAM_MTAS_TERMVECTOR, false)) {
        // compute keys
        Set<String> keys = getIdsFromParameters(rb.req.getParams(),
            PARAM_MTAS_TERMVECTOR);
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
          for (String key : keys) {
            System.out.println("Normal request termvector "+key);
          }
        } else {
          for (String key : keys) {
            System.out.println("Special request termvector "+key);
          }
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.component.SearchComponent#handleResponses(org.
   * apache.solr.handler.component.ResponseBuilder,
   * org.apache.solr.handler.component.ShardRequest)
   */
  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    // System.out.println(System.nanoTime()+" - "+Thread.currentThread().getId()
    // + " - "
    // + rb.req.getParams().getBool("isShard", false) + " HANDLERESPONSES "
    // + rb.stage + " " + rb.req.getParamString());
  }

  /**
   * Merge responses tree set.
   *
   * @param originalList
   *          the original list
   * @param shardList
   *          the shard list
   */
  private void mergeResponsesTreeSet(TreeSet<Object> originalList,
      TreeSet<Object> shardList) {
    for (Object item : shardList) {
      originalList.add(item);
    }
  }

  /**
   * Merge responses array list.
   *
   * @param originalList
   *          the original list
   * @param shardList
   *          the shard list
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void mergeResponsesArrayList(ArrayList<Object> originalList,
      ArrayList<Object> shardList) throws IOException {
    // get keys from original
    HashMap<String, Object> originalKeyList = new HashMap<String, Object>();
    for (Object item : originalList) {
      if (item instanceof NamedList<?>) {
        NamedList<Object> itemList = (NamedList<Object>) item;
        Object key = itemList.get("key");
        if ((key != null) && (key instanceof String)) {
          originalKeyList.put((String) key, item);
        }
      }
    }
    for (Object item : shardList) {
      if (item instanceof NamedList<?>) {
        NamedList<Object> itemList = (NamedList<Object>) item;
        Object key = itemList.get("key");
        // item with key
        if ((key != null) && (key instanceof String)) {
          // merge
          if (originalKeyList.containsKey(key)) {
            Object originalItem = originalKeyList.get(key);
            if (originalItem.getClass().equals(item.getClass())) {
              mergeResponsesNamedList((NamedList<Object>) originalItem,
                  (NamedList<Object>) item);
            } else {
              // ignore?
            }
            // add
          } else {
            originalList.add(adjustablePartsCloned(item));
          }
        } else {
          originalList.add(item);
        }
      } else {
        originalList.add(item);
      }
    }
  }

  /**
   * Merge responses named list.
   *
   * @param mainResponse
   *          the main response
   * @param shardResponse
   *          the shard response
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void mergeResponsesNamedList(NamedList<Object> mainResponse,
      NamedList<Object> shardResponse) throws IOException {
    Iterator<Entry<String, Object>> it = shardResponse.iterator();
    while (it.hasNext()) {
      Entry<String, Object> entry = it.next();
      String name = entry.getKey();
      Object shardValue = entry.getValue();
      int originalId = mainResponse.indexOf(name, 0);
      if (originalId < 0) {
        mainResponse.add(name, adjustablePartsCloned(shardValue));
      } else {
        Object original = mainResponse.getVal(originalId);
        if (original == null) {
          original = adjustablePartsCloned(shardValue);
        } else if (original.getClass().equals(shardValue.getClass())) {
          // merge ArrayList
          if (original instanceof ArrayList) {
            ArrayList originalList = (ArrayList) original;
            ArrayList shardList = (ArrayList) shardValue;
            mergeResponsesArrayList(originalList, shardList);
            // merge Namedlist
          } else if (original instanceof NamedList<?>) {
            mergeResponsesNamedList((NamedList<Object>) original,
                (NamedList<Object>) shardValue);
            // merge TreeSet
          } else if (original instanceof TreeSet<?>) {
            mergeResponsesTreeSet((TreeSet<Object>) original,
                (TreeSet<Object>) shardValue);
          } else if (original instanceof ComponentSortSelect) {
            ComponentSortSelect originalComponentSortSelect = (ComponentSortSelect) original;
            originalComponentSortSelect.merge((ComponentSortSelect) shardValue);
          } else if (original instanceof String) {
            // ignore?
          } else if (original instanceof Integer) {
            original = (Integer) original + ((Integer) shardValue);
          } else if (original instanceof Long) {
            original = (Long) original + ((Long) shardValue);
          } else {
            // ignore?
          }
          mainResponse.setVal(originalId, original);
        } else {
          // ignore?
        }
      }
    }
  }

  /**
   * Adjustable parts cloned.
   *
   * @param original
   *          the original
   * @return the object
   */
  private Object adjustablePartsCloned(Object original) {
    if (original instanceof NamedList) {
      NamedList<Object> newObject = new SimpleOrderedMap();
      NamedList<Object> originalObject = (NamedList<Object>) original;
      for (int i = 0; i < originalObject.size(); i++) {
        newObject.add(originalObject.getName(i),
            adjustablePartsCloned(originalObject.getVal(i)));
      }
      return newObject;
    } else if (original instanceof ArrayList) {
      ArrayList<Object> newObject = new ArrayList<Object>();
      ArrayList<Object> originalObject = (ArrayList<Object>) original;
      for (int i = 0; i < originalObject.size(); i++) {
        newObject.add(adjustablePartsCloned(originalObject.get(i)));
      }
      return newObject;
    } else if (original instanceof Integer) {
      Integer originalObject = (Integer) original;
      return new Integer(originalObject.intValue());
    }
    return original;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#finishStage(org.apache.
   * solr.handler.component.ResponseBuilder)
   */
  @Override
  public void finishStage(ResponseBuilder rb) {
    // System.out.println(System.nanoTime()+" - "+Thread.currentThread().getId()
    // + " - "
    // + rb.req.getParams().getBool("isShard", false) + " FINISHRESPONSES "
    // + rb.stage + " " + rb.req.getParamString());
    if (rb.req.getParams().getBool(PARAM_MTAS, false)) {
      // mtas response
      NamedList<Object> mtasResponse = null;
      try {
        mtasResponse = (NamedList<Object>) rb.rsp.getValues().get("mtas");
      } catch (ClassCastException e) {
        mtasResponse = null;
      }
      if (mtasResponse == null) {
        mtasResponse = new SimpleOrderedMap<>();
        rb.rsp.add("mtas", mtasResponse);
      }
      // get fields stage
      if ((rb.stage == ResponseBuilder.STAGE_GET_FIELDS)) {
        if (rb.req.getParams().getBool(PARAM_MTAS_KWIC, false)) {
          finishStageArrayList(rb, mtasResponse, "kwic", null);
        }
        if (rb.req.getParams().getBool(PARAM_MTAS_LIST, false)) {
          finishStageArrayList(rb, mtasResponse, "list",
              ShardRequest.PURPOSE_PRIVATE);
        }
        // execute query stage
      } else if ((rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY)) {
        if (rb.req.getParams().getBool(PARAM_MTAS_STATS, false)) {
          finishStageNamedList(rb, mtasResponse, "stats", null);
        }
        if (rb.req.getParams().getBool(PARAM_MTAS_LIST, false)) {
          finishStageArrayList(rb, mtasResponse, "list", null);
        }
        if (rb.req.getParams().getBool(PARAM_MTAS_GROUP, false)) {
          finishStageArrayList(rb, mtasResponse, "group", null);
        }
        if (rb.req.getParams().getBool(PARAM_MTAS_TERMVECTOR, false)) {
          finishStageArrayList(rb, mtasResponse, "termvector", null);
        }
        if (rb.req.getParams().getBool(PARAM_MTAS_FACET, false)) {
          finishStageArrayList(rb, mtasResponse, "facet", null);
        }
        if (rb.req.getParams().getBool(PARAM_MTAS_PREFIX, false)) {
          finishStageArrayList(rb, mtasResponse, "prefix", null);
          // repair prefix lists
          try {
            ArrayList<NamedList> list = (ArrayList<NamedList>) mtasResponse
                .findRecursive("prefix");
            if (list != null) {
              for (NamedList item : list) {
                TreeSet<String> singlePosition = (TreeSet<String>) item
                    .get("singlePosition");
                TreeSet<String> multiplePosition = (TreeSet<String>) item
                    .get("multiplePosition");
                for (String prefix : multiplePosition) {
                  if (singlePosition.contains(prefix)) {
                    singlePosition.remove(prefix);
                  }
                }
              }
            }
          } catch (ClassCastException e) {

          }
        }
      }
    }

  }

  /**
   * Finish stage array list.
   *
   * @param rb
   *          the rb
   * @param mtasResponse
   *          the mtas response
   * @param key
   *          the key
   * @param preferredPurpose
   *          the preferred purpose
   */
  private void finishStageArrayList(ResponseBuilder rb,
      NamedList<Object> mtasResponse, String key, Integer preferredPurpose) {
    // create new response for key
    ArrayList<Object> mtasListResponse = new ArrayList<Object>();
    mtasResponse.removeAll(key);
    mtasResponse.add(key, mtasListResponse);
    // collect responses for each shard
    HashMap<String, ArrayList<Object>> mtasListShardResponses = new HashMap<String, ArrayList<Object>>();
    for (ShardRequest sreq : rb.finished) {
      for (ShardResponse response : sreq.responses) {
        // only continue if new shard or preferred purpose
        if (mtasListShardResponses.containsKey(response.getShard())
            && ((preferredPurpose == null)
                || (sreq.purpose != preferredPurpose))) {
          break;
        }
        // update
        try {
          NamedList<Object> result = response.getSolrResponse().getResponse();
          ArrayList<Object> data = (ArrayList<Object>) result
              .findRecursive("mtas", key);
          if (data != null) {
            mtasListShardResponses.put(response.getShard(), decode(data));
          }
        } catch (ClassCastException e) {

        }
      }
    }

    try {
      for (ArrayList<Object> mtasListShardResponse : mtasListShardResponses
          .values()) {
        mergeResponsesArrayList(mtasListResponse, mtasListShardResponse);
      }
      rewrite(mtasListResponse);
    } catch (IOException e) {
      mtasListResponse.add(e.getMessage());
    }
  }

  /**
   * Finish stage named list.
   *
   * @param rb
   *          the rb
   * @param mtasResponse
   *          the mtas response
   * @param key
   *          the key
   * @param preferredPurpose
   *          the preferred purpose
   */
  private void finishStageNamedList(ResponseBuilder rb,
      NamedList<Object> mtasResponse, String key, Integer preferredPurpose) {
    // create new response for key
    NamedList<Object> mtasListResponse = new SimpleOrderedMap<>();
    mtasResponse.removeAll(key);
    mtasResponse.add(key, mtasListResponse);
    // collect responses for each shard
    HashMap<String, NamedList<Object>> mtasListShardResponses = new HashMap<String, NamedList<Object>>();
    for (ShardRequest sreq : rb.finished) {
      for (ShardResponse response : sreq.responses) {
        // only continue if new shard or preferred purpose
        if (mtasListShardResponses.containsKey(response.getShard())
            && ((preferredPurpose == null)
                || (sreq.purpose != preferredPurpose))) {
          break;
        }
        // update
        try {
          NamedList<Object> result = response.getSolrResponse().getResponse();
          NamedList<Object> data = (NamedList<Object>) result
              .findRecursive("mtas", key);
          if (data != null) {
            mtasListShardResponses.put(response.getShard(), decode(data));
          }
        } catch (ClassCastException e) {

        }
      }
      try {
        for (NamedList<Object> mtasListShardResponse : mtasListShardResponses
            .values()) {
          mergeResponsesNamedList(mtasListResponse, mtasListShardResponse);
        }
        rewrite(mtasListResponse);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#distributedProcess(org.
   * apache.solr.handler.component.ResponseBuilder)
   */
  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
//    System.out.println(Thread.currentThread().getId() + " - "
//        + rb.req.getParams().getBool("isShard", false) + " DISTIRBUTEDPROCESS "
//        + rb.stage + " " + rb.req.getParamString());
    if (rb.req.getParams().getBool(PARAM_MTAS, false)) {
      ComponentFields mtasFields = getMtasFields(rb);
      if (rb.req.getParams().getBool(PARAM_MTAS_LIST, false)) {
        distributedProcessList(rb, mtasFields);
      }
      if (rb.req.getParams().getBool(PARAM_MTAS_TERMVECTOR, false)) {
        distributedProcessTermvector(rb, mtasFields);
      }
    }
    return ResponseBuilder.STAGE_DONE;
  }

  private void distributedProcessTermvector(ResponseBuilder rb,
      ComponentFields mtasFields) throws IOException {
    System.out.println(Thread.currentThread().getId() + "\t" + rb.stage);
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      HashMap<String, HashMap<String, Integer>> listShardTotals = new HashMap<String, HashMap<String, Integer>>();
      for (ShardRequest sreq : rb.finished) {
        if (sreq.params.getBool(PARAM_MTAS, false)
            && sreq.params.getBool(PARAM_MTAS_TERMVECTOR, false)) {
          HashMap<String, HashMap<String, HashSet<String>>> missingKeys = computeMissingItemsPerShard(
              sreq.responses, "mtas", "termvector");
          System.out
              .println(Thread.currentThread().getId() + "\t" + missingKeys);
        }
      }
    }
  }

  /**
   * Distributed process list.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   */
  private void distributedProcessList(ResponseBuilder rb,
      ComponentFields mtasFields) {
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      if (mtasFields.doList) {
        // compute total from shards
        HashMap<String, HashMap<String, Integer>> listShardTotals = new HashMap<String, HashMap<String, Integer>>();
        for (ShardRequest sreq : rb.finished) {
          if (sreq.params.getBool(PARAM_MTAS, false)
              && sreq.params.getBool(PARAM_MTAS_LIST, false)) {
            for (ShardResponse response : sreq.responses) {
              NamedList<Object> result = response.getSolrResponse()
                  .getResponse();
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
              }
            }
          }
        }
        // compute shard requests
        HashMap<String, ModifiableSolrParams> shardRequests = new HashMap<String, ModifiableSolrParams>();
        int requestId = 0;
        for (String field : mtasFields.list.keySet()) {
          for (ComponentList list : mtasFields.list.get(field).listList) {
            requestId++;
            if (listShardTotals.containsKey(list.key) && (list.number > 0)) {
              Integer position = 0, total = 0;
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
                position += subTotal;
              }
            }
          }
        }

        for (String shardName : shardRequests.keySet()) {
          ShardRequest sreq = new ShardRequest();
          sreq.shards = new String[] { shardName };
          sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
          sreq.params = new ModifiableSolrParams();
          sreq.params.add("fq", rb.req.getParams().getParams("fq"));
          sreq.params.add("q", rb.req.getParams().getParams("q"));
          sreq.params.add("cache", rb.req.getParams().getParams("cache"));
          sreq.params.add("rows", "0");
          sreq.params.add(PARAM_MTAS,
              rb.req.getOriginalParams().getParams(PARAM_MTAS));
          sreq.params.add(PARAM_MTAS_LIST,
              rb.req.getOriginalParams().getParams(PARAM_MTAS_LIST));
          sreq.params.add(shardRequests.get(shardName));
          rb.addRequest(this, sreq);
        }
      }
    }
  }

  private HashMap<String, HashMap<String, HashSet<String>>> computeMissingItemsPerShard(
      List<ShardResponse> shardResponses, String... args) throws IOException {
    HashMap<String, HashMap<String, HashSet<String>>> result = new HashMap<String, HashMap<String, HashSet<String>>>();
    HashMap<String, HashMap<String, HashSet<String>>> itemsPerShardSets = new HashMap<String, HashMap<String, HashSet<String>>>();
    HashMap<String, HashSet<String>> itemSets = new HashMap<String, HashSet<String>>();
    // loop over responses different shards
    for (ShardResponse shardResponse : shardResponses) {
      NamedList<Object> response = shardResponse.getSolrResponse()
          .getResponse();
      try {
        ArrayList<NamedList<Object>> data = (ArrayList<NamedList<Object>>) response
            .findRecursive(args);
        if (data != null) {
          // loop over different keys
          for (NamedList<Object> dataItem : data) {
            Object oKey = dataItem.get("key");
            Object oList = dataItem.get("list");
            if (oKey != null && oList != null && oKey instanceof String
                && oList instanceof ComponentSortSelect) {
              ComponentSortSelect list = (ComponentSortSelect) oList;
              if (list.sortType.equals(CodecUtil.SORT_TERM)) {
                break;
              } else {
                String key = (String) oKey;
                HashMap<String, HashSet<String>> itemsPerShardSet;
                HashSet<String> itemSet, tmpItemSet = new HashSet<String>();
                if (itemsPerShardSets.containsKey(key)) {
                  itemsPerShardSet = itemsPerShardSets.get(key);
                  itemSet = itemSets.get(key);
                } else {
                  itemsPerShardSet = new HashMap<String, HashSet<String>>();
                  itemSet = new HashSet<String>();
                  itemsPerShardSets.put(key, itemsPerShardSet);
                  itemSets.put(key, itemSet);
                }
                itemsPerShardSet.put(shardResponse.getShardAddress(),
                    tmpItemSet);
                Iterator<Entry<String, Object>> it = list.getNamedList()
                    .iterator();
                String item;
                while (it.hasNext()) {
                  item = it.next().getKey();
                  tmpItemSet.add(item);
                  itemSet.add(item);
                }
              }
            }
          }
        }
      } catch (ClassCastException e) {
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

  /**
   * Gets the mtas fields.
   *
   * @param rb
   *          the rb
   * @return the mtas fields
   */
  private ComponentFields getMtasFields(ResponseBuilder rb) {
    return (ComponentFields) rb.req.getContext().get(ComponentFields.class);
  }

  /**
   * Encode.
   *
   * @param o
   *          the o
   * @return the string
   */
  private String encode(Object o) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream;
    try {
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(o);
      objectOutputStream.close();
      return Base64.byteArrayToBase64(byteArrayOutputStream.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Decode.
   *
   * @param s
   *          the s
   * @return the object
   */
  private Object decode(String s) {
    byte[] bytes = Base64.base64ToByteArray(s);
    ObjectInputStream objectInputStream;
    try {
      objectInputStream = new ObjectInputStream(
          new ByteArrayInputStream(bytes));
      return objectInputStream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Decode.
   *
   * @param l
   *          the l
   * @return the array list
   */
  private ArrayList decode(ArrayList l) {
    for (int i = 0; i < l.size(); i++) {
      if (l.get(i) instanceof NamedList) {
        l.set(i, decode((NamedList) l.get(i)));
      } else if (l.get(i) instanceof ArrayList) {
        l.set(i, decode((ArrayList) l.get(i)));
      }
    }
    return l;
  }

  /**
   * Decode.
   *
   * @param nl
   *          the nl
   * @return the named list
   */
  private NamedList<Object> decode(NamedList<Object> nl) {
    for (int i = 0; i < nl.size(); i++) {
      String key = nl.getName(i);
      Object o = nl.getVal(i);
      if (key.matches("^_encoded_.*$")) {
        if (o instanceof String) {
          Object decodedObject = decode((String) nl.getVal(i));
          String decodedKey = key.replaceFirst("^_encoded_", "");
          if (decodedKey.equals("")) {
            decodedKey = "_" + decodedObject.getClass().getSimpleName() + "_";
          }
          nl.setName(i, decodedKey);
          nl.setVal(i, decodedObject);
        } else if (o instanceof NamedList) {
          NamedList nl2 = (NamedList) o;
          for (int j = 0; j < nl2.size(); j++) {
            if (nl2.getVal(j) instanceof String) {
              nl2.setVal(j, decode((String) nl2.getVal(j)));
            }
          }
        } else {
          System.out.println("unknown type " + o.getClass().getCanonicalName());
        }
      } else {
        if (o instanceof NamedList) {
          nl.setVal(i, decode((NamedList<Object>) o));
        } else if (o instanceof ArrayList) {
          nl.setVal(i, decode((ArrayList<Object>) o));
        }
      }
    }
    return nl;
  }

  /**
   * Rewrite.
   *
   * @param al
   *          the al
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void rewrite(ArrayList<Object> al) throws IOException {
    for (int i = 0; i < al.size(); i++) {
      if (al.get(i) instanceof NamedList) {
        rewrite((NamedList) al.get(i));
      } else if (al.get(i) instanceof ArrayList) {
        rewrite((ArrayList) al.get(i));
      }
    }
  }

  /**
   * Rewrite.
   *
   * @param nl
   *          the nl
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void rewrite(NamedList<Object> nl) throws IOException {
    HashMap<String, NamedList<Object>> collapseNamedList = null;
    HashMap<String, String> mergeMappingList = new HashMap<String, String>();
    Pattern mergePattern = Pattern.compile("_merge_([^_]+)_([^_]+)+");
    int length = nl.size();
    for (int i = 0; i < length; i++) {
      if (nl.getVal(i) instanceof NamedList) {
        NamedList o = (NamedList) nl.getVal(i);
        rewrite(o);
        nl.setVal(i, o);
      } else if (nl.getVal(i) instanceof ArrayList) {
        ArrayList o = (ArrayList) nl.getVal(i);
        rewrite(o);
        nl.setVal(i, o);
      } else if (nl.getVal(i) instanceof MtasDataItem) {
        MtasDataItem dataItem = (MtasDataItem) nl.getVal(i);
        nl.setVal(i, dataItem.rewrite());
      } else if (nl.getVal(i) instanceof ComponentSortSelect) {
        ComponentSortSelect o = (ComponentSortSelect) nl.getVal(i);
        if (o.dataCollector.getCollectorType()
            .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
          NamedList<Object> nnl = o.getNamedList();
          rewrite(nnl);
          nl.setVal(i, nnl);
          Matcher mergeMatcher = mergePattern.matcher(nl.getName(i));
          if (mergeMatcher.find()) {
            mergeMappingList.put(mergeMatcher.group(1), mergeMatcher.group(2));
          }
        } else if (o.dataCollector.getCollectorType()
            .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
          NamedList<Object> nnl = o.getData();
          if (nnl.size() > 0) {
            rewrite(nnl);
            collapseNamedList = new HashMap<String, NamedList<Object>>();
            collapseNamedList.put(nl.getName(i), nnl);
            nl.setVal(i, nnl);
          } else {
            nl.setVal(i, null);
          }
        }
      }
    }
    // merge
    if (mergeMappingList.size() > 0) {
      for (String key : mergeMappingList.keySet()) {
        Object so = nl
            .get("_merge_" + key + "_" + mergeMappingList.get(key) + "_");
        Object to = nl.get(mergeMappingList.get(key));
        if (to != null && so != null && to instanceof NamedList
            && so instanceof NamedList) {
          NamedList<Object> snl = (NamedList<Object>) so;
          NamedList<Object> tnl = (NamedList<Object>) to;
          for (int i = 0; i < tnl.size(); i++) {
            Object item = snl.get(tnl.getName(i));
            if (item != null && tnl.getVal(i) instanceof NamedList) {
              ((NamedList<Object>) tnl.getVal(i)).add(key, item);
            }
          }
        } else {
          throw new IOException(
              "can't merge " + key + " with " + mergeMappingList.get(key));
        }
      }
      for (String key : mergeMappingList.keySet()) {
        nl.remove("_merge_" + key + "_" + mergeMappingList.get(key) + "_");
      }
    }
    // collapse
    if (collapseNamedList != null) {
      for (String key : collapseNamedList.keySet()) {
        nl.remove(key);
      }
      for (NamedList<Object> items : collapseNamedList.values()) {
        nl.addAll(items);
      }
    }
  }

  /**
   * Creates the stats position.
   *
   * @param position
   *          the position
   * @param encode
   *          the encode
   * @return the named list
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private SimpleOrderedMap<Object> createStatsPosition(
      ComponentPosition position, Boolean encode) throws IOException {
    // System.out.println("Create stats position " + position.dataType + " "
    // + position.statsType + " " + position.statsItems + " --- " + encode);
    SimpleOrderedMap<Object> mtasPositionResponse = new SimpleOrderedMap<>();
    mtasPositionResponse.add("key", position.key);
    ComponentSortSelect data = new ComponentSortSelect(position.dataCollector,
        position.dataType, position.statsType, position.statsItems);
    if (encode) {
      mtasPositionResponse.add("_encoded_data", encode(data));
    } else {
      mtasPositionResponse.add(position.dataCollector.getCollectorType(), data);
      rewrite(mtasPositionResponse);
    }
    return mtasPositionResponse;
  }

  /**
   * Creates the stats token.
   *
   * @param token
   *          the token
   * @param encode
   *          the encode
   * @return the named list
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private SimpleOrderedMap<Object> createStatsToken(ComponentToken token,
      Boolean encode) throws IOException {
    // System.out.println("Create stats position " + position.dataType + " "
    // + position.statsType + " " + position.statsItems + " --- " + encode);
    SimpleOrderedMap<Object> mtasTokenResponse = new SimpleOrderedMap<>();
    mtasTokenResponse.add("key", token.key);
    ComponentSortSelect data = new ComponentSortSelect(token.dataCollector,
        token.dataType, token.statsType, token.statsItems);
    if (encode) {
      mtasTokenResponse.add("_encoded_data", encode(data));
    } else {
      mtasTokenResponse.add(token.dataCollector.getCollectorType(), data);
      rewrite(mtasTokenResponse);
    }
    return mtasTokenResponse;
  }

  /**
   * Creates the stats span.
   *
   * @param span
   *          the span
   * @param encode
   *          the encode
   * @return the named list
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private SimpleOrderedMap<Object> createStatsSpan(ComponentSpan span,
      Boolean encode) throws IOException {
    // System.out.println("Create stats span " + span.dataType + " "
    // + span.statsType + " " + span.statsItems + " --- " + encode);
    SimpleOrderedMap<Object> mtasSpanResponse = new SimpleOrderedMap<>();
    mtasSpanResponse.add("key", span.key);
    ComponentSortSelect data = new ComponentSortSelect(span.dataCollector,
        span.dataType, span.statsType, span.statsItems);
    if (encode) {
      mtasSpanResponse.add("_encoded_data", encode(data));
    } else {
      mtasSpanResponse.add(span.dataCollector.getCollectorType(), data);
      rewrite(mtasSpanResponse);
    }
    return mtasSpanResponse;
  }

  /**
   * Creates the term vector.
   *
   * @param termVector
   *          the term vector
   * @param encode
   *          the encode
   * @return the named list
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private SimpleOrderedMap<Object> createTermVector(
      ComponentTermVector termVector, Boolean encode) throws IOException {
    SimpleOrderedMap<Object> mtasTermVectorResponse = new SimpleOrderedMap<>();
    mtasTermVectorResponse.add("key", termVector.key);
    ComponentSortSelect defaultData = new ComponentSortSelect(
        termVector.dataDefaultCollector,
        new String[] { termVector.defaultDataType },
        new String[] { termVector.defaultStatsType },
        new TreeSet[] { termVector.defaultStatsItems },
        new String[] { termVector.sortType },
        new String[] { termVector.sortDirection },
        new Integer[] { termVector.start },
        new Integer[] { termVector.number });
    ComponentSortSelect functionData = null;
    if (termVector.dataFunctionCollector != null) {
      functionData = new ComponentSortSelect(termVector.dataFunctionCollector,
          new String[] { termVector.functionDataType },
          new String[] { termVector.functionStatsType },
          new TreeSet[] { termVector.functionStatsItems },
          new String[] { termVector.sortType },
          new String[] { termVector.sortDirection },
          new Integer[] { termVector.start },
          new Integer[] { termVector.number });
    }
    if (encode) {
      mtasTermVectorResponse.add("_encoded_list", encode(defaultData));
      if (functionData != null) {
        mtasTermVectorResponse.add("_encoded__merge_function_list_",
            encode(functionData));
      }
    } else {
      mtasTermVectorResponse.add("list", defaultData);
      if (functionData != null) {
        mtasTermVectorResponse.add("_merge_function_list_", functionData);
      }
      rewrite(mtasTermVectorResponse);
    }

    return mtasTermVectorResponse;
  }

  /**
   * Creates the prefix.
   *
   * @param prefix
   *          the prefix
   * @param encode
   *          the encode
   * @return the named list
   */
  private SimpleOrderedMap<Object> createPrefix(ComponentPrefix prefix,
      Boolean encode) {
    SimpleOrderedMap<Object> mtasPrefixResponse = new SimpleOrderedMap<Object>();
    mtasPrefixResponse.add("key", prefix.key);
    if (encode) {
      mtasPrefixResponse.add("_encoded_singlePosition",
          encode(prefix.singlePositionList));
      mtasPrefixResponse.add("_encoded_multiplePosition",
          encode(prefix.multiplePositionList));
    } else {
      mtasPrefixResponse.add("singlePosition", prefix.singlePositionList);
      mtasPrefixResponse.add("multiplePosition", prefix.multiplePositionList);
    }
    return mtasPrefixResponse;
  }

  /**
   * Creates the list.
   *
   * @param list
   *          the list
   * @return the named list
   */
  private SimpleOrderedMap<Object> createList(ComponentList list) {
    SimpleOrderedMap<Object> mtasListResponse = new SimpleOrderedMap<>();
    mtasListResponse.add("key", list.key);
    mtasListResponse.add("total", list.total);
    if (list.output != null) {
      ArrayList<NamedList<Object>> mtasListItemResponses = new ArrayList<NamedList<Object>>();
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

          TreeMap<Integer, ArrayList<ArrayList<String>>> hitData = new TreeMap<Integer, ArrayList<ArrayList<String>>>();
          TreeMap<Integer, ArrayList<ArrayList<String>>> leftData = null,
              rightData = null;
          if (list.left > 0) {
            leftData = new TreeMap<Integer, ArrayList<ArrayList<String>>>();
          }
          if (list.right > 0) {
            rightData = new TreeMap<Integer, ArrayList<ArrayList<String>>>();
          }
          for (int position = Math.max(0,
              hit.startPosition - list.left); position <= (hit.endPosition
                  + list.right); position++) {
            ArrayList<ArrayList<String>> hitDataItem = new ArrayList<ArrayList<String>>();
            if (hit.hits.containsKey(position)) {
              for (String term : hit.hits.get(position)) {
                ArrayList<String> hitDataSubItem = new ArrayList<String>();
                hitDataSubItem.add(CodecUtil.termPrefix(term));
                hitDataSubItem.add(CodecUtil.termValue(term));
                hitDataItem.add(hitDataSubItem);
              }
            }
            if (position < hit.startPosition) {
              leftData.put(position, hitDataItem);
            } else if (position > hit.endPosition) {
              rightData.put(position, hitDataItem);
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

          ArrayList<NamedList<Object>> mtasListItemResponseItemTokens = new ArrayList<NamedList<Object>>();
          for (MtasToken<?> token : tokenHit.tokens) {
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
   * Creates the group.
   *
   * @param group
   *          the group
   * @param encode
   *          the encode
   * @return the named list
   */
  private SimpleOrderedMap<Object> createGroup(ComponentGroup group,
      Boolean encode) {
    SimpleOrderedMap<Object> mtasGroupResponse = new SimpleOrderedMap<>();
    mtasGroupResponse.add("key", group.key);
    ComponentSortSelect data = new ComponentSortSelect(group.dataCollector,
        new String[] { group.dataType }, new String[] { group.statsType },
        new TreeSet[] { group.statsItems }, new String[] { group.sortType },
        new String[] { group.sortDirection }, new Integer[] { group.start },
        new Integer[] { group.number });
    if (encode) {
      mtasGroupResponse.add("_encoded_list", encode(data));
    } else {
      try {
        mtasGroupResponse.add("list", data);
        rewrite(mtasGroupResponse);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return mtasGroupResponse;
  }

  /**
   * Creates the facet.
   *
   * @param facet
   *          the facet
   * @param encode
   *          the encode
   * @return the named list
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private SimpleOrderedMap<Object> createFacet(ComponentFacet facet,
      Boolean encode) throws IOException {
    SimpleOrderedMap<Object> mtasFacetResponse = new SimpleOrderedMap<>();
    mtasFacetResponse.add("key", facet.key);
    SimpleOrderedMap<Object> mtasFacetItemResponses = new SimpleOrderedMap();
    ComponentSortSelect data = new ComponentSortSelect(facet.dataCollector,
        facet.baseDataTypes, facet.baseStatsTypes, facet.baseStatsItems,
        facet.baseSortTypes, facet.baseSortDirections, null, facet.baseNumbers);
    if (encode) {
      mtasFacetResponse.add("_encoded_list", encode(data));
    } else {
      try {
        mtasFacetResponse.add("list", data);
        rewrite(mtasFacetResponse);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return mtasFacetResponse;
  }

  /**
   * Creates the kwic.
   *
   * @param kwic
   *          the kwic
   * @return the named list
   */
  private SimpleOrderedMap<Object> createKwic(ComponentKwic kwic) {
    SimpleOrderedMap<Object> mtasKwicResponse = new SimpleOrderedMap<>();
    mtasKwicResponse.add("key", kwic.key);
    ArrayList<NamedList<Object>> mtasKwicItemResponses = new ArrayList<NamedList<Object>>();
    if (kwic.output.equals(ComponentKwic.KWIC_OUTPUT_HIT)) {
      for (int docId : kwic.hits.keySet()) {
        NamedList<Object> mtasKwicItemResponse = new SimpleOrderedMap<>();
        ArrayList<KwicHit> list = kwic.hits.get(docId);
        ArrayList<NamedList<Object>> mtasKwicItemResponseItems = new ArrayList<NamedList<Object>>();
        for (KwicHit h : list) {
          NamedList<Object> mtasKwicItemResponseItem = new SimpleOrderedMap<>();
          TreeMap<Integer, ArrayList<ArrayList<String>>> hitData = new TreeMap<Integer, ArrayList<ArrayList<String>>>();
          TreeMap<Integer, ArrayList<ArrayList<String>>> leftData = null,
              rightData = null;
          if (kwic.left > 0) {
            leftData = new TreeMap<Integer, ArrayList<ArrayList<String>>>();
          }
          if (kwic.right > 0) {
            rightData = new TreeMap<Integer, ArrayList<ArrayList<String>>>();
          }
          for (int position = Math.max(0,
              h.startPosition - kwic.left); position <= (h.endPosition
                  + kwic.right); position++) {
            if (h.hits.containsKey(position)) {
              ArrayList<ArrayList<String>> hitDataItem = new ArrayList<ArrayList<String>>();
              for (String term : h.hits.get(position)) {
                ArrayList<String> hitDataSubItem = new ArrayList<String>();
                hitDataSubItem.add(CodecUtil.termPrefix(term));
                hitDataSubItem.add(CodecUtil.termValue(term));
                hitDataItem.add(hitDataSubItem);
              }
              if (position < h.startPosition) {
                leftData.put(position, hitDataItem);
              } else if (position > h.endPosition) {
                rightData.put(position, hitDataItem);
              } else {
                hitData.put(position, hitDataItem);
              }
            }
          }
          if (kwic.left > 0) {
            mtasKwicItemResponseItem.add("left", leftData);
          }
          mtasKwicItemResponseItem.add("hit", hitData);
          if (kwic.right > 0) {
            mtasKwicItemResponseItem.add("right", rightData);
          }
          mtasKwicItemResponseItems.add(mtasKwicItemResponseItem);
        }
        mtasKwicItemResponse.add("documentKey", kwic.uniqueKey.get(docId));
        mtasKwicItemResponse.add("documentTotal", kwic.subTotal.get(docId));
        mtasKwicItemResponse.add("documentMinPosition",
            kwic.minPosition.get(docId));
        mtasKwicItemResponse.add("documentMaxPosition",
            kwic.maxPosition.get(docId));
        mtasKwicItemResponse.add("list", mtasKwicItemResponseItems);
        mtasKwicItemResponses.add(mtasKwicItemResponse);
      }
    } else if (kwic.output.equals(ComponentKwic.KWIC_OUTPUT_TOKEN)) {
      for (int docId : kwic.tokens.keySet()) {
        NamedList<Object> mtasKwicItemResponse = new SimpleOrderedMap<>();
        ArrayList<KwicToken> list = kwic.tokens.get(docId);
        ArrayList<NamedList<Object>> mtasKwicItemResponseItems = new ArrayList<NamedList<Object>>();
        for (KwicToken k : list) {
          NamedList<Object> mtasKwicItemResponseItem = new SimpleOrderedMap<>();
          mtasKwicItemResponseItem.add("startPosition", k.startPosition);
          mtasKwicItemResponseItem.add("endPosition", k.endPosition);
          ArrayList<NamedList<Object>> mtasKwicItemResponseItemTokens = new ArrayList<NamedList<Object>>();
          for (MtasToken<?> token : k.tokens) {
            NamedList<Object> mtasKwicItemResponseItemToken = new SimpleOrderedMap<>();
            if (token.getId() != null) {
              mtasKwicItemResponseItemToken.add("mtasId", token.getId());
            }
            mtasKwicItemResponseItemToken.add("prefix", token.getPrefix());
            mtasKwicItemResponseItemToken.add("value", token.getPostfix());
            if (token.getPositionStart() != null) {
              mtasKwicItemResponseItemToken.add("positionStart",
                  token.getPositionStart());
              mtasKwicItemResponseItemToken.add("positionEnd",
                  token.getPositionEnd());
            }
            if (token.getPositions() != null) {
              mtasKwicItemResponseItemToken.add("positions",
                  token.getPositions());
            }
            if (token.getParentId() != null) {
              mtasKwicItemResponseItemToken.add("parentMtasId",
                  token.getParentId());
            }
            if (token.getPayload() != null) {
              mtasKwicItemResponseItemToken.add("payload", token.getPayload());
            }
            if (token.getOffsetStart() != null) {
              mtasKwicItemResponseItemToken.add("offsetStart",
                  token.getOffsetStart());
              mtasKwicItemResponseItemToken.add("offsetEnd",
                  token.getOffsetEnd());
            }
            if (token.getRealOffsetStart() != null) {
              mtasKwicItemResponseItemToken.add("realOffsetStart",
                  token.getRealOffsetStart());
              mtasKwicItemResponseItemToken.add("realOffsetEnd",
                  token.getRealOffsetEnd());
            }
            mtasKwicItemResponseItemTokens.add(mtasKwicItemResponseItemToken);
          }
          mtasKwicItemResponseItem.add("tokens",
              mtasKwicItemResponseItemTokens);
          mtasKwicItemResponseItems.add(mtasKwicItemResponseItem);
        }
        mtasKwicItemResponse.add("documentKey", kwic.uniqueKey.get(docId));
        mtasKwicItemResponse.add("documentTotal", kwic.subTotal.get(docId));
        mtasKwicItemResponse.add("documentMinPosition",
            kwic.minPosition.get(docId));
        mtasKwicItemResponse.add("documentMaxPosition",
            kwic.maxPosition.get(docId));
        mtasKwicItemResponse.add("list", mtasKwicItemResponseItems);
        mtasKwicItemResponses.add(mtasKwicItemResponse);
      }
    }
    mtasKwicResponse.add("list", mtasKwicItemResponses);
    return mtasKwicResponse;
  }

  /**
   * The Class ComponentSortSelect.
   */
  public static class ComponentSortSelect implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The stats type. */
    public String dataType, statsType;

    /** The stats items. */
    public TreeSet<String> statsItems;

    /** The sort direction. */
    public String sortType, sortDirection;

    /** The number. */
    public Integer start, number;

    /** The data collector. */
    public MtasDataCollector<?, ?> dataCollector = null;

    /** The sub stats type. */
    private String[] subDataType, subStatsType;

    /** The sub stats items. */
    private TreeSet<String>[] subStatsItems;

    /** The sub sort direction. */
    private String[] subSortType, subSortDirection;

    /** The sub number. */
    private Integer[] subStart, subNumber;

    /**
     * Instantiates a new component sort select.
     *
     * @param dataCollector
     *          the data collector
     * @param dataType
     *          the data type
     * @param statsType
     *          the stats type
     * @param statsItems
     *          the stats items
     * @param sortType
     *          the sort type
     * @param sortDirection
     *          the sort direction
     * @param start
     *          the start
     * @param number
     *          the number
     */
    public ComponentSortSelect(MtasDataCollector<?, ?> dataCollector,
        String[] dataType, String[] statsType, TreeSet<String>[] statsItems,
        String[] sortType, String[] sortDirection, Integer[] start,
        Integer[] number) {
      this.dataCollector = dataCollector;
      this.dataType = (dataType == null) ? null : dataType[0];
      this.statsType = (statsType == null) ? null : statsType[0];
      this.statsItems = (statsItems == null) ? null : statsItems[0];
      this.sortType = (sortType == null) ? null : sortType[0];
      this.sortDirection = (sortDirection == null) ? null : sortDirection[0];
      this.start = (start == null) ? null : start[0];
      this.number = (number == null) ? null : number[0];
      if ((dataType != null) && (dataType.length > 1)) {
        subDataType = new String[dataType.length - 1];
        subStatsType = new String[dataType.length - 1];
        subStatsItems = new TreeSet[dataType.length - 1];
        subSortType = new String[dataType.length - 1];
        subSortDirection = new String[dataType.length - 1];
        System.arraycopy(dataType, 1, subDataType, 0, dataType.length - 1);
        System.arraycopy(statsType, 1, subStatsType, 0, dataType.length - 1);
        System.arraycopy(statsItems, 1, subStatsItems, 0, dataType.length - 1);
        System.arraycopy(sortType, 1, subSortType, 0, dataType.length - 1);
        System.arraycopy(sortDirection, 1, subSortDirection, 0,
            dataType.length - 1);
      } else {
        subDataType = null;
        subStatsType = null;
        subStatsItems = null;
        subSortType = null;
        subSortDirection = null;
      }
    }

    /**
     * Instantiates a new component sort select.
     *
     * @param dataCollector
     *          the data collector
     * @param dataType
     *          the data type
     * @param statsType
     *          the stats type
     * @param statsItems
     *          the stats items
     */
    public ComponentSortSelect(MtasDataCollector dataCollector, String dataType,
        String statsType, TreeSet<String> statsItems) {
      this(dataCollector, new String[] { dataType }, new String[] { statsType },
          new TreeSet[] { statsItems }, new String[] { null },
          new String[] { null }, new Integer[] { 0 }, new Integer[] { 1 });
    }

    /**
     * Merge.
     *
     * @param newItem
     *          the new item
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public void merge(ComponentSortSelect newItem) throws IOException {
      dataCollector.merge(newItem.dataCollector);
    }

    /**
     * Gets the total.
     *
     * @return the total
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public int getTotal() throws IOException {
      if (dataCollector.getCollectorType()
          .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
        return dataCollector.getSize();
      } else {
        throw new IOException(
            "only allowed for " + DataCollector.COLLECTOR_TYPE_LIST);
      }
    }

    /**
     * Gets the data.
     *
     * @return the data
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public NamedList<Object> getData() throws IOException {
      if (dataCollector.getCollectorType()
          .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
        NamedList<Object> mtasResponse = new SimpleOrderedMap<>();
        MtasDataItem dataItem = dataCollector.getData();
        if (dataItem != null) {
          mtasResponse.addAll(dataItem.rewrite());
        }
        if ((subDataType != null) && (dataItem.getSub() != null)) {
          ComponentSortSelect css = new ComponentSortSelect(dataItem.getSub(),
              subDataType, subStatsType, subStatsItems, subSortType,
              subSortDirection, subStart, subNumber);
          if (dataItem.getSub().getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
            mtasResponse.add(dataItem.getSub().getCollectorType(),
                css.getNamedList());
          } else if (dataItem.getSub().getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
            mtasResponse.add(dataItem.getSub().getCollectorType(),
                css.getData());
          }
        }
        return mtasResponse;
      } else {
        throw new IOException(
            "only allowed for " + DataCollector.COLLECTOR_TYPE_DATA);
      }
    }

    /**
     * Gets the named list.
     *
     * @return the named list
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public NamedList<Object> getNamedList() throws IOException {
      if (dataCollector.getCollectorType()
          .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
        SimpleOrderedMap<Object> mtasResponseList = new SimpleOrderedMap<>();
        Map<String, MtasDataItem<?>> dataList = (Map<String, MtasDataItem<?>>) dataCollector
            .getList();
        for (String key : dataList.keySet()) {
          SimpleOrderedMap<Object> mtasResponseListItem = new SimpleOrderedMap<>();
          MtasDataItem<?> dataItem = dataList.get(key);
          mtasResponseListItem.addAll(dataItem.rewrite());
          if ((subDataType != null) && (dataItem.getSub() != null)) {
            ComponentSortSelect css = new ComponentSortSelect(dataItem.getSub(),
                subDataType, subStatsType, subStatsItems, subSortType,
                subSortDirection, subStart, subNumber);
            if (dataItem.getSub().getCollectorType()
                .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
              mtasResponseListItem.add(dataItem.getSub().getCollectorType(),
                  css.getNamedList());
            } else if (dataItem.getSub().getCollectorType()
                .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
              mtasResponseListItem.add(dataItem.getSub().getCollectorType(),
                  css.getData());
            }
          }
          mtasResponseList.add(key, mtasResponseListItem);
        }
        return mtasResponseList;
      } else {
        throw new IOException(
            "only allowed for " + DataCollector.COLLECTOR_TYPE_LIST);
      }
    }

  }

}
