package mtas.codec.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import mtas.analysis.token.MtasToken;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.parser.function.MtasFunctionParser;
import mtas.parser.function.ParseException;
import mtas.parser.function.util.MtasFunctionParserFunction;
import mtas.parser.function.util.MtasFunctionParserFunctionDefault;
import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * The Class CodecComponent.
 */
public class CodecComponent {

  /**
   * The Class ComponentFields.
   */
  public static class ComponentFields {

    /** The list. */
    public Map<String, ComponentField> list;

    /** The do kwic. */
    public boolean doKwic;

    /** The do list. */
    public boolean doList;

    /** The do group. */
    public boolean doGroup;

    /** The do term vector. */
    public boolean doTermVector;

    /** The do stats. */
    public boolean doStats;

    /** The do stats spans. */
    public boolean doStatsSpans;

    /** The do stats positions. */
    public boolean doStatsPositions;

    /** The do stats tokens. */
    public boolean doStatsTokens;

    /** The do prefix. */
    public boolean doPrefix;

    /** The do facet. */
    public boolean doFacet;

    /**
     * Instantiates a new component fields.
     */
    public ComponentFields() {
      list = new HashMap<String, ComponentField>();
      doKwic = false;
      doList = false;
      doGroup = false;
      doStats = false;
      doTermVector = false;
      doStatsSpans = false;
      doStatsPositions = false;
      doStatsTokens = false;
      doPrefix = false;
      doFacet = false;
    }
  }

  /**
   * The Class ComponentField.
   */
  public static class ComponentField {

    /** The field. */
    public String field;

    /** The unique key field. */
    public String uniqueKeyField;

    /** The kwic list. */
    public List<ComponentKwic> kwicList;

    /** The list list. */
    public List<ComponentList> listList;

    /** The group list. */
    public List<ComponentGroup> groupList;

    /** The facet list. */
    public List<ComponentFacet> facetList;

    /** The term vector list. */
    public List<ComponentTermVector> termVectorList;

    /** The stats position list. */
    public List<ComponentPosition> statsPositionList;

    /** The stats token list. */
    public List<ComponentToken> statsTokenList;

    /** The stats span list. */
    public List<ComponentSpan> statsSpanList;

    /** The span query list. */
    public List<SpanQuery> spanQueryList;

    /** The prefix. */
    public ComponentPrefix prefix;

    /**
     * Instantiates a new component field.
     *
     * @param field the field
     * @param uniqueKeyField the unique key field
     */
    public ComponentField(String field, String uniqueKeyField) {
      this.field = field;
      this.uniqueKeyField = uniqueKeyField;
      kwicList = new ArrayList<ComponentKwic>();
      listList = new ArrayList<ComponentList>();
      groupList = new ArrayList<ComponentGroup>();
      facetList = new ArrayList<ComponentFacet>();
      termVectorList = new ArrayList<ComponentTermVector>();
      statsPositionList = new ArrayList<ComponentPosition>();
      statsTokenList = new ArrayList<ComponentToken>();
      statsSpanList = new ArrayList<ComponentSpan>();
      spanQueryList = new ArrayList<SpanQuery>();
      prefix = null;
    }
  }

  /**
   * The Class ComponentPrefix.
   */
  public static class ComponentPrefix {

    /** The key. */
    public String key;

    /** The single position list. */
    public TreeSet<String> singlePositionList;

    /** The multiple position list. */
    public TreeSet<String> multiplePositionList;

    /** The set position list. */
    public TreeSet<String> setPositionList;

    /** The intersecting list. */
    public TreeSet<String> intersectingList;

    /**
     * Instantiates a new component prefix.
     *
     * @param key the key
     */
    public ComponentPrefix(String key) {
      this.key = key;
      singlePositionList = new TreeSet<String>();
      multiplePositionList = new TreeSet<String>();
      setPositionList = new TreeSet<String>();
      intersectingList = new TreeSet<String>();
    }

    /**
     * Adds the single position.
     *
     * @param prefix the prefix
     */
    public void addSinglePosition(String prefix) {
      if (!prefix.trim().equals("")) {
        if (!singlePositionList.contains(prefix)
            && !multiplePositionList.contains(prefix)) {
          singlePositionList.add(prefix);
        }
      }
    }

    /**
     * Adds the multiple position.
     *
     * @param prefix the prefix
     */
    public void addMultiplePosition(String prefix) {
      if (!prefix.trim().equals("")) {
        if (!singlePositionList.contains(prefix)) {
          if (!multiplePositionList.contains(prefix)) {
            multiplePositionList.add(prefix);
          }
        } else {
          singlePositionList.remove(prefix);
          multiplePositionList.add(prefix);
        }
      }
    }

    /**
     * Adds the set position.
     *
     * @param prefix the prefix
     */
    public void addSetPosition(String prefix) {
      if (!prefix.trim().equals("")) {
        if (!singlePositionList.contains(prefix)) {
          if (!setPositionList.contains(prefix)) {
            setPositionList.add(prefix);
          }
        } else {
          singlePositionList.remove(prefix);
          setPositionList.add(prefix);
        }
      }
    }

    /**
     * Adds the intersecting.
     *
     * @param prefix the prefix
     */
    public void addIntersecting(String prefix) {
      if (!prefix.trim().equals("")) {
        intersectingList.add(prefix);
      }
    }

  }

  /**
   * The Class ComponentKwic.
   */
  public static class ComponentKwic {

    /** The query. */
    public SpanQuery query;

    /** The key. */
    public String key;

    /** The tokens. */
    public HashMap<Integer, ArrayList<KwicToken>> tokens;

    /** The hits. */
    public HashMap<Integer, ArrayList<KwicHit>> hits;

    /** The unique key. */
    public HashMap<Integer, String> uniqueKey;

    /** The sub total. */
    public HashMap<Integer, Integer> subTotal;

    /** The min position. */
    public HashMap<Integer, Integer> minPosition;

    /** The max position. */
    public HashMap<Integer, Integer> maxPosition;

    /** The prefixes. */
    public ArrayList<String> prefixes;

    /** The start. */
    public int left, right, start;

    /** The number. */
    public Integer number;

    /** The output. */
    public String output;

    /** The Constant KWIC_OUTPUT_TOKEN. */
    public static final String KWIC_OUTPUT_TOKEN = "token";

    /** The Constant KWIC_OUTPUT_HIT. */
    public static final String KWIC_OUTPUT_HIT = "hit";

    /**
     * Instantiates a new component kwic.
     *
     * @param query the query
     * @param key the key
     * @param prefixes the prefixes
     * @param number the number
     * @param start the start
     * @param left the left
     * @param right the right
     * @param output the output
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public ComponentKwic(SpanQuery query, String key, String prefixes,
        Integer number, int start, int left, int right, String output)
        throws IOException {
      this.query = query;
      this.key = key;
      this.left = (left > 0) ? left : 0;
      this.right = (right > 0) ? right : 0;
      this.start = (start > 0) ? start : 0;
      this.number = (number != null && number >= 0) ? number : null;
      this.output = output;
      tokens = new HashMap<Integer, ArrayList<KwicToken>>();
      hits = new HashMap<Integer, ArrayList<KwicHit>>();
      uniqueKey = new HashMap<Integer, String>();
      subTotal = new HashMap<Integer, Integer>();
      minPosition = new HashMap<Integer, Integer>();
      maxPosition = new HashMap<Integer, Integer>();
      this.prefixes = new ArrayList<String>();
      if ((prefixes != null) && (prefixes.trim().length() > 0)) {
        List<String> l = Arrays.asList(prefixes.split(Pattern.quote(",")));
        for (String ls : l) {
          if (ls.trim().length() > 0) {
            this.prefixes.add(ls.trim());
          }
        }
      }
      if (this.output == null) {
        if (this.prefixes.size() > 0) {
          this.output = ComponentKwic.KWIC_OUTPUT_HIT;
        } else {
          this.output = ComponentKwic.KWIC_OUTPUT_TOKEN;
        }
      } else if (!this.output.equals(ComponentKwic.KWIC_OUTPUT_HIT)
          && !this.output.equals(ComponentKwic.KWIC_OUTPUT_TOKEN)) {
        throw new IOException("unrecognized output '" + this.output + "'");
      }
    }
  }

  /**
   * The Class ComponentList.
   */
  public static class ComponentList {

    /** The span query. */
    public SpanQuery spanQuery;

    /** The key. */
    public String field, queryValue, queryType, key;

    /** The tokens. */
    public ArrayList<ListToken> tokens;

    /** The hits. */
    public ArrayList<ListHit> hits;

    /** The unique key. */
    public HashMap<Integer, String> uniqueKey;

    /** The sub total. */
    public HashMap<Integer, Integer> subTotal;

    /** The min position. */
    public HashMap<Integer, Integer> minPosition;

    /** The max position. */
    public HashMap<Integer, Integer> maxPosition;

    /** The prefixes. */
    public ArrayList<String> prefixes;

    /** The start. */
    public int left, right, total, position, start;

    /** The number. */
    public int number;

    /** The output. */
    public String prefix, output;

    /** The Constant LIST_OUTPUT_TOKEN. */
    public static final String LIST_OUTPUT_TOKEN = "token";

    /** The Constant LIST_OUTPUT_HIT. */
    public static final String LIST_OUTPUT_HIT = "hit";

    /**
     * Instantiates a new component list.
     *
     * @param spanQuery the span query
     * @param field the field
     * @param queryValue the query value
     * @param queryType the query type
     * @param key the key
     * @param prefix the prefix
     * @param start the start
     * @param number the number
     * @param left the left
     * @param right the right
     * @param output the output
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public ComponentList(SpanQuery spanQuery, String field, String queryValue,
        String queryType, String key, String prefix, int start, int number,
        int left, int right, String output) throws IOException {
      this.spanQuery = spanQuery;
      this.field = field;
      this.queryValue = queryValue;
      this.queryType = queryType;
      this.key = key;
      this.left = left;
      this.right = right;
      this.start = start;
      this.number = number;
      this.output = output;
      this.prefix = prefix;
      total = 0;
      position = 0;
      tokens = new ArrayList<ListToken>();
      hits = new ArrayList<ListHit>();
      uniqueKey = new HashMap<Integer, String>();
      subTotal = new HashMap<Integer, Integer>();
      minPosition = new HashMap<Integer, Integer>();
      maxPosition = new HashMap<Integer, Integer>();
      this.prefixes = new ArrayList<String>();
      if ((prefix != null) && (prefix.trim().length() > 0)) {
        List<String> l = Arrays.asList(prefix.split(Pattern.quote(",")));
        for (String ls : l) {
          if (ls.trim().length() > 0) {
            this.prefixes.add(ls.trim());
          }
        }
      }
      // check output
      if (this.output == null) {
        if (this.prefixes.size() > 0) {
          this.output = ComponentList.LIST_OUTPUT_HIT;
        } else {
          this.output = ComponentList.LIST_OUTPUT_TOKEN;
        }
      } else if (!this.output.equals(ComponentList.LIST_OUTPUT_HIT)
          && !this.output.equals(ComponentList.LIST_OUTPUT_TOKEN)) {
        throw new IOException("unrecognized output '" + this.output + "'");
      }
    }
  }

  /**
   * The Class ComponentGroup.
   */
  public static class ComponentGroup {

    /** The span query. */
    public SpanQuery spanQuery;

    /** The sort direction. */
    public String dataType, statsType, sortType, sortDirection;

    /** The stats items. */
    public TreeSet<String> statsItems;

    /** The number. */
    public Integer start, number;

    /** The key. */
    public String field, queryValue, queryType, key;

    /** The data collector. */
    public MtasDataCollector<?, ?> dataCollector;

    /** The prefixes. */
    ArrayList<String> prefixes;

    /** The hit inside. */
    HashSet<String> hitInside;

    /** The right. */
    HashSet<String>[] hitInsideLeft, hitInsideRight, hitLeft, hitRight, left,
        right;

    /**
     * Instantiates a new component group.
     *
     * @param spanQuery the span query
     * @param field the field
     * @param queryValue the query value
     * @param queryType the query type
     * @param key the key
     * @param number the number
     * @param groupingHitInsidePrefixes the grouping hit inside prefixes
     * @param groupingHitInsideLeftPosition the grouping hit inside left position
     * @param groupingHitInsideLeftPrefixes the grouping hit inside left prefixes
     * @param groupingHitInsideRightPosition the grouping hit inside right position
     * @param groupingHitInsideRightPrefixes the grouping hit inside right prefixes
     * @param groupingHitLeftPosition the grouping hit left position
     * @param groupingHitLeftPrefixes the grouping hit left prefixes
     * @param groupingHitRightPosition the grouping hit right position
     * @param groupingHitRightPrefixes the grouping hit right prefixes
     * @param groupingLeftPosition the grouping left position
     * @param groupingLeftPrefixes the grouping left prefixes
     * @param groupingRightPosition the grouping right position
     * @param groupingRightPrefixes the grouping right prefixes
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public ComponentGroup(SpanQuery spanQuery, String field, String queryValue,
        String queryType, String key, int number,
        String groupingHitInsidePrefixes,
        String[] groupingHitInsideLeftPosition,
        String[] groupingHitInsideLeftPrefixes,
        String[] groupingHitInsideRightPosition,
        String[] groupingHitInsideRightPrefixes,
        String[] groupingHitLeftPosition, String[] groupingHitLeftPrefixes,
        String[] groupingHitRightPosition, String[] groupingHitRightPrefixes,
        String[] groupingLeftPosition, String[] groupingLeftPrefixes,
        String[] groupingRightPosition, String[] groupingRightPrefixes)
        throws IOException {
      this.spanQuery = spanQuery;
      this.field = field;
      this.queryValue = queryValue;
      this.queryType = queryType;
      this.key = key;
      this.dataType = CodecUtil.DATA_TYPE_LONG;
      this.statsItems = CodecUtil.createStatsItems("n,sum,mean");
      this.statsType = CodecUtil.createStatsType(this.statsItems, this.sortType,
          null);
      this.sortType = CodecUtil.STATS_TYPE_SUM;
      this.sortDirection = CodecUtil.SORT_DESC;
      this.start = 0;
      this.number = number;
      HashSet<String> tmpPrefixes = new HashSet<String>();
      // analyze grouping condition
      if (groupingHitInsidePrefixes != null) {
        hitInside = new HashSet<String>();
        String[] tmpList = groupingHitInsidePrefixes.split(",");
        for (String tmpItem : tmpList) {
          if (!tmpItem.trim().equals("")) {
            hitInside.add(tmpItem.trim());
          }
        }
        tmpPrefixes.addAll(hitInside);
      } else {
        hitInside = null;
      }
      hitInsideLeft = createPositionedPrefixes(tmpPrefixes,
          groupingHitInsideLeftPosition, groupingHitInsideLeftPrefixes);
      hitInsideRight = createPositionedPrefixes(tmpPrefixes,
          groupingHitInsideRightPosition, groupingHitInsideRightPrefixes);
      hitLeft = createPositionedPrefixes(tmpPrefixes, groupingHitLeftPosition,
          groupingHitLeftPrefixes);
      hitRight = createPositionedPrefixes(tmpPrefixes, groupingHitRightPosition,
          groupingHitRightPrefixes);
      left = createPositionedPrefixes(tmpPrefixes, groupingLeftPosition,
          groupingLeftPrefixes);
      right = createPositionedPrefixes(tmpPrefixes, groupingRightPosition,
          groupingRightPrefixes);
      prefixes = new ArrayList<String>(tmpPrefixes);
      // datacollector
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_LIST, this.dataType, this.statsType,
          this.statsItems, this.sortType, this.sortDirection, this.start,
          this.number, null, null);
    }

  }

  /**
   * Creates the positioned prefixes.
   *
   * @param prefixList the prefix list
   * @param position the position
   * @param prefixes the prefixes
   * @return the hash set[]
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static HashSet<String>[] createPositionedPrefixes(
      HashSet<String> prefixList, String[] position, String[] prefixes)
      throws IOException {
    Pattern p = Pattern.compile("([0-9]+)(\\-([0-9]+))?");
    Matcher m;
    if (position == null && prefixes == null) {
      return null;
    } else if (prefixes == null || position == null
        || position.length != prefixes.length) {
      throw new IOException("incorrect position/prefixes");
    } else if (position.length == 0) {
      return null;
    } else {
      // analyze positions
      int[][] tmpPosition = new int[position.length][];
      int maxPosition = -1;
      for (int i = 0; i < position.length; i++) {
        m = p.matcher(position[i]);
        if (m.find()) {
          if (m.group(3) == null) {
            int start = Integer.parseInt(m.group(1));
            tmpPosition[i] = new int[] { start };
            maxPosition = Math.max(maxPosition, start);
          } else {
            int start = Integer.parseInt(m.group(1));
            int end = Integer.parseInt(m.group(3));
            if (start > end) {
              throw new IOException("incorrect position " + position[i]);
            } else {
              tmpPosition[i] = new int[end - start + 1];
              for (int t = start; t <= end; t++)
                tmpPosition[i][t - start] = t;
              maxPosition = Math.max(maxPosition, end);
            }
          }
        } else {
          throw new IOException("incorrect position " + position[i]);
        }
      }
      @SuppressWarnings("unchecked")
      HashSet<String>[] result = new HashSet[maxPosition + 1];
      Arrays.fill(result, null);
      List<String> tmpPrefixList;
      String[] tmpList;
      for (int i = 0; i < tmpPosition.length; i++) {
        tmpList = prefixes[i].split(",");
        tmpPrefixList = new ArrayList<String>();
        for (String tmpItem : tmpList) {
          if (!tmpItem.trim().equals("")) {
            tmpPrefixList.add(tmpItem.trim());
          }
        }
        if (tmpPrefixList.size() == 0) {
          throw new IOException("incorrect prefixes " + prefixes[i]);
        }
        for (int t = 0; t < tmpPosition[i].length; t++) {
          if (result[tmpPosition[i][t]] == null) {
            result[tmpPosition[i][t]] = new HashSet<String>();
          }
          result[tmpPosition[i][t]].addAll(tmpPrefixList);
        }
        prefixList.addAll(tmpPrefixList);
      }
      return result;
    }
  }

  /**
   * The Class ComponentFacet.
   */
  public static class ComponentFacet {

    /** The span queries. */
    public SpanQuery[] spanQueries;

    /** The base sort directions. */
    public String[] baseFields, baseFieldTypes, baseTypes, baseSortTypes,
        baseSortDirections;

    /** The base stats types. */
    public String[] baseCollectorTypes, baseDataTypes, baseStatsTypes;

    /** The base stats items. */
    public TreeSet<String>[] baseStatsItems;

    /** The field. */
    public String key, field;

    /** The data collector. */
    public MtasDataCollector<?, ?> dataCollector;

    /** The base function list. */
    public HashMap<MtasDataCollector<?, ?>, SubComponentFunction[]>[] baseFunctionList;

    /** The base numbers. */
    public Integer[] baseNumbers;

    /** The base maximum longs. */
    public Long[] baseMinimumLongs, baseMaximumLongs;

    /** The base maximum doubles. */
    public Double[] baseMinimumDoubles, baseMaximumDoubles;

    /** The base parsers. */
    public MtasFunctionParserFunction[] baseParsers;

    /** The base function keys. */
    public String[][] baseFunctionKeys;

    /** The base function expressions. */
    public String[][] baseFunctionExpressions;

    /** The base function types. */
    public String[][] baseFunctionTypes;

    /** The base function parser functions. */
    public MtasFunctionParserFunction[][] baseFunctionParserFunctions;

    /** The Constant TYPE_INTEGER. */
    public static final String TYPE_INTEGER = "integer";

    /** The Constant TYPE_DOUBLE. */
    public static final String TYPE_DOUBLE = "double";

    /** The Constant TYPE_LONG. */
    public static final String TYPE_LONG = "long";

    /** The Constant TYPE_FLOAT. */
    public static final String TYPE_FLOAT = "float";

    /** The Constant TYPE_STRING. */
    public static final String TYPE_STRING = "string";

    /**
     * Instantiates a new component facet.
     *
     * @param spanQueries the span queries
     * @param field the field
     * @param key the key
     * @param baseFields the base fields
     * @param baseFieldTypes the base field types
     * @param baseTypes the base types
     * @param baseSortTypes the base sort types
     * @param baseSortDirections the base sort directions
     * @param baseNumbers the base numbers
     * @param baseMinimumDoubles the base minimum doubles
     * @param baseMaximumDoubles the base maximum doubles
     * @param baseFunctionKeys the base function keys
     * @param baseFunctionExpressions the base function expressions
     * @param baseFunctionTypes the base function types
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ParseException the parse exception
     */
    @SuppressWarnings("unchecked")
    public ComponentFacet(SpanQuery[] spanQueries, String field, String key,
        String[] baseFields, String[] baseFieldTypes, String[] baseTypes,
        String[] baseSortTypes, String[] baseSortDirections,
        Integer[] baseNumbers, Double[] baseMinimumDoubles,
        Double[] baseMaximumDoubles, String[][] baseFunctionKeys,
        String[][] baseFunctionExpressions, String[][] baseFunctionTypes)
        throws IOException, ParseException {
      this.spanQueries = spanQueries;
      this.field = field;
      this.key = key;
      this.baseFields = baseFields;
      this.baseFieldTypes = baseFieldTypes;
      this.baseTypes = baseTypes;
      this.baseSortTypes = baseSortTypes;
      this.baseSortDirections = baseSortDirections;
      this.baseNumbers = baseNumbers;
      this.baseMinimumDoubles = baseMinimumDoubles;
      this.baseMaximumDoubles = baseMaximumDoubles;
      // compute types
      this.baseMinimumLongs = new Long[baseFields.length];
      this.baseMaximumLongs = new Long[baseFields.length];
      this.baseCollectorTypes = new String[baseFields.length];
      this.baseStatsItems = new TreeSet[baseFields.length];
      this.baseStatsTypes = new String[baseFields.length];
      this.baseDataTypes = new String[baseFields.length];
      this.baseParsers = new MtasFunctionParserFunction[baseFields.length];
      this.baseFunctionList = new HashMap[baseFields.length];
      this.baseFunctionParserFunctions = new MtasFunctionParserFunction[baseFields.length][];
      for (int i = 0; i < baseFields.length; i++) {
        if (baseMinimumDoubles[i] != null) {
          this.baseMinimumLongs[i] = baseMinimumDoubles[i].longValue();
        } else {
          this.baseMinimumLongs[i] = null;
        }
        if (baseMaximumDoubles[i] != null) {
          this.baseMaximumLongs[i] = baseMaximumDoubles[i].longValue();
        } else {
          this.baseMaximumLongs[i] = null;
        }
        baseDataTypes[i] = CodecUtil.DATA_TYPE_LONG;
        baseFunctionList[i] = new HashMap<MtasDataCollector<?, ?>, SubComponentFunction[]>();
        baseFunctionParserFunctions[i] = null;
        baseParsers[i] = new MtasFunctionParserFunctionDefault(
            this.spanQueries.length);
        if (this.baseSortDirections[i] == null) {
          this.baseSortDirections[i] = CodecUtil.SORT_ASC;
        } else if (!this.baseSortDirections[i].equals(CodecUtil.SORT_ASC)
            && !this.baseSortDirections[i].equals(CodecUtil.SORT_DESC)) {
          throw new IOException(
              "unrecognized sortDirection " + this.baseSortDirections[i]);
        }
        if (this.baseSortTypes[i] == null) {
          this.baseSortTypes[i] = CodecUtil.SORT_TERM;
        } else if (!this.baseSortTypes[i].equals(CodecUtil.SORT_TERM)
            && !CodecUtil.STATS_TYPES.contains(this.baseSortTypes[i])) {
          throw new IOException(
              "unrecognized sortType " + this.baseSortTypes[i]);
        }
        this.baseCollectorTypes[i] = DataCollector.COLLECTOR_TYPE_LIST;
        this.baseStatsItems[i] = CodecUtil.createStatsItems(baseTypes[i]);
        this.baseStatsTypes[i] = CodecUtil.createStatsType(baseStatsItems[i],
            this.baseSortTypes[i], new MtasFunctionParserFunctionDefault(1));
      }
      if (baseFunctionKeys != null && baseFunctionExpressions != null
          && baseFunctionTypes != null) {
        if (baseFunctionKeys.length == baseFields.length
            && baseFunctionExpressions.length == baseFields.length
            && baseFunctionTypes.length == baseFields.length) {
          this.baseFunctionKeys = new String[baseFields.length][];
          this.baseFunctionExpressions = new String[baseFields.length][];
          this.baseFunctionTypes = new String[baseFields.length][];
          for (int i = 0; i < baseFields.length; i++) {
            if (baseFunctionKeys[i].length == baseFunctionExpressions[i].length
                && baseFunctionKeys[i].length == baseFunctionTypes[i].length) {
              this.baseFunctionKeys[i] = new String[baseFunctionKeys[i].length];
              this.baseFunctionExpressions[i] = new String[baseFunctionExpressions[i].length];
              this.baseFunctionTypes[i] = new String[baseFunctionTypes[i].length];
              baseFunctionParserFunctions[i] = new MtasFunctionParserFunction[baseFunctionExpressions[i].length];
              for (int j = 0; j < baseFunctionKeys[i].length; j++) {
                this.baseFunctionKeys[i][j] = baseFunctionKeys[i][j];
                this.baseFunctionExpressions[i][j] = baseFunctionExpressions[i][j];
                this.baseFunctionTypes[i][j] = baseFunctionTypes[i][j];
                baseFunctionParserFunctions[i][j] = new MtasFunctionParser(
                    new BufferedReader(
                        new StringReader(baseFunctionExpressions[i][j])))
                            .parse();
              }
            } else {
              this.baseFunctionKeys[i] = new String[0];
              this.baseFunctionExpressions[i] = new String[0];
              this.baseFunctionTypes[i] = new String[0];
              baseFunctionParserFunctions[i] = new MtasFunctionParserFunction[0];
            }
          }
        }
      }
      if (baseFields.length > 0) {
        if (baseFields.length == 1) {
          dataCollector = DataCollector.getCollector(this.baseCollectorTypes[0],
              this.baseDataTypes[0], this.baseStatsTypes[0],
              this.baseStatsItems[0], this.baseSortTypes[0],
              this.baseSortDirections[0], 0, this.baseNumbers[0], null, null);
        } else {
          String[] subBaseCollectorTypes = Arrays
              .copyOfRange(baseCollectorTypes, 1, baseDataTypes.length);
          String[] subBaseDataTypes = Arrays.copyOfRange(baseDataTypes, 1,
              baseDataTypes.length);
          String[] subBaseStatsTypes = Arrays.copyOfRange(baseStatsTypes, 1,
              baseStatsTypes.length);
          TreeSet<String>[] subBaseStatsItems = Arrays
              .copyOfRange(baseStatsItems, 1, baseStatsItems.length);
          String[] subBaseSortTypes = Arrays.copyOfRange(baseSortTypes, 1,
              baseSortTypes.length);
          String[] subBaseSortDirections = Arrays
              .copyOfRange(baseSortDirections, 1, baseSortDirections.length);
          Integer[] subNumbers = Arrays.copyOfRange(baseNumbers, 1,
              baseNumbers.length);
          Integer[] subStarts = ArrayUtils.toObject(new int[subNumbers.length]);
          dataCollector = DataCollector.getCollector(this.baseCollectorTypes[0],
              this.baseDataTypes[0], this.baseStatsTypes[0],
              this.baseStatsItems[0], this.baseSortTypes[0],
              this.baseSortDirections[0], 0, this.baseNumbers[0],
              subBaseCollectorTypes, subBaseDataTypes, subBaseStatsTypes,
              subBaseStatsItems, subBaseSortTypes, subBaseSortDirections,
              subStarts, subNumbers, null, null);
        }
      } else {
        throw new IOException("no baseFields");
      }
    }

    /**
     * Function sum rule.
     *
     * @return true, if successful
     */
    public boolean functionSumRule() {
      if (baseFunctionParserFunctions != null) {
        for (int i = 0; i < baseFields.length; i++) {
          for (MtasFunctionParserFunction function : baseFunctionParserFunctions[i]) {
            if (!function.sumRule()) {
              return false;
            }
          }
        }
      }
      return true;
    }

    /**
     * Function need positions.
     *
     * @return true, if successful
     */
    public boolean functionNeedPositions() {
      if (baseFunctionParserFunctions != null) {
        for (int i = 0; i < baseFields.length; i++) {
          for (MtasFunctionParserFunction function : baseFunctionParserFunctions[i]) {
            if (function.needPositions()) {
              return true;
            }
          }
        }
      }
      return false;
    }

    /**
     * Base parser sum rule.
     *
     * @return true, if successful
     */
    public boolean baseParserSumRule() {
      for (int i = 0; i < baseFields.length; i++) {
        if (!baseParsers[i].sumRule()) {
          return false;
        }
      }
      return true;
    }

    /**
     * Base parser need positions.
     *
     * @return true, if successful
     */
    public boolean baseParserNeedPositions() {
      for (int i = 0; i < baseFields.length; i++) {
        if (baseParsers[i].needPositions()) {
          return true;
        }
      }
      return false;
    }

  }

  /**
   * The Class ComponentTermVector.
   */
  public static class ComponentTermVector {

    /** The boundary. */
    public String key, prefix, regexp, boundary;

    /** The list. */
    public HashSet<String> list;

    /** The functions. */
    public ArrayList<SubComponentFunction> functions;

    /** The number. */
    public int number;

    /** The start value. */
    public String startValue;

    /** The compiled automaton. */
    public CompiledAutomaton compiledAutomaton;

    /** The sub component function. */
    public SubComponentFunction subComponentFunction;

    /** The boundary registration. */
    public boolean boundaryRegistration;

    /**
     * Instantiates a new component term vector.
     *
     * @param key the key
     * @param prefix the prefix
     * @param regexp the regexp
     * @param type the type
     * @param sortType the sort type
     * @param sortDirection the sort direction
     * @param startValue the start value
     * @param number the number
     * @param functionKey the function key
     * @param functionExpression the function expression
     * @param functionType the function type
     * @param boundary the boundary
     * @param list the list
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ParseException the parse exception
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ComponentTermVector(String key, String prefix, String regexp,
        String type, String sortType, String sortDirection, String startValue,
        int number, String[] functionKey, String[] functionExpression,
        String[] functionType, String boundary, String[] list)
        throws IOException, ParseException {
      this.key = key;
      this.prefix = prefix;
      this.regexp = regexp;
      sortType = sortType == null ? CodecUtil.SORT_TERM : sortType;
      sortDirection = sortDirection == null ? (sortType == CodecUtil.SORT_TERM)
          ? CodecUtil.SORT_ASC : CodecUtil.SORT_DESC : sortDirection;
      if (list != null && list.length > 0) {
        this.list = new HashSet(Arrays.asList(list));
        this.boundary = null;
        this.number = Integer.MAX_VALUE;
        this.startValue = null;
        sortType = CodecUtil.SORT_TERM;
        sortDirection = CodecUtil.SORT_ASC;
      } else {
        this.list = null;
        this.startValue = startValue;
        if (boundary == null) {
          this.boundary = null;
          this.number = number;
        } else {
          this.boundary = boundary;
          this.number = Integer.MAX_VALUE;
        }
      }
      functions = new ArrayList<SubComponentFunction>();
      if (functionKey != null && functionExpression != null
          && functionType != null) {
        if (functionKey.length == functionExpression.length
            && functionKey.length == functionType.length) {
          for (int i = 0; i < functionKey.length; i++) {
            functions
                .add(new SubComponentFunction(DataCollector.COLLECTOR_TYPE_LIST,
                    functionKey[i], functionExpression[i], functionType[i]));
          }
        }
      }
      if (!sortType.equals(CodecUtil.SORT_TERM)
          && !CodecUtil.STATS_TYPES.contains(sortType)) {
        throw new IOException("unknown sortType '" + sortType + "'");
      } else if (!sortType.equals(CodecUtil.SORT_TERM)) {
        if (!(sortType.equals(CodecUtil.STATS_TYPE_SUM)
            || sortType.equals(CodecUtil.STATS_TYPE_N))) {
          throw new IOException(
              "sortType '" + sortType + "' not supported for termVector");
        }
      }
      if (!sortDirection.equals(CodecUtil.SORT_ASC)
          && !sortDirection.equals(CodecUtil.SORT_DESC)) {
        throw new IOException(
            "unrecognized sortDirection '" + sortDirection + "'");
      }
      boundaryRegistration = this.boundary != null;
      String segmentRegistration = null;
      if (this.boundary != null) {
        if (sortDirection.equals(CodecUtil.SORT_ASC)) {
          segmentRegistration = MtasDataCollector.SEGMENT_BOUNDARY_ASC;
        } else if (sortDirection.equals(CodecUtil.SORT_DESC)) {
          segmentRegistration = MtasDataCollector.SEGMENT_BOUNDARY_DESC;
        }
      } else if (!sortType.equals(CodecUtil.SORT_TERM)) {
        if (sortDirection.equals(CodecUtil.SORT_ASC)) {
          segmentRegistration = MtasDataCollector.SEGMENT_SORT_ASC;
        } else if (sortDirection.equals(CodecUtil.SORT_DESC)) {
          segmentRegistration = MtasDataCollector.SEGMENT_SORT_DESC;
        }
      }
      // create main subComponentFunction
      this.subComponentFunction = new SubComponentFunction(
          DataCollector.COLLECTOR_TYPE_LIST, key, type,
          new MtasFunctionParserFunctionDefault(1), sortType, sortDirection, 0,
          this.number, segmentRegistration, boundary);

      if ((regexp == null) || (regexp.isEmpty())) {
        RegExp re = new RegExp(prefix + MtasToken.DELIMITER + ".*");
        compiledAutomaton = new CompiledAutomaton(re.toAutomaton());
      } else {
        RegExp re = new RegExp(
            prefix + MtasToken.DELIMITER + regexp + "\u0000*");
        compiledAutomaton = new CompiledAutomaton(re.toAutomaton());
      }
    }

    /**
     * Function sum rule.
     *
     * @return true, if successful
     */
    public boolean functionSumRule() {
      if (functions != null) {
        for (SubComponentFunction function : functions) {
          if (!function.parserFunction.sumRule()) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Function need positions.
     *
     * @return true, if successful
     */
    public boolean functionNeedPositions() {
      if (functions != null) {
        for (SubComponentFunction function : functions) {
          if (function.parserFunction.needPositions()) {
            return true;
          }
        }
      }
      return false;
    }

  }

  /**
   * The Class ComponentSpan.
   */
  public static class ComponentSpan {

    /** The queries. */
    public SpanQuery[] queries;

    /** The key. */
    public String key;

    /** The data type. */
    public String dataType;

    /** The stats type. */
    public String statsType;

    /** The stats items. */
    public TreeSet<String> statsItems;

    /** The maximum double. */
    public Double minimumDouble, maximumDouble;

    /** The maximum long. */
    public Long minimumLong, maximumLong;

    /** The data collector. */
    public MtasDataCollector<?, ?> dataCollector;

    /** The functions. */
    public ArrayList<SubComponentFunction> functions;

    /** The parser. */
    public MtasFunctionParserFunction parser;

    /**
     * Instantiates a new component span.
     *
     * @param queries the queries
     * @param key the key
     * @param minimumDouble the minimum double
     * @param maximumDouble the maximum double
     * @param type the type
     * @param functionKey the function key
     * @param functionExpression the function expression
     * @param functionType the function type
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ParseException the parse exception
     */
    public ComponentSpan(SpanQuery[] queries, String key, Double minimumDouble,
        Double maximumDouble, String type, String[] functionKey,
        String[] functionExpression, String[] functionType)
        throws IOException, ParseException {
      this.queries = queries;
      this.key = key;
      functions = new ArrayList<SubComponentFunction>();
      if (functionKey != null && functionExpression != null
          && functionType != null) {
        if (functionKey.length == functionExpression.length
            && functionKey.length == functionType.length) {
          for (int i = 0; i < functionKey.length; i++) {
            functions
                .add(new SubComponentFunction(DataCollector.COLLECTOR_TYPE_DATA,
                    functionKey[i], functionExpression[i], functionType[i]));
          }
        }
      }
      parser = new MtasFunctionParserFunctionDefault(queries.length);
      dataType = parser.getType();
      statsItems = CodecUtil.createStatsItems(type);
      statsType = CodecUtil.createStatsType(this.statsItems, null, parser);
      if (minimumDouble != null) {
        this.minimumDouble = minimumDouble;
        this.minimumLong = minimumDouble.longValue();
      } else {
        this.minimumDouble = null;
        this.minimumLong = null;
      }
      if (maximumDouble != null) {
        this.maximumDouble = maximumDouble;
        this.maximumLong = maximumDouble.longValue();
      } else {
        this.maximumDouble = null;
        this.maximumLong = null;
      }
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_DATA, dataType, this.statsType,
          this.statsItems, null, null, null, null, null, null);
    }

    /**
     * Function sum rule.
     *
     * @return true, if successful
     */
    public boolean functionSumRule() {
      if (functions != null) {
        for (SubComponentFunction function : functions) {
          if (!function.parserFunction.sumRule()) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Function need positions.
     *
     * @return true, if successful
     */
    public boolean functionNeedPositions() {
      if (functions != null) {
        for (SubComponentFunction function : functions) {
          if (function.parserFunction.needPositions()) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     * Function need arguments.
     *
     * @return the hash set
     */
    public HashSet<Integer> functionNeedArguments() {
      HashSet<Integer> list = new HashSet<Integer>();
      if (functions != null) {
        for (SubComponentFunction function : functions) {
          list.addAll(function.parserFunction.needArgument());
        }
      }
      return list;
    }

  }

  /**
   * The Class ComponentPosition.
   */
  public static class ComponentPosition {

    /** The field. */
    public String field;

    /** The key. */
    public String key;

    /** The stats type. */
    public String dataType, statsType;

    /** The stats items. */
    public TreeSet<String> statsItems;

    /** The maximum double. */
    public Double minimumDouble, maximumDouble;

    /** The maximum long. */
    public Long minimumLong, maximumLong;

    /** The data collector. */
    public MtasDataCollector<?, ?> dataCollector;

    /** The function parser. */
    public MtasFunctionParserFunction functionParser;

    /**
     * Instantiates a new component position.
     *
     * @param field the field
     * @param key the key
     * @param minimumDouble the minimum double
     * @param maximumDouble the maximum double
     * @param statsType the stats type
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ParseException the parse exception
     */
    public ComponentPosition(String field, String key, Double minimumDouble,
        Double maximumDouble, String statsType)
        throws IOException, ParseException {
      this.field = field;
      this.key = key;
      dataType = CodecUtil.DATA_TYPE_LONG;
      this.statsItems = CodecUtil.createStatsItems(statsType);
      this.statsType = CodecUtil.createStatsType(this.statsItems, null,
          functionParser);
      if (minimumDouble != null) {
        this.minimumDouble = minimumDouble;
        this.minimumLong = minimumDouble.longValue();
      } else {
        this.minimumDouble = null;
        this.minimumLong = null;
      }
      if (maximumDouble != null) {
        this.maximumDouble = maximumDouble;
        this.maximumLong = maximumDouble.longValue();
      } else {
        this.maximumDouble = null;
        this.maximumLong = null;
      }
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_DATA, dataType, this.statsType,
          this.statsItems, null, null, null, null, null, null);
    }
  }

  /**
   * The Class ComponentToken.
   */
  public static class ComponentToken {

    /** The field. */
    public String field;

    /** The key. */
    public String key;

    /** The stats type. */
    public String dataType, statsType;

    /** The stats items. */
    public TreeSet<String> statsItems;

    /** The maximum double. */
    public Double minimumDouble, maximumDouble;

    /** The maximum long. */
    public Long minimumLong, maximumLong;

    /** The data collector. */
    public MtasDataCollector<?, ?> dataCollector;

    /** The function parser. */
    public MtasFunctionParserFunction functionParser;

    /**
     * Instantiates a new component token.
     *
     * @param field the field
     * @param key the key
     * @param minimumDouble the minimum double
     * @param maximumDouble the maximum double
     * @param statsType the stats type
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ParseException the parse exception
     */
    public ComponentToken(String field, String key, Double minimumDouble,
        Double maximumDouble, String statsType)
        throws IOException, ParseException {
      this.field = field;
      this.key = key;
      dataType = CodecUtil.DATA_TYPE_LONG;
      this.statsItems = CodecUtil.createStatsItems(statsType);
      this.statsType = CodecUtil.createStatsType(this.statsItems, null,
          functionParser);
      if (minimumDouble != null) {
        this.minimumDouble = minimumDouble;
        this.minimumLong = minimumDouble.longValue();
      } else {
        this.minimumDouble = null;
        this.minimumLong = null;
      }
      if (maximumDouble != null) {
        this.maximumDouble = maximumDouble;
        this.maximumLong = maximumDouble.longValue();
      } else {
        this.maximumDouble = null;
        this.maximumLong = null;
      }
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_DATA, dataType, this.statsType,
          this.statsItems, null, null, null, null, null, null);
    }
  }

  /**
   * The Class SubComponentFunction.
   */
  public static class SubComponentFunction {

    /** The type. */
    public String key, expression, type;

    /** The parser function. */
    public MtasFunctionParserFunction parserFunction;

    /** The sort direction. */
    public String statsType, dataType, sortType, sortDirection;

    /** The stats items. */
    public TreeSet<String> statsItems;

    /** The data collector. */
    public MtasDataCollector<?, ?> dataCollector;

    /**
     * Instantiates a new sub component function.
     *
     * @param collectorType the collector type
     * @param key the key
     * @param type the type
     * @param parserFunction the parser function
     * @param sortType the sort type
     * @param sortDirection the sort direction
     * @param start the start
     * @param number the number
     * @param segmentRegistration the segment registration
     * @param boundary the boundary
     * @throws ParseException the parse exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public SubComponentFunction(String collectorType, String key, String type,
        MtasFunctionParserFunction parserFunction, String sortType,
        String sortDirection, Integer start, Integer number,
        String segmentRegistration, String boundary)
        throws ParseException, IOException {
      this.key = key;
      this.expression = null;
      this.type = type;
      this.parserFunction = parserFunction;
      this.sortType = sortType;
      this.sortDirection = sortDirection;
      this.dataType = parserFunction.getType();
      this.statsItems = CodecUtil.createStatsItems(this.type);
      this.statsType = CodecUtil.createStatsType(statsItems, null,
          parserFunction);
      if (collectorType.equals(DataCollector.COLLECTOR_TYPE_LIST)) {
        dataCollector = DataCollector.getCollector(
            DataCollector.COLLECTOR_TYPE_LIST, dataType, statsType, statsItems,
            sortType, sortDirection, start, number, null, null, null, null,
            null, null, null, null, segmentRegistration, boundary);
      } else if (collectorType.equals(DataCollector.COLLECTOR_TYPE_DATA)) {
        dataCollector = DataCollector.getCollector(
            DataCollector.COLLECTOR_TYPE_DATA, dataType, statsType, statsItems,
            sortType, sortDirection, start, number, segmentRegistration,
            boundary);
      }
    }

    /**
     * Instantiates a new sub component function.
     *
     * @param collectorType the collector type
     * @param key the key
     * @param expression the expression
     * @param type the type
     * @throws ParseException the parse exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public SubComponentFunction(String collectorType, String key,
        String expression, String type) throws ParseException, IOException {
      this.key = key;
      this.expression = expression;
      this.type = type;
      this.sortType = null;
      this.sortDirection = null;
      parserFunction = new MtasFunctionParser(
          new BufferedReader(new StringReader(this.expression))).parse();
      dataType = parserFunction.getType();
      statsItems = CodecUtil.createStatsItems(this.type);
      statsType = CodecUtil.createStatsType(statsItems, null, parserFunction);
      if (collectorType.equals(DataCollector.COLLECTOR_TYPE_LIST)) {
        dataCollector = DataCollector.getCollector(
            DataCollector.COLLECTOR_TYPE_LIST, dataType, statsType, statsItems,
            sortType, sortDirection, 0, Integer.MAX_VALUE, null, null);
      } else if (collectorType.equals(DataCollector.COLLECTOR_TYPE_DATA)) {
        dataCollector = DataCollector.getCollector(
            DataCollector.COLLECTOR_TYPE_DATA, dataType, statsType, statsItems,
            sortType, sortDirection, null, null, null, null);
      }
    }
  }

  /**
   * The Class KwicToken.
   */
  public static class KwicToken {

    /** The start position. */
    public int startPosition;

    /** The end position. */
    public int endPosition;

    /** The tokens. */
    public ArrayList<MtasToken<String>> tokens;

    /**
     * Instantiates a new kwic token.
     *
     * @param match the match
     * @param tokens the tokens
     */
    public KwicToken(Match match, ArrayList<MtasToken<String>> tokens) {
      startPosition = match.startPosition;
      endPosition = match.endPosition - 1;
      this.tokens = tokens;
    }
  }

  /**
   * The Class KwicHit.
   */
  public static class KwicHit {

    /** The start position. */
    public int startPosition;

    /** The end position. */
    public int endPosition;

    /** The hits. */
    public HashMap<Integer, ArrayList<String>> hits;

    /**
     * Instantiates a new kwic hit.
     *
     * @param match the match
     * @param hits the hits
     */
    public KwicHit(Match match, HashMap<Integer, ArrayList<String>> hits) {
      startPosition = match.startPosition;
      endPosition = match.endPosition - 1;
      this.hits = hits;
    }
  }

  /**
   * The Class GroupHit.
   */
  public static class GroupHit {

    /** The hash right. */
    private int hash, hashLeft, hashHit, hashRight;
    
    /** The key right. */
    private String key, keyLeft, keyHit, keyRight;

    /** The data right. */
    public ArrayList<String>[] dataHit, dataLeft, dataRight;
    
    /** The missing right. */
    public HashSet<String>[] missingHit, missingLeft, missingRight;
    
    /** The unknown right. */
    public HashSet<String>[] unknownHit, unknownLeft, unknownRight;

    /** The key start. */
    public static String KEY_START = MtasToken.DELIMITER + "grouphit"
        + MtasToken.DELIMITER;

    /**
     * Sort.
     *
     * @param data the data
     * @return the array list
     */
    private ArrayList<MtasTreeHit<String>> sort(
        ArrayList<MtasTreeHit<String>> data) {
      Collections.sort(data, new Comparator<MtasTreeHit<String>>() {
        @Override
        public int compare(MtasTreeHit<String> hit1, MtasTreeHit<String> hit2) {
          int compare = (hit1.additionalId > hit2.additionalId) ? 1
              : ((hit1.additionalId < hit2.additionalId) ? -1 : 0);
          compare = (compare == 0)
              ? ((hit1.additionalRef > hit2.additionalRef) ? 1
                  : (hit2.additionalRef < hit2.additionalRef) ? -1 : 0)
              : compare;
          return compare;
        }
      });
      return data;
    }

    /**
     * Instantiates a new group hit.
     *
     * @param list the list
     * @param start the start
     * @param end the end
     * @param hitStart the hit start
     * @param hitEnd the hit end
     * @param group the group
     * @param knownPrefixes the known prefixes
     * @throws UnsupportedEncodingException the unsupported encoding exception
     */
    @SuppressWarnings("unchecked")
    public GroupHit(ArrayList<MtasTreeHit<String>> list, int start, int end,
        int hitStart, int hitEnd, ComponentGroup group,
        HashSet<String> knownPrefixes) throws UnsupportedEncodingException {
      // compute dimensions
      int leftRangeStart = start;
      int leftRangeEnd = Math.min(end - 1, hitStart - 1);
      int leftRangeLength = Math.max(0, 1 + leftRangeEnd - leftRangeStart);
      int hitLength = 1 + hitEnd - hitStart;
      int rightRangeStart = Math.max(start, hitEnd + 1);
      int rightRangeEnd = end - 1;
      int rightRangeLength = Math.max(0, 1 + rightRangeEnd - rightRangeStart);
      // create initial arrays
      if (leftRangeLength > 0) {
        keyLeft = "";
        dataLeft = (ArrayList<String>[]) new ArrayList[leftRangeLength];
        missingLeft = (HashSet<String>[]) new HashSet[leftRangeLength];
        unknownLeft = (HashSet<String>[]) new HashSet[leftRangeLength];
        for (int p = 0; p < leftRangeLength; p++) {
          dataLeft[p] = new ArrayList<String>();
          missingLeft[p] = new HashSet<String>();
          unknownLeft[p] = new HashSet<String>();
        }
      } else {
        keyLeft = null;
        dataLeft = null;
        missingLeft = null;
        unknownLeft = null;
      }
      if (hitLength > 0) {
        keyHit = "";
        dataHit = (ArrayList<String>[]) new ArrayList[hitLength];
        missingHit = (HashSet<String>[]) new HashSet[hitLength];
        unknownHit = (HashSet<String>[]) new HashSet[hitLength];
        for (int p = 0; p < hitLength; p++) {
          dataHit[p] = new ArrayList<String>();
          missingHit[p] = new HashSet<String>();
          unknownHit[p] = new HashSet<String>();
        }
      } else {
        keyHit = null;
        dataHit = null;
        missingHit = null;
        unknownHit = null;
      }
      if (rightRangeLength > 0) {
        keyRight = "";
        dataRight = (ArrayList<String>[]) new ArrayList[rightRangeLength];
        missingRight = (HashSet<String>[]) new HashSet[rightRangeLength];
        unknownRight = (HashSet<String>[]) new HashSet[rightRangeLength];
        for (int p = 0; p < rightRangeLength; p++) {
          dataRight[p] = new ArrayList<String>();
          missingRight[p] = new HashSet<String>();
          unknownRight[p] = new HashSet<String>();
        }
      } else {
        keyRight = null;
        dataRight = null;
        missingRight = null;
        unknownRight = null;
      }

      // construct missing sets
      if (group.hitInside != null) {
        for (int p = hitStart; p <= hitEnd; p++) {
          missingHit[p - hitStart].addAll(group.hitInside);
        }
      }
      if (group.hitInsideLeft != null) {
        for (int p = hitStart; p <= Math.min(hitEnd,
            hitStart + group.hitInsideLeft.length - 1); p++) {
          missingHit[p - hitStart].addAll(group.hitInsideLeft[p - hitStart]);
        }
      }
      if (group.hitLeft != null) {
        for (int p = hitStart; p <= Math.min(hitEnd,
            hitStart + group.hitLeft.length - 1); p++) {
          missingHit[p - hitStart].addAll(group.hitLeft[p - hitStart]);
        }
      }
      if (group.hitInsideRight != null) {
        for (int p = Math.max(hitStart,
            hitEnd - group.hitInsideRight.length + 1); p <= hitEnd; p++) {
          missingHit[p - hitStart].addAll(group.hitInsideRight[p - hitStart]);
        }
      }
      if (group.hitRight != null) {
        for (int p = hitStart; p <= Math.min(hitEnd,
            hitStart + group.hitRight.length - 1); p++) {
          missingHit[p - hitStart].addAll(group.hitRight[p - hitStart]);
        }
      }
      if (group.left != null) {
        for (int p = 0; p < Math.min(leftRangeLength, group.left.length); p++) {
          missingLeft[p].addAll(group.left[p]);
        }
      }
      if (group.hitRight != null) {
        for (int p = 0; p <= Math.min(leftRangeLength,
            group.hitRight.length - dataHit.length); p++) {
          missingLeft[p].addAll(group.hitRight[p + dataHit.length]);
        }
      }
      if (group.right != null) {
        for (int p = 0; p < Math.min(rightRangeLength,
            group.right.length); p++) {
          missingRight[p].addAll(group.right[p]);
        }
      }
      if (group.hitRight != null) {
        for (int p = 0; p <= Math.min(rightRangeLength,
            group.hitLeft.length - dataHit.length); p++) {
          missingRight[p].addAll(group.hitLeft[p + dataHit.length]);
        }
      }

      // fill arrays and update missing administration
      ArrayList<MtasTreeHit<String>> sortedList = sort(list);
      for (MtasTreeHit<String> hit : sortedList) {
        // inside hit
        if (group.hitInside != null && hit.idData != null
            && group.hitInside.contains(hit.idData)) {
          for (int p = Math.max(hitStart, hit.startPosition); p <= Math
              .min(hitEnd, hit.endPosition); p++) {
            // keyHit += hit.refData;
            dataHit[p - hitStart].add(hit.refData);
            missingHit[p - hitStart]
                .remove(MtasToken.getPrefixFromValue(hit.refData));
            // System.out.print(p + "." + hit.idData + ":" + hit.refData +
            // "\t");
          }
        } else if ((group.hitInsideLeft != null || group.hitLeft != null
            || group.hitInsideRight != null || group.hitRight != null)
            && hit.idData != null) {
          for (int p = Math.max(hitStart, hit.startPosition); p <= Math
              .min(hitEnd, hit.endPosition); p++) {
            int pHitLeft = p - hitStart;
            int pHitRight = hitEnd - p;
            if (group.hitInsideLeft != null
                && pHitLeft <= (group.hitInsideLeft.length - 1)
                && group.hitInsideLeft[pHitLeft] != null
                && group.hitInsideLeft[pHitLeft].contains(hit.idData)) {
              // keyHit += hit.refData;
              dataHit[p - hitStart].add(hit.refData);
              missingHit[p - hitStart]
                  .remove(MtasToken.getPrefixFromValue(hit.refData));
              // System.out.print(p+"."+hit.idData + ":" + hit.additionalRef +
              // "\t");
            } else if (group.hitLeft != null
                && pHitLeft <= (group.hitLeft.length - 1)
                && group.hitLeft[pHitLeft] != null
                && group.hitLeft[pHitLeft].contains(hit.idData)) {
              // keyHit += hit.refData;
              dataHit[p - hitStart].add(hit.refData);
              missingHit[p - hitStart]
                  .remove(MtasToken.getPrefixFromValue(hit.refData));
              // System.out.print(p+"."+hit.idData + ":" + hit.additionalRef +
              // "\t");
            } else if (group.hitInsideRight != null
                && pHitRight <= (group.hitInsideRight.length - 1)
                && group.hitInsideRight[pHitRight] != null
                && group.hitInsideRight[pHitRight].contains(hit.idData)) {
              // keyHit += hit.refData;
              dataHit[p - hitStart].add(hit.refData);
              missingHit[p - hitStart]
                  .remove(MtasToken.getPrefixFromValue(hit.refData));
              // System.out.print(p+"."+hit.idData + ":" + hit.additionalRef +
              // "\t");
            } else if (group.hitRight != null
                && pHitRight <= (group.hitRight.length - 1)
                && group.hitRight[pHitRight] != null
                && group.hitRight[pHitRight].contains(hit.idData)) {
              // keyHit += hit.refData;
              dataHit[p - hitStart].add(hit.refData);
              missingHit[p - hitStart]
                  .remove(MtasToken.getPrefixFromValue(hit.refData));
              // System.out.print(p+"."+hit.idData + ":" + hit.additionalRef +
              // "\t");
            }
          }
        }
        // left
        if (hit.startPosition < hitStart) {
          if ((group.left != null || (group.hitRight != null
              && group.hitRight.length > (1 + hitEnd - hitStart)))
              && hit.idData != null) {
            for (int p = Math.min(hit.endPosition,
                hitStart - 1); p >= hit.startPosition; p--) {
              int pLeft = hitStart - 1 - p;
              int pHitRight = hitEnd - p;
              if (group.left != null && pLeft <= (group.left.length - 1)
                  && group.left[pLeft] != null
                  && group.left[pLeft].contains(hit.idData)) {
                dataLeft[p - leftRangeStart].add(hit.refData);
                missingLeft[p - leftRangeStart]
                    .remove(MtasToken.getPrefixFromValue(hit.refData));
                // System.out.print("L"+p+"."+prefix + ":" + value + "\t");
              } else if (group.hitRight != null
                  && pHitRight <= (group.hitRight.length - 1)
                  && group.hitRight[pHitRight] != null
                  && group.hitRight[pHitRight].contains(hit.idData)) {
                dataLeft[p - leftRangeStart].add(hit.refData);
                missingLeft[p - leftRangeStart]
                    .remove(MtasToken.getPrefixFromValue(hit.refData));
                // System.out.print("L"+p+"."+prefix + ":" + value + "\t");
              }
            }
          }
        }
        // right
        if (hit.endPosition > hitEnd) {
          if ((group.right != null || (group.hitLeft != null
              && group.hitLeft.length > (1 + hitEnd - hitStart)))
              && hit.idData != null) {
            for (int p = Math.max(hit.startPosition,
                hitEnd + 1); p <= hit.endPosition; p++) {
              int pRight = p - hitEnd - 1;
              int pHitLeft = p - hitStart;
              if (group.right != null && pRight <= (group.right.length - 1)
                  && group.right[pRight] != null
                  && group.right[pRight].contains(hit.idData)) {
                dataRight[p - rightRangeStart].add(hit.refData);
                missingRight[p - rightRangeStart]
                    .remove(MtasToken.getPrefixFromValue(hit.refData));
                // System.out.print("R"+p+"."+prefix + ":" + value + "\t");
              } else if (group.hitLeft != null
                  && pHitLeft <= (group.hitLeft.length - 1)
                  && group.hitLeft[pHitLeft] != null
                  && group.hitLeft[pHitLeft].contains(hit.idData)) {
                dataRight[p - rightRangeStart].add(hit.refData);
                missingRight[p - rightRangeStart]
                    .remove(MtasToken.getPrefixFromValue(hit.refData));
                // System.out.print("R"+p+"."+prefix + ":" + value + "\t");
              }
            }
          }
        }
      }
      // register unknown
      if (missingLeft != null) {
        for (int i = 0; i < missingLeft.length; i++) {
          for (String prefix : missingLeft[i]) {
            if (!knownPrefixes.contains(prefix)) {
              unknownLeft[i].add(prefix);
            }
          }
        }
      }
      if (missingHit != null) {
        for (int i = 0; i < missingHit.length; i++) {
          for (String prefix : missingHit[i]) {
            if (!knownPrefixes.contains(prefix)) {
              unknownHit[i].add(prefix);
            }
          }
        }
      }
      if (missingRight != null) {
        for (int i = 0; i < missingRight.length; i++) {
          for (String prefix : missingRight[i]) {
            if (!knownPrefixes.contains(prefix)) {
              unknownRight[i].add(prefix);
            }
          }
        }
      }
      // construct keys
      keyLeft = dataToString(dataLeft, missingLeft);
      keyHit = dataToString(dataHit, missingHit);
      keyRight = dataToString(dataRight, missingRight);
      key = KEY_START;
      if (keyLeft != null) {
        key += keyLeft;
        hashLeft = keyLeft.hashCode();
      } else {
        hashLeft = 1;
      }
      key += "|";
      if (keyHit != null) {
        key += keyHit;
        hashHit = keyHit.hashCode();
      } else {
        hashHit = 1;
      }
      key += "|";
      if (keyRight != null) {
        key += keyRight;
        hashRight = keyRight.hashCode();
      } else {
        hashRight = 1;
      }
      // compute hash
      hash = hashHit * (hashLeft ^ 3) * (hashRight ^ 5);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      return hash;
    }

    /**
     * Data equals.
     *
     * @param d1 the d1
     * @param d2 the d2
     * @return true, if successful
     */
    private boolean dataEquals(ArrayList<String>[] d1, ArrayList<String>[] d2) {
      ArrayList<String> a1, a2;
      if (d1 == null && d2 == null) {
        return true;
      } else if (d1 == null || d2 == null) {
        return false;
      } else {
        if (d1.length == d2.length) {
          for (int i = 0; i < d1.length; i++) {
            a1 = d1[i];
            a2 = d2[i];
            if (a1 != null && a2 != null && a1.size() == a2.size()) {
              for (int j = 0; j < a1.size(); j++) {
                if (!a1.get(j).equals(a2.get(j))) {
                  return false;
                }
              }
            } else {
              return false;
            }
          }
          return true;
        } else {
          return false;
        }
      }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      GroupHit other = (GroupHit) obj;
      if (hashCode() != other.hashCode())
        return false;
      if (!dataEquals(dataHit, other.dataHit))
        return false;
      if (!dataEquals(dataLeft, other.dataLeft))
        return false;
      if (!dataEquals(dataRight, other.dataRight))
        return false;
      return true;
    }

    /**
     * Data to string.
     *
     * @param data the data
     * @param missing the missing
     * @return the string
     * @throws UnsupportedEncodingException the unsupported encoding exception
     */
    private String dataToString(ArrayList<String>[] data,
        HashSet<String>[] missing) throws UnsupportedEncodingException {
      String text = "";
      Encoder encoder = Base64.getEncoder();
      String prefix, postfix;
      if (data != null && missing != null && data.length == missing.length) {
        for (int i = 0; i < data.length; i++) {
          if (i > 0) {
            text += ",";
          }
          for (int j = 0; j < data[i].size(); j++) {
            if (j > 0) {
              text += "&";
            }
            prefix = MtasToken.getPrefixFromValue(data[i].get(j));
            postfix = MtasToken.getPostfixFromValue(data[i].get(j));
            text += encoder.encodeToString(prefix.getBytes("utf-8"));
            if (postfix != "") {
              text += ".";
              text += encoder.encodeToString(postfix.getBytes("utf-8"));
            }
          }
          if (missing[i] != null) {
            String[] tmpMissing = missing[i]
                .toArray(new String[missing[i].size()]);
            for (int j = 0; j < tmpMissing.length; j++) {
              if (j > 0 || data[i].size() > 0) {
                text += "&";
              }
              text += encoder.encodeToString(("!" + tmpMissing[j]).getBytes());
            }
          }
        }
      }
      return text;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public String toString() {
      return key;
    }

    /**
     * Key to sub sub object.
     *
     * @param key the key
     * @param newKey the new key
     * @return the hash map[]
     */
    private static HashMap[] keyToSubSubObject(String key,
        StringBuilder newKey) {
      if (key != "") {
        newKey.append(" [");
        String prefix, postfix, parts[] = key.split(Pattern.quote("&"));
        HashMap[] result = new HashMap[parts.length];
        Pattern pattern = Pattern.compile("^([^\\.]*)\\.([^\\.]*)$");
        Decoder decoder = Base64.getDecoder();
        Matcher matcher;
        String tmpNewKey = null;
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].equals("")) {
            result[i] = null;
          } else {
            HashMap<String, String> subResult = new HashMap<String, String>();
            matcher = pattern.matcher(parts[i]);
            if (tmpNewKey == null) {
              tmpNewKey = "";
            } else {
              tmpNewKey += " & ";
            }
            if (matcher.matches()) {
              prefix = new String(decoder.decode(matcher.group(1)));
              postfix = new String(decoder.decode(matcher.group(2)));
              tmpNewKey += prefix.replace("=", "\\=");
              tmpNewKey += "=\"" + postfix.replace("\"", "\\\"") + "\"";
              subResult.put("prefix", prefix);
              subResult.put("value", postfix);
            } else {
              prefix = new String(decoder.decode(parts[i]));
              tmpNewKey += prefix.replace("=", "\\=");
              if (prefix.startsWith("!")) {
                subResult.put("missing", prefix.substring(1));
              } else {
                subResult.put("prefix", prefix);
              }
            }
            result[i] = subResult;
          }
        }
        if (tmpNewKey != null) {
          newKey.append(tmpNewKey);
        }
        newKey.append("]");
        return result;
      } else {
        newKey.append(" []");
        return null;
      }
    }

    /**
     * Key to sub object.
     *
     * @param key the key
     * @param newKey the new key
     * @return the hash map
     */
    private static HashMap keyToSubObject(String key, StringBuilder newKey) {
      HashMap<Integer, HashMap[]> result = new HashMap<Integer, HashMap[]>();
      if (key == null || key.trim().equals("")) {
        return null;
      } else {
        String parts[] = key.split(Pattern.quote(","), -1);
        if (parts.length > 0) {
          for (int i = 0; i < parts.length; i++) {
            result.put(i, keyToSubSubObject(parts[i].trim(), newKey));
          }
          return result;
        } else {
          return null;
        }
      }
    }

    /**
     * Key to object.
     *
     * @param key the key
     * @param newKey the new key
     * @return the hash map
     */
    public static HashMap keyToObject(String key, StringBuilder newKey) {
      if (key.startsWith(KEY_START)) {
        String content = key.substring(KEY_START.length());
        StringBuilder keyLeft = new StringBuilder(""),
            keyHit = new StringBuilder(""), keyRight = new StringBuilder("");
        HashMap<String, HashMap<Integer, HashMap[]>> result = new HashMap<String, HashMap<Integer, HashMap[]>>();
        HashMap<Integer, HashMap[]> resultLeft = null, resultHit = null,
            resultRight = null;
        String[] parts = content.split(Pattern.quote("|"), -1);
        if (parts.length == 3) {
          resultLeft = keyToSubObject(parts[0].trim(), keyLeft);
          resultHit = keyToSubObject(parts[1].trim(), keyHit);
          if (parts.length > 2) {
            resultRight = keyToSubObject(parts[2].trim(), keyRight);
          }
        } else if (parts.length == 1) {
          resultHit = keyToSubObject(parts[0].trim(), keyHit);
        }
        if (resultLeft != null) {
          result.put("left", resultLeft);
        }
        result.put("hit", resultHit);
        if (resultRight != null) {
          result.put("right", resultRight);
        }
        newKey.append(keyLeft);
        newKey.append(" |");
        newKey.append(keyHit);
        newKey.append(" |");
        newKey.append(keyRight);
        return result;
      } else {
        return null;
      }
    }

  }

  /**
   * The Class ListToken.
   */
  public static class ListToken {

    /** The doc position. */
    public Integer docId, docPosition;

    /** The end position. */
    public int startPosition, endPosition;

    /** The tokens. */
    public ArrayList<MtasToken<String>> tokens;

    /**
     * Instantiates a new list token.
     *
     * @param docId the doc id
     * @param docPosition the doc position
     * @param match the match
     * @param tokens the tokens
     */
    public ListToken(Integer docId, Integer docPosition, Match match,
        ArrayList<MtasToken<String>> tokens) {
      this.docId = docId;
      this.docPosition = docPosition;
      startPosition = match.startPosition;
      endPosition = match.endPosition - 1;
      this.tokens = tokens;
    }
  }

  /**
   * The Class ListHit.
   */
  public static class ListHit {

    /** The doc position. */
    public Integer docId, docPosition;

    /** The end position. */
    public int startPosition, endPosition;

    /** The hits. */
    public HashMap<Integer, ArrayList<String>> hits;

    /**
     * Instantiates a new list hit.
     *
     * @param docId the doc id
     * @param docPosition the doc position
     * @param match the match
     * @param hits the hits
     */
    public ListHit(Integer docId, Integer docPosition, Match match,
        HashMap<Integer, ArrayList<String>> hits) {
      this.docId = docId;
      this.docPosition = docPosition;
      startPosition = match.startPosition;
      endPosition = match.endPosition - 1;
      this.hits = hits;
    }
  }

  /**
   * The Class Match.
   */
  public static class Match {

    /** The start position. */
    public int startPosition;

    /** The end position. */
    public int endPosition;

    /**
     * Instantiates a new match.
     *
     * @param startPosition the start position
     * @param endPosition the end position
     */
    public Match(int startPosition, int endPosition) {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object object) {
      if (this.getClass().equals(object.getClass())) {
        if ((((Match) object).startPosition == startPosition)
            && (((Match) object).endPosition == endPosition)) {
          return true;
        }
      }
      return false;
    }
  }

}
