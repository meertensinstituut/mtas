package mtas.codec.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
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
import mtas.codec.util.DataCollector.MtasDataCollector;
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
/**
 * @author matthijs
 *
 */
/**
 * @author matthijs
 *
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

    /**
     * Instantiates a new component prefix.
     *
     * @param key the key
     */
    public ComponentPrefix(String key) {
      this.key = key;
      singlePositionList = new TreeSet<String>();
      multiplePositionList = new TreeSet<String>();
    }

    /**
     * Adds the single position.
     *
     * @param prefix the prefix
     */
    public void addSinglePosition(String prefix) {
      if (!singlePositionList.contains(prefix)
          && !multiplePositionList.contains(prefix)) {
        singlePositionList.add(prefix);
      }
    }

    /**
     * Adds the multiple position.
     *
     * @param prefix the prefix
     */
    public void addMultiplePosition(String prefix) {
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
        String queryType, String key, String groupingHitInsidePrefixes,
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
      this.number = 10;
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
          this.number, null, null, null, null, null, null, null, null, false);
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

    /** The base numbers. */
    public Integer[] baseNumbers;

    /** The base maximum longs. */
    public Long[] baseMinimumLongs, baseMaximumLongs;

    /** The base maximum doubles. */
    public Double[] baseMinimumDoubles, baseMaximumDoubles;

    /** The base function parsers. */
    public MtasFunctionParserFunction[] baseFunctionParsers;

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
     * @param baseFunctions the base functions
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ParseException the parse exception
     */
    public ComponentFacet(SpanQuery[] spanQueries, String field, String key,
        String[] baseFields, String[] baseFieldTypes, String[] baseTypes,
        String[] baseSortTypes, String[] baseSortDirections,
        Integer[] baseNumbers, Double[] baseMinimumDoubles,
        Double[] baseMaximumDoubles, String[] baseFunctions)
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
      this.baseFunctionParsers = new MtasFunctionParserFunction[baseFields.length];
      this.baseDataTypes = new String[baseFields.length];
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
        if (baseFunctions[i] != null) {
          baseFunctionParsers[i] = new MtasFunctionParser(
              new BufferedReader(new StringReader(baseFunctions[i]))).parse();
        } else {
          baseFunctionParsers[i] = new MtasFunctionParserFunctionDefault(
              spanQueries.length);
        }
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
        this.baseDataTypes[i] = baseFunctionParsers[i].getType();
        this.baseCollectorTypes[i] = DataCollector.COLLECTOR_TYPE_LIST;
        this.baseStatsItems[i] = CodecUtil.createStatsItems(baseTypes[i]);
        this.baseStatsTypes[i] = CodecUtil.createStatsType(baseStatsItems[i],
            this.baseSortTypes[i], this.baseFunctionParsers[i]);
      }
      if (baseFields.length > 0) {
        if (baseFields.length == 1) {
          dataCollector = DataCollector.getCollector(this.baseCollectorTypes[0],
              this.baseDataTypes[0], this.baseStatsTypes[0],
              this.baseStatsItems[0], this.baseSortTypes[0],
              this.baseSortDirections[0], 0, this.baseNumbers[0], null, null,
              null, null, null, null, null, null, false);
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
              subStarts, subNumbers, false);
        }
      } else {
        throw new IOException("no baseFields");
      }
    }

  }

  /**
   * The Class ComponentTermVector.
   */
  public static class ComponentTermVector {

    /** The sort direction. */
    public String key, defaultDataType, functionDataType, defaultStatsType,
        functionStatsType, prefix, regexp, sortType, sortDirection;

    /** The default stats items. */
    public TreeSet<String> functionStatsItems, defaultStatsItems;

    /** The number. */
    public int start, number;

    /** The start value. */
    public String startValue;

    /** The compiled automaton. */
    public CompiledAutomaton compiledAutomaton;

    /** The data function collector. */
    public MtasDataCollector<?, ?> dataFunctionCollector;

    /** The data default collector. */
    public MtasDataCollector<?, ?> dataDefaultCollector;

    /** The function parser. */
    public MtasFunctionParserFunction functionParser;

    /** The default parser. */
    public MtasFunctionParserFunction defaultParser;

    /**
     * Instantiates a new component term vector.
     *
     * @param key the key
     * @param prefix the prefix
     * @param regexp the regexp
     * @param statsType the stats type
     * @param sortType the sort type
     * @param sortDirection the sort direction
     * @param startValue the start value
     * @param number the number
     * @param function the function
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ParseException the parse exception
     */
    public ComponentTermVector(String key, String prefix, String regexp,
        String statsType, String sortType, String sortDirection,
        String startValue, int number, String function)
        throws IOException, ParseException {
      this.key = key;      
      this.defaultParser = new MtasFunctionParserFunctionDefault(1);
      if (function != null) {
        functionParser = new MtasFunctionParser(
            new BufferedReader(new StringReader(function))).parse();
        functionDataType = functionParser.getType();
        functionStatsItems = CodecUtil.createStatsItems(statsType);
        functionStatsType = CodecUtil.createStatsType(
            this.functionStatsItems, this.sortType, this.functionParser);
        defaultDataType = defaultParser.getType();
        defaultStatsItems = CodecUtil.createStatsItems(null);
        defaultStatsType = CodecUtil.createStatsType(
            this.defaultStatsItems, this.sortType, this.defaultParser);
      } else {
        functionParser = null;
        functionDataType = null;
        functionStatsItems = null;
        functionStatsType = null;
        defaultDataType = defaultParser.getType();
        defaultStatsItems = CodecUtil.createStatsItems(statsType);
        defaultStatsType = CodecUtil.createStatsType(
            this.defaultStatsItems, this.sortType, this.defaultParser);
      }
      this.prefix = prefix;
      this.regexp = regexp;
      this.sortType = sortType == null ? CodecUtil.SORT_TERM : sortType;
      this.sortDirection = sortDirection == null
          ? (this.sortType == CodecUtil.SORT_TERM) ? CodecUtil.SORT_ASC
              : CodecUtil.SORT_DESC
          : sortDirection;
      this.startValue = startValue;
      this.start = 0;
      this.number = number;      
      if (!this.sortType.equals(CodecUtil.SORT_TERM)
          && !CodecUtil.STATS_TYPES.contains(this.sortType)) {
        throw new IOException("unknown sortType '" + this.sortType + "'");
      } else if (!this.sortType.equals(CodecUtil.SORT_TERM)) {
        if (!(this.sortType.equals(CodecUtil.STATS_TYPE_SUM)
            || this.sortType.equals(CodecUtil.STATS_TYPE_N))) {
          throw new IOException(
              "sortType '" + this.sortType + "' not supported for termVector");
        }
      }
      if (!this.sortDirection.equals(CodecUtil.SORT_ASC)
          && !this.sortDirection.equals(CodecUtil.SORT_DESC)) {
        throw new IOException(
            "unrecognized sortDirection '" + this.sortDirection + "'");
      }
      boolean segmentRegistration = !this.sortType.equals(CodecUtil.SORT_TERM);
      //create datacollectors
      if (functionParser != null) {
        dataFunctionCollector = DataCollector.getCollector(
            DataCollector.COLLECTOR_TYPE_LIST, this.functionDataType,
            this.functionStatsType, this.functionStatsItems, this.sortType,
            this.sortDirection, this.start, this.number, null, null, null, null,
            null, null, null, null, false);
        dataDefaultCollector = DataCollector.getCollector(
            DataCollector.COLLECTOR_TYPE_LIST, this.defaultDataType,
            this.defaultStatsType, this.defaultStatsItems, this.sortType,
            this.sortDirection, this.start, this.number, null, null, null, null,
            null, null, null, null, segmentRegistration);
      } else {
        dataFunctionCollector = null;
        dataDefaultCollector = DataCollector.getCollector(
            DataCollector.COLLECTOR_TYPE_LIST, this.defaultDataType,
            this.defaultStatsType, this.defaultStatsItems, this.sortType,
            this.sortDirection, this.start, this.number, null, null, null, null,
            null, null, null, null, segmentRegistration);
      }
      if ((regexp == null) || (regexp.isEmpty())) {
        RegExp re = new RegExp(prefix + MtasToken.DELIMITER + ".*");
        compiledAutomaton = new CompiledAutomaton(re.toAutomaton());
      } else {
        RegExp re = new RegExp(
            prefix + MtasToken.DELIMITER + regexp + "\u0000*");
        compiledAutomaton = new CompiledAutomaton(re.toAutomaton());
      }
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

    /** The function parser. */
    public MtasFunctionParserFunction functionParser;

    /**
     * Instantiates a new component span.
     *
     * @param queries the queries
     * @param key the key
     * @param minimumDouble the minimum double
     * @param maximumDouble the maximum double
     * @param type the type
     * @param function the function
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public ComponentSpan(SpanQuery[] queries, String key, Double minimumDouble,
        Double maximumDouble, String type, String function) throws IOException {
      this.queries = queries;
      this.key = key;
      if (function != null) {
        try {
          functionParser = new MtasFunctionParser(
              new BufferedReader(new StringReader(function))).parse();
        } catch (ParseException e) {
          throw new IOException("couldn't parse function " + function);
        }
      } else {
        functionParser = new MtasFunctionParserFunctionDefault(queries.length);
      }
      dataType = functionParser.getType();
      statsItems = CodecUtil.createStatsItems(type);
      statsType = CodecUtil.createStatsType(this.statsItems, null,
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
          this.statsItems, null, null, null, null, null, null, null, null, null,
          null, null, null, false);
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
          this.statsItems, null, null, null, null, null, null, null, null, null,
          null, null, null, false);
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
          this.statsItems, null, null, null, null, null, null, null, null, null,
          null, null, null, false);
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

    /** The key. */
    public String key;

    /** The data right. */
    public ArrayList<String>[] dataHit, dataLeft, dataRight;

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
          return hit1.data.compareTo(hit2.data);
        }
      });
      return data;
    }

    /**
     * Creates the hash.
     *
     * @param data the data
     * @return the string
     */
    private String createHash(ArrayList<MtasTreeHit<String>> data) {
      String hash = "";
      for (MtasTreeHit<String> item : data) {
        hash += CodecUtil.termValue(item.data) + " ";
      }
      hash = hash.trim();
      return hash;
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
     */
    public GroupHit(ArrayList<MtasTreeHit<String>> list, int start, int end,
        int hitStart, int hitEnd, ComponentGroup group) {
      int leftRangeStart = start;
      int leftRangeEnd = Math.min(end, hitStart - 1);
      int leftRange = 1 + leftRangeEnd - leftRangeStart;
      int hitRangeStart = Math.max(hitStart, start);
      int hitRangeEnd = Math.min(hitEnd, end);
      int hitRange = 1 + hitRangeEnd - hitRangeStart;
      int rightRangeStart = Math.max(start, hitEnd + 1);
      int rightRangeEnd = end;
      int rightRange = 1 + rightRangeEnd - rightRangeStart;
      // System.out.print(start+"-"+end+"\t"+hitStart+"-"+hitEnd+"\t");
      // if(leftRange>0) {
      // System.out.print("L:"+leftRangeStart+"-"+leftRangeEnd+"\t");
      // }
      // if(hitRange>0) {
      // System.out.print("H:"+hitRangeStart+"-"+hitRangeEnd+"\t");
      // }
      // if(rightRange>0) {
      // System.out.print("R:"+rightRangeStart+"-"+rightRangeEnd+"\t");
      // }
      // System.out.println();
      if (leftRange > 0) {
        dataLeft = (ArrayList<String>[]) new ArrayList[leftRange];
        for (int p = 0; p < leftRange; p++) {
          dataLeft[p] = new ArrayList();
        }
      } else {
        dataLeft = null;
      }
      if (hitRange > 0) {
        dataHit = (ArrayList<String>[]) new ArrayList[hitRange];
        for (int p = 0; p < hitRange; p++) {
          dataHit[p] = new ArrayList();
        }
      } else {
        dataHit = null;
      }
      if (rightRange > 0) {
        dataRight = (ArrayList<String>[]) new ArrayList[rightRange];
        for (int p = 0; p < rightRange; p++) {
          dataRight[p] = new ArrayList();
        }
      } else {
        dataRight = null;
      }
      ArrayList<MtasTreeHit<String>> sortedList = sort(list);
      for (MtasTreeHit<String> hit : sortedList) {
        String prefix = CodecUtil.termPrefix(hit.data);
        String value = CodecUtil.termValue(hit.data);
        List<String> positionList;
        // inside hit
        if (group.hitInside != null && group.hitInside.contains(prefix)) {
          for (int p = Math.max(hitStart, hit.startPosition); p <= Math
              .min(hitEnd, hit.endPosition); p++) {
            dataHit[p - hitRangeStart].add(CodecUtil.termPrefixValue(hit.data));
            // System.out.print(p+"."+prefix + ":" + value + "\t");
          }
        } else if (group.hitInsideLeft != null || group.hitLeft != null
            || group.hitInsideRight != null || group.hitRight != null) {
          for (int p = Math.max(hitStart, hit.startPosition); p <= Math
              .min(hitEnd, hit.endPosition); p++) {
            int pHitLeft = p - hitStart;
            int pHitRight = hitEnd - p;
            if (group.hitInsideLeft != null
                && pHitLeft <= (group.hitInsideLeft.length - 1)
                && group.hitInsideLeft[pHitLeft].contains(prefix)) {
              dataHit[p - hitRangeStart]
                  .add(CodecUtil.termPrefixValue(hit.data));
              // System.out.print(p+"."+prefix + ":" + value + "\t");
            } else if (group.hitLeft != null
                && pHitLeft <= (group.hitLeft.length - 1)
                && group.hitLeft[pHitLeft].contains(prefix)) {
              dataHit[p - hitRangeStart]
                  .add(CodecUtil.termPrefixValue(hit.data));
              // System.out.print(p+"."+prefix + ":" + value + "\t");
            } else if (group.hitInsideRight != null
                && pHitRight <= (group.hitInsideRight.length - 1)
                && group.hitInsideRight[pHitRight].contains(prefix)) {
              dataHit[p - hitRangeStart]
                  .add(CodecUtil.termPrefixValue(hit.data));
              // System.out.print(p+"."+prefix + ":" + value + "\t");
            } else if (group.hitRight != null
                && pHitRight <= (group.hitRight.length - 1)
                && group.hitRight[pHitRight].contains(prefix)) {
              dataHit[p - hitRangeStart]
                  .add(CodecUtil.termPrefixValue(hit.data));
              // System.out.print(p+"."+prefix + ":" + value + "\t");
            }
          }
        }
        // left
        if (hit.startPosition < hitStart) {
          if (group.left != null || (group.hitRight != null
              && group.hitRight.length > (1 + hitEnd - hitStart))) {
            for (int p = Math.min(hit.endPosition,
                hitStart - 1); p >= hit.startPosition; p--) {
              int pLeft = hitStart - 1 - p;
              int pHitRight = hitEnd - p;
              if (group.left != null && pLeft <= (group.left.length - 1)
                  && group.left[pLeft].contains(prefix)) {
                dataLeft[p - leftRangeStart]
                    .add(CodecUtil.termPrefixValue(hit.data));
                // System.out.print("L"+p+"."+prefix + ":" + value + "\t");
              } else if (group.hitRight != null
                  && pHitRight <= (group.hitRight.length - 1)
                  && group.hitRight[pHitRight].contains(prefix)) {
                dataLeft[p - leftRangeStart]
                    .add(CodecUtil.termPrefixValue(hit.data));
                // System.out.print("L"+p+"."+prefix + ":" + value + "\t");
              }
            }
          }
        }
        // right
        if (hit.endPosition > hitEnd) {
          if (group.right != null || (group.hitLeft != null
              && group.hitLeft.length > (1 + hitEnd - hitStart))) {
            for (int p = Math.max(hit.startPosition,
                hitEnd + 1); p <= hit.endPosition; p++) {
              int pRight = p - hitEnd - 1;
              int pHitLeft = p - hitStart;
              if (group.right != null && pRight <= (group.right.length - 1)
                  && group.right[pRight].contains(prefix)) {
                dataRight[p - rightRangeStart]
                    .add(CodecUtil.termPrefixValue(hit.data));
                // System.out.print("R"+p+"."+prefix + ":" + value + "\t");
              } else if (group.hitLeft != null
                  && pHitLeft <= (group.hitLeft.length - 1)
                  && group.hitLeft[pHitLeft].contains(prefix)) {
                dataRight[p - rightRangeStart]
                    .add(CodecUtil.termPrefixValue(hit.data));
                // System.out.print("R"+p+"."+prefix + ":" + value + "\t");
              }
            }
          }
        }
        // compute key
        key = "";
        key += "left: " + (dataLeft != null ? dataToString(dataLeft) : null)
            + " - ";
        key += "hit: " + (dataHit != null ? dataToString(dataHit) : null)
            + " - ";
        key += "right: " + (dataRight != null ? dataToString(dataRight) : null);
        key = key.trim();
      }
    }

    /**
     * Data to string.
     *
     * @param data the data
     * @return the string
     */
    private String dataToString(ArrayList<String>[] data) {
      String text = "";
      if (data != null) {
        for (int i = 0; i < data.length; i++) {
          text += (i > 0 ? ", " : "") + data[i].toString();
        }
      }
      return text.trim();
    }

    /**
     * Gets the key.
     *
     * @return the key
     */
    public String getKey() {
      return key;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return (key != null) ? key.replaceAll("\u0001", ":") : key;
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
