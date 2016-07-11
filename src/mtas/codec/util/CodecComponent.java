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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;

public class CodecComponent {

  /**
   * The Class ComponentFields contains
   * <ul>
   * <li>{@link ComponentField} for each field</li>
   * <li>Collector actions to be performed for this field</li>
   * </ul>
   */
  public static class ComponentFields {

    /** The list of {@link ComponentField} for each field */
    public Map<String, ComponentField> list;

    /** Do kwic. */
    public boolean doKwic;

    /** Do list. */
    public boolean doList;

    /** Do group. */
    public boolean doGroup;

    /** Do term vector. */
    public boolean doTermVector;

    /** Do stats. */
    public boolean doStats;

    /** Do stats spans. */
    public boolean doStatsSpans;

    /** Do stats positions. */
    public boolean doStatsPositions;

    /** Do stats tokens. */
    public boolean doStatsTokens;

    /** Do prefix. */
    public boolean doPrefix;
    
    
    /** Do facet. */
    public boolean doFacet;

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
   * The Class ComponentField contains for each field
   * <ul>
   * <li>unique key</li>
   * <li>list of {@link ComponentKwic}</li>
   * <li>list of {@link ComponentList}</li>
   * <li>list of {@link ComponentFacet}</li>
   * <li>list of {@link ComponentTermVector}</li>
   * <li>list of {@link ComponentPosition}</li>
   * <li>list of {@link ComponentSpan}</li>
   * <li>list of {@link SpanQuery}</li>
   * <li>{@link ComponentPrefix}</li>
   * </ul>
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

    /** The stats position list. */
    public List<ComponentToken> statsTokenList;

    /** The stats span list. */
    public List<ComponentSpan> statsSpanList;

    /** The span query list. */
    public List<SpanQuery> spanQueryList;

    /** The prefix. */
    public ComponentPrefix prefix;
    
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
   * The Class ComponentPrefix contains
   * <ul>
   * <li>List of singlePosition prefixes</li>
   * <li>List of multiplePosition prefixes</li>
   * </ul>
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
     * @param key
     *          the key
     */
    public ComponentPrefix(String key) {
      this.key = key;
      singlePositionList = new TreeSet<String>();
      multiplePositionList = new TreeSet<String>();
    }

    /**
     * Adds the single position.
     *
     * @param prefix
     *          the prefix
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
     * @param prefix
     *          the prefix
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
   * The Class ComponentKwic contains
   * <ul>
   * <li>The SpanQuery to match</li>
   * <li>A list of {@link KwicToken} if output is {@link KWIC_OUTPUT_TOKEN}</li>
   * <li>A list of {@link KwicHit} if output is {@link KWIC_OUTPUT_HIT}</li>
   * </ul>
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

    /** The left, right and start. */
    public int left, right, start;

    /** The number. */
    public Integer number;

    /** The output. */
    public String output;

    /** Provide full token information */
    public static final String KWIC_OUTPUT_TOKEN = "token";

    /** Provide basic information for the hit */
    public static final String KWIC_OUTPUT_HIT = "hit";

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
   * The Class ComponentList contains
   * <ul>
   * <li>The SpanQuery to match</li>
   * <li>A list of {@link KwicToken} if output is {@link LIST_OUTPUT_TOKEN}</li>
   * <li>A list of {@link KwicHit} if output is {@link LIST_OUTPUT_HIT}</li>
   * </ul>
   */
  public static class ComponentList {

    /** The span query. */
    public SpanQuery spanQuery;

    /** The field. */
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

    /** The left, right, total, position and start. */
    public int left, right, total, position, start;

    /** The number. */
    public int number;

    /** The prefix and output. */
    public String prefix, output;

    /** Provide full token information */
    public static final String LIST_OUTPUT_TOKEN = "token";

    /** Provide basic information for the hit */
    public static final String LIST_OUTPUT_HIT = "hit";

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

  public static class ComponentGroup {

    /** The span query. */
    public SpanQuery spanQuery;

    public String dataType, statsType, sortType, sortDirection;
    public TreeSet<String> statsItems;
    public Integer start, number;
    public String field, queryValue, queryType, key;
    public MtasDataCollector<?, ?> dataCollector;    
    ArrayList<String> prefixes;
    HashSet<String> hitInside;
    HashSet<String>[] hitInsideLeft, hitInsideRight, hitLeft, hitRight, left,
        right;

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
      //datacollector
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_LIST, this.dataType, this.statsType,
          this.statsItems, this.sortType, this.sortDirection, this.start,
          this.number, null, null, null, null, null, null, null, null);
    }

  }

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

    public SpanQuery[] spanQueries;
    public String[] baseFields, baseFieldTypes, baseTypes, baseSortTypes,
        baseSortDirections;
    public String[] baseCollectorTypes, baseDataTypes, baseStatsTypes;
    public TreeSet<String>[] baseStatsItems;

    public String key, field;
    public MtasDataCollector<?, ?> dataCollector;
    public Integer[] baseNumbers;
    public Long[] baseMinimumLongs, baseMaximumLongs;
    public Double[] baseMinimumDoubles, baseMaximumDoubles;
    public MtasFunctionParserFunction[] baseFunctionParsers;

    public static final String TYPE_INTEGER = "integer";
    public static final String TYPE_DOUBLE = "double";
    public static final String TYPE_LONG = "long";
    public static final String TYPE_FLOAT = "float";
    public static final String TYPE_STRING = "string";

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
              null, null, null, null, null, null);
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
              subStarts, subNumbers);
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

    public String key, dataType, statsType, prefix, regexp, sortType,
        sortDirection;
    public TreeSet<String> statsItems;
    public Integer start, number;
    public CompiledAutomaton compiledAutomaton;
    public MtasDataCollector<?, ?> dataCollector;
    public Double minimumDouble, maximumDouble;
    public Long minimumLong, maximumLong;
    public MtasFunctionParserFunction functionParser;

    public ComponentTermVector(String key, String prefix, String regexp,
        String statsType, String sortType, String sortDirection, Integer start,
        Integer number, Double minimumDouble, Double maximumDouble,
        String function) throws IOException, ParseException {
      this.key = key;
      if (function != null) {
        functionParser = new MtasFunctionParser(
            new BufferedReader(new StringReader(function))).parse();
      } else {
        functionParser = new MtasFunctionParserFunctionDefault(1);
      }
      dataType = functionParser.getType();
      this.prefix = prefix;
      this.regexp = regexp;
      this.sortType = sortType == null ? CodecUtil.SORT_TERM : sortType;
      this.sortDirection = sortDirection == null
          ? (this.sortType == CodecUtil.SORT_TERM) ? CodecUtil.SORT_ASC
              : CodecUtil.SORT_DESC
          : sortDirection;
      this.start = start;
      this.number = number;
      this.statsItems = CodecUtil.createStatsItems(statsType);
      this.statsType = CodecUtil.createStatsType(this.statsItems, this.sortType,
          this.functionParser);
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
      if (!this.sortType.equals(CodecUtil.SORT_TERM)
          && !CodecUtil.STATS_TYPES.contains(this.sortType)) {
        throw new IOException("unknown sortType '" + this.sortType + "'");
      }
      if (!this.sortDirection.equals(CodecUtil.SORT_ASC)
          && !this.sortDirection.equals(CodecUtil.SORT_DESC)) {
        throw new IOException(
            "unrecognized sortDirection " + this.sortDirection);
      }
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_LIST, this.dataType, this.statsType,
          this.statsItems, this.sortType, this.sortDirection, this.start,
          this.number, null, null, null, null, null, null, null, null);
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
    public SpanQuery[] queries;
    public String key;
    public String dataType;
    public String statsType;
    public TreeSet<String> statsItems;
    public Double minimumDouble, maximumDouble;
    public Long minimumLong, maximumLong;
    public MtasDataCollector<?, ?> dataCollector;
    public MtasFunctionParserFunction functionParser;

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
          null, null, null);
    }
  }

  /**
   * The Class ComponentPosition.
   */
  public static class ComponentPosition {
    public String field;
    public String key;
    public String dataType, statsType;
    public TreeSet<String> statsItems;
    public Double minimumDouble, maximumDouble;
    public Long minimumLong, maximumLong;
    public MtasDataCollector<?, ?> dataCollector;
    public MtasFunctionParserFunction functionParser;

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
          null, null, null);
    }
  }
  
  public static class ComponentToken {
    public String field;
    public String key;
    public String dataType, statsType;
    public TreeSet<String> statsItems;
    public Double minimumDouble, maximumDouble;
    public Long minimumLong, maximumLong;
    public MtasDataCollector<?, ?> dataCollector;
    public MtasFunctionParserFunction functionParser;

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
          null, null, null);
    }
  }

  /**
   * The Class KwicToken.
   */
  public static class KwicToken {
    public int startPosition;
    public int endPosition;
    public ArrayList<MtasToken<String>> tokens;

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
    public int startPosition;
    public int endPosition;
    public HashMap<Integer, ArrayList<String>> hits;

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
    public String key;
    public ArrayList<String>[] dataHit, dataLeft, dataRight;
    
    private ArrayList<MtasTreeHit<String>> sort(ArrayList<MtasTreeHit<String>> data) {
      Collections.sort(data, new Comparator<MtasTreeHit<String>>() {
        @Override
        public int compare(MtasTreeHit<String> hit1, MtasTreeHit<String> hit2) {
          return hit1.data.compareTo(hit2.data);
        }
      });
      return data;
    }

    private String createHash(ArrayList<MtasTreeHit<String>> data) {
      String hash = "";
      for (MtasTreeHit<String> item : data) {
        hash += CodecUtil.termValue(item.data) + " ";
      }
      hash = hash.trim();
      return hash;
    }

    public GroupHit(ArrayList<MtasTreeHit<String>> list, int start, int end,
        int hitStart, int hitEnd, ComponentGroup group) {
      int leftRangeStart = start;
      int leftRangeEnd = Math.min(end, hitStart-1);
      int leftRange = 1+leftRangeEnd - leftRangeStart;
      int hitRangeStart = Math.max(hitStart, start);
      int hitRangeEnd = Math.min(hitEnd, end);
      int hitRange = 1+hitRangeEnd - hitRangeStart;
      int rightRangeStart = Math.max(start, hitEnd+1);
      int rightRangeEnd = end;
      int rightRange = 1+rightRangeEnd - rightRangeStart;
//      System.out.print(start+"-"+end+"\t"+hitStart+"-"+hitEnd+"\t");
//      if(leftRange>0) {
//        System.out.print("L:"+leftRangeStart+"-"+leftRangeEnd+"\t");
//      }
//      if(hitRange>0) {
//        System.out.print("H:"+hitRangeStart+"-"+hitRangeEnd+"\t");
//      }
//      if(rightRange>0) {
//        System.out.print("R:"+rightRangeStart+"-"+rightRangeEnd+"\t");
//      }
//      System.out.println();
      if(leftRange>0) {        
        dataLeft = (ArrayList<String>[])new ArrayList[leftRange]; 
        for(int p=0; p<leftRange; p++) {
          dataLeft[p] = new ArrayList();
        }
      } else {
        dataLeft = null;
      }
      if(hitRange>0) {        
        dataHit = (ArrayList<String>[])new ArrayList[hitRange]; 
        for(int p=0; p<hitRange; p++) {
          dataHit[p] = new ArrayList();
        }
      } else {
        dataHit = null;
      }
      if(rightRange>0) {        
        dataRight = (ArrayList<String>[])new ArrayList[rightRange]; 
        for(int p=0; p<rightRange; p++) {
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
        //inside hit
        if (group.hitInside != null && group.hitInside.contains(prefix)) {
          for (int p = Math.max(hitStart, hit.startPosition); p <= Math
              .min(hitEnd, hit.endPosition); p++) {            
            dataHit[p-hitRangeStart].add(CodecUtil.termPrefixValue(hit.data));
            //System.out.print(p+"."+prefix + ":" + value + "\t");
          }
        } else if (group.hitInsideLeft!=null || group.hitLeft!=null || group.hitInsideRight!=null || group.hitRight!=null) {
          for (int p = Math.max(hitStart, hit.startPosition); p <= Math
              .min(hitEnd, hit.endPosition); p++) {
            int pHitLeft = p-hitStart;
            int pHitRight = hitEnd-p;
            if(group.hitInsideLeft!=null && pHitLeft<=(group.hitInsideLeft.length-1) && group.hitInsideLeft[pHitLeft].contains(prefix)) {
              dataHit[p-hitRangeStart].add(CodecUtil.termPrefixValue(hit.data));
              //System.out.print(p+"."+prefix + ":" + value + "\t");
            } else if(group.hitLeft!=null && pHitLeft<=(group.hitLeft.length-1) && group.hitLeft[pHitLeft].contains(prefix)) {
              dataHit[p-hitRangeStart].add(CodecUtil.termPrefixValue(hit.data));
              //System.out.print(p+"."+prefix + ":" + value + "\t");
            } else if(group.hitInsideRight!=null && pHitRight<=(group.hitInsideRight.length-1) && group.hitInsideRight[pHitRight].contains(prefix)) {
              dataHit[p-hitRangeStart].add(CodecUtil.termPrefixValue(hit.data));
              //System.out.print(p+"."+prefix + ":" + value + "\t");
            } else if(group.hitRight!=null && pHitRight<=(group.hitRight.length-1) && group.hitRight[pHitRight].contains(prefix)) {
              dataHit[p-hitRangeStart].add(CodecUtil.termPrefixValue(hit.data));
              //System.out.print(p+"."+prefix + ":" + value + "\t");
            }
          }
        }
        //left
        if(hit.startPosition<hitStart) {
          if(group.left!=null || (group.hitRight!=null && group.hitRight.length>(1+hitEnd-hitStart))) {
            for (int p = Math.min(hit.endPosition, hitStart-1); p >= hit.startPosition; p--) {  
              int pLeft = hitStart-1-p;
              int pHitRight = hitEnd-p;
              if(group.left!=null && pLeft<=(group.left.length-1) && group.left[pLeft].contains(prefix)) {
                dataLeft[p-leftRangeStart].add(CodecUtil.termPrefixValue(hit.data));
                //System.out.print("L"+p+"."+prefix + ":" + value + "\t"); 
              } else if(group.hitRight!=null && pHitRight<=(group.hitRight.length-1) && group.hitRight[pHitRight].contains(prefix)) {
                dataLeft[p-leftRangeStart].add(CodecUtil.termPrefixValue(hit.data));
                //System.out.print("L"+p+"."+prefix + ":" + value + "\t"); 
              }
            }
          }
        }
        //right
        if(hit.endPosition>hitEnd) {
          if(group.right!=null || (group.hitLeft!=null && group.hitLeft.length>(1+hitEnd-hitStart))) {
            for (int p = Math.max(hit.startPosition, hitEnd+1); p <= hit.endPosition; p++) {  
              int pRight = p-hitEnd-1;
              int pHitLeft = p-hitStart;
              if(group.right!=null && pRight<=(group.right.length-1) && group.right[pRight].contains(prefix)) {
                dataRight[p-rightRangeStart].add(CodecUtil.termPrefixValue(hit.data));
                //System.out.print("R"+p+"."+prefix + ":" + value + "\t"); 
              } else if(group.hitLeft!=null && pHitLeft<=(group.hitLeft.length-1) && group.hitLeft[pHitLeft].contains(prefix)) {
                dataRight[p-rightRangeStart].add(CodecUtil.termPrefixValue(hit.data));
                //System.out.print("R"+p+"."+prefix + ":" + value + "\t"); 
              }
            }
          }
        }
        //compute key
        key = "";
        key+="left: "+(dataLeft!=null?dataToString(dataLeft):null)+" - ";
        key+="hit: "+(dataHit!=null?dataToString(dataHit):null)+" - ";
        key+="right: "+(dataRight!=null?dataToString(dataRight):null);
        key = key.trim();
      }      
    }

    private String dataToString(ArrayList<String>[] data) {
      String text = "";
      if(data!=null) {
        for(int i=0; i<data.length; i++) {
          text+=(i>0?", ":"")+data[i].toString();
        }
      }
      return text.trim();
    }
    
    public String getKey() {
      return key;
    }
    
    @Override
    public String toString() {
      return (key!=null)?key.replaceAll("\u0001", ":"):key;
    }

  }

  /**
   * The Class ListToken.
   */
  public static class ListToken {
    public Integer docId, docPosition;
    public int startPosition, endPosition;
    public ArrayList<MtasToken<String>> tokens;

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
    public Integer docId, docPosition;
    public int startPosition, endPosition;
    public HashMap<Integer, ArrayList<String>> hits;

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
    public int startPosition;
    public int endPosition;

    public Match(int startPosition, int endPosition) {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
    }

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
