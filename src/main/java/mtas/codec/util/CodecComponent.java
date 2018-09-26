package mtas.codec.util;

import mtas.analysis.token.MtasToken;
import mtas.analysis.token.MtasTokenString;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.codec.util.distance.Distance;
import mtas.parser.function.MtasFunctionParser;
import mtas.parser.function.ParseException;
import mtas.parser.function.util.MtasFunctionParserFunction;
import mtas.parser.function.util.MtasFunctionParserFunctionDefault;
import mtas.search.spans.util.MtasSpanQuery;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.util.BytesRef;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CodecComponent {
  private CodecComponent() {
  }

  public static class ComponentFields {
    public ComponentStatus status;
    public ComponentVersion version;
    public Map<String, ComponentField> list;
    public List<ComponentCollection> collection;
    public boolean doDocument;
    public boolean doKwic;
    public boolean doList;
    public boolean doGroup;
    public boolean doTermVector;
    public boolean doStats;
    public boolean doStatsSpans;
    public boolean doStatsPositions;
    public boolean doStatsTokens;
    public boolean doPrefix;
    public boolean doFacet;
    public boolean doCollection;
    public boolean doStatus;
    public boolean doVersion;

    public ComponentFields() {
      status = null;
      list = new HashMap<>();
      collection = new ArrayList<>();
      doDocument = false;
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
      doCollection = false;
      doStatus = false;
      doVersion = false;
    }
  }

  public interface BasicComponent {
  }

  public static class ComponentField implements BasicComponent {
    public String uniqueKeyField;
    public List<ComponentDocument> documentList;
    public List<ComponentKwic> kwicList;
    public List<ComponentList> listList;
    public List<ComponentGroup> groupList;
    public List<ComponentFacet> facetList;
    public List<ComponentTermVector> termVectorList;
    public List<ComponentPosition> statsPositionList;
    public List<ComponentToken> statsTokenList;
    public List<ComponentSpan> statsSpanList;
    public List<MtasSpanQuery> spanQueryList;
    public ComponentPrefix prefix;

    public ComponentField(String uniqueKeyField) {
      this.uniqueKeyField = uniqueKeyField;
      // initialise
      documentList = new ArrayList<>();
      kwicList = new ArrayList<>();
      listList = new ArrayList<>();
      groupList = new ArrayList<>();
      facetList = new ArrayList<>();
      termVectorList = new ArrayList<>();
      statsPositionList = new ArrayList<>();
      statsTokenList = new ArrayList<>();
      statsSpanList = new ArrayList<>();
      spanQueryList = new ArrayList<>();
      prefix = null;
    }
  }

  public static class ComponentPrefix implements BasicComponent {
    public String key;
    public SortedSet<String> singlePositionList;
    public SortedSet<String> multiplePositionList;
    public SortedSet<String> setPositionList;
    public SortedSet<String> intersectingList;

    public ComponentPrefix(String key) {
      this.key = key;
      singlePositionList = new TreeSet<>();
      multiplePositionList = new TreeSet<>();
      setPositionList = new TreeSet<>();
      intersectingList = new TreeSet<>();
    }

    public void addSinglePosition(String prefix) {
      if (!prefix.trim().isEmpty() && !singlePositionList.contains(prefix)
          && !multiplePositionList.contains(prefix)) {
        singlePositionList.add(prefix);
      }
    }

    public void addMultiplePosition(String prefix) {
      if (!prefix.trim().isEmpty()) {
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

    public void addSetPosition(String prefix) {
      if (!prefix.trim().isEmpty()) {
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

    public void addIntersecting(String prefix) {
      if (!prefix.trim().isEmpty()) {
        intersectingList.add(prefix);
      }
    }
  }

  public static class ComponentDocument implements BasicComponent {
    public String key;
    public String prefix;
    public String regexp;
    public String ignoreRegexp;
    public Set<String> list;
    public Set<String> ignoreList;
    public boolean listRegexp;
    public boolean listExpand;
    public boolean ignoreListRegexp;
    public int listExpandNumber;
    public String dataType;
    public String statsType;
    public SortedSet<String> statsItems;
    public int listNumber;
    public Map<Integer, String> uniqueKey;
    public Map<Integer, MtasDataCollector<?, ?>> statsData;
    public Map<Integer, MtasDataCollector<?, ?>> statsList;

    public ComponentDocument(String key, String prefix, String statsType,
        String regexp, String[] list, int listNumber, Boolean listRegexp,
        Boolean listExpand, int listExpandNumber, String ignoreRegexp,
        String[] ignoreList, Boolean ignoreListRegexp) throws IOException {
      this.key = key;
      this.prefix = prefix;
      this.regexp = regexp;
      if (list != null && list.length > 0) {
        this.list = new HashSet<>(Arrays.asList(list));
        this.listRegexp = listRegexp != null ? listRegexp : false;
        this.listExpand = (listExpand != null && listExpandNumber > 0)
            ? listExpand : false;
        if (this.listExpand) {
          this.listExpandNumber = listExpandNumber;
        } else {
          this.listExpandNumber = 0;
        }
      } else {
        this.list = null;
        this.listRegexp = false;
        this.listExpand = false;
        this.listExpandNumber = 0;
      }
      this.ignoreRegexp = ignoreRegexp;
      if (ignoreList != null && ignoreList.length > 0) {
        this.ignoreList = new HashSet<>(Arrays.asList(ignoreList));
        this.ignoreListRegexp = ignoreListRegexp != null ? ignoreListRegexp
            : false;
      } else {
        this.ignoreList = null;
        this.ignoreListRegexp = false;
      }
      this.listNumber = listNumber;
      uniqueKey = new HashMap<>();
      dataType = CodecUtil.DATA_TYPE_LONG;
      statsItems = CodecUtil.createStatsItems(statsType);
      this.statsType = CodecUtil.createStatsType(statsItems, null, null);
      this.statsData = new HashMap<>();
      if (this.listNumber > 0) {
        this.statsList = new HashMap<>();
      } else {
        this.statsList = null;
      }
    }
  }

  public static class ComponentKwic implements BasicComponent {
    public MtasSpanQuery query;
    public String key;
    public Map<Integer, List<KwicToken>> tokens;
    public Map<Integer, List<KwicHit>> hits;
    public Map<Integer, String> uniqueKey;
    public Map<Integer, Integer> subTotal;
    public Map<Integer, Integer> minPosition;
    public Map<Integer, Integer> maxPosition;
    public List<String> prefixes;
    public int left;
    public int right;
    public int start;
    public Integer number;
    public String output;
    public static final String KWIC_OUTPUT_TOKEN = "token";
    public static final String KWIC_OUTPUT_HIT = "hit";

    public ComponentKwic(MtasSpanQuery query, String key, String prefixes,
        Integer number, int start, int left, int right, String output)
        throws IOException {
      this.query = query;
      this.key = key;
      this.left = (left > 0) ? left : 0;
      this.right = (right > 0) ? right : 0;
      this.start = (start > 0) ? start : 0;
      this.number = (number != null && number >= 0) ? number : null;
      this.output = output;
      tokens = new HashMap<>();
      hits = new HashMap<>();
      uniqueKey = new HashMap<>();
      subTotal = new HashMap<>();
      minPosition = new HashMap<>();
      maxPosition = new HashMap<>();
      this.prefixes = new ArrayList<>();
      if ((prefixes != null) && (prefixes.trim().length() > 0)) {
        List<String> l = Arrays.asList(prefixes.split(Pattern.quote(",")));
        for (String ls : l) {
          if (ls.trim().length() > 0) {
            this.prefixes.add(ls.trim());
          }
        }
      }
      if (this.output == null) {
        if (!this.prefixes.isEmpty()) {
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

  public static class ComponentList implements BasicComponent {
    public MtasSpanQuery spanQuery;
    public String field;
    public String queryValue;
    public String queryType;
    public String queryPrefix;
    public String queryIgnore;
    public String queryMaximumIgnoreLength;
    public String key;
    public Map<String, String[]> queryVariables;
    public List<ListToken> tokens;
    public List<ListHit> hits;
    public Map<Integer, String> uniqueKey;
    public Map<Integer, Integer> subTotal;
    public Map<Integer, Integer> minPosition;
    public Map<Integer, Integer> maxPosition;
    public List<String> prefixes;
    public int left;
    public int right;
    public int total;
    public int position;
    public int start;
    public int number;
    public String prefix;
    public String output;

    public static final String LIST_OUTPUT_TOKEN = "token";
    public static final String LIST_OUTPUT_HIT = "hit";

    public ComponentList(MtasSpanQuery spanQuery, String field,
        String queryValue, String queryType, String queryPrefix,
        Map<String, String[]> queryVariables, String queryIgnore,
        String queryMaximumIgnoreLength, String key, String prefix, int start,
        int number, int left, int right, String output) throws IOException {
      this.spanQuery = spanQuery;
      this.field = field;
      this.queryValue = queryValue;
      this.queryType = queryType;
      this.queryPrefix = queryPrefix;
      this.queryIgnore = queryIgnore;
      this.queryMaximumIgnoreLength = queryMaximumIgnoreLength;
      this.queryVariables = queryVariables;
      this.key = key;
      this.left = left;
      this.right = right;
      this.start = start;
      this.number = number;
      this.output = output;
      this.prefix = prefix;
      total = 0;
      position = 0;
      tokens = new ArrayList<>();
      hits = new ArrayList<>();
      uniqueKey = new HashMap<>();
      subTotal = new HashMap<>();
      minPosition = new HashMap<>();
      maxPosition = new HashMap<>();
      this.prefixes = new ArrayList<>();
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
        if (!this.prefixes.isEmpty()) {
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

  public static class ComponentGroup implements BasicComponent {
    public MtasSpanQuery spanQuery;
    public String dataType;
    public String statsType;
    public String sortType;
    public String sortDirection;
    public SortedSet<String> statsItems;
    public Integer start;
    public Integer number;
    public String key;
    public MtasDataCollector<?, ?> dataCollector;

    ArrayList<String> prefixes;
    HashSet<String> hitInside;
    HashSet<String>[] hitInsideLeft;
    HashSet<String>[] hitInsideRight;
    HashSet<String>[] hitLeft;
    HashSet<String>[] hitRight;
    HashSet<String>[] left;
    HashSet<String>[] right;

    public ComponentGroup(MtasSpanQuery spanQuery, String key, int number,
        int start, String groupingHitInsidePrefixes,
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
      this.key = key;
      this.dataType = CodecUtil.DATA_TYPE_LONG;
      this.sortType = CodecUtil.STATS_TYPE_SUM;
      this.sortDirection = CodecUtil.SORT_DESC;
      this.statsItems = CodecUtil.createStatsItems("n,sum,mean");
      this.statsType = CodecUtil.createStatsType(this.statsItems, this.sortType,
          null);
      this.start = start;
      this.number = number;
      HashSet<String> tmpPrefixes = new HashSet<>();
      // analyze grouping condition
      if (groupingHitInsidePrefixes != null) {
        hitInside = new HashSet<>();
        String[] tmpList = groupingHitInsidePrefixes.split(",");
        for (String tmpItem : tmpList) {
          if (!tmpItem.trim().isEmpty()) {
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
      prefixes = new ArrayList<>(tmpPrefixes);
      // datacollector
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_LIST, this.dataType, this.statsType,
          this.statsItems, this.sortType, this.sortDirection, this.start,
          this.number, null, null);
    }

    private static HashSet<String>[] createPositionedPrefixes(
        HashSet<String> prefixList, String[] position, String[] prefixes)
        throws IOException {
      Pattern p = Pattern.compile("^([0-9]+)(\\-([0-9]+))?$");
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
          tmpPrefixList = new ArrayList<>();
          for (String tmpItem : tmpList) {
            if (!tmpItem.trim().isEmpty()) {
              tmpPrefixList.add(tmpItem.trim());
            }
          }
          if (tmpPrefixList.isEmpty()) {
            throw new IOException("incorrect prefixes " + prefixes[i]);
          }
          for (int t = 0; t < tmpPosition[i].length; t++) {
            if (result[tmpPosition[i][t]] == null) {
              result[tmpPosition[i][t]] = new HashSet<>();
            }
            result[tmpPosition[i][t]].addAll(tmpPrefixList);
          }
          prefixList.addAll(tmpPrefixList);
        }
        return result;
      }
    }

  }

  public static class ComponentFacet implements BasicComponent {
    public MtasSpanQuery[] spanQueries;
    public String[] baseFields;
    public String[] baseFieldTypes;
    public String[] baseTypes;
    public String[] baseSortTypes;
    public String[] baseSortDirections;
    public Double[] baseRangeSizes;
    public Double[] baseRangeBases;
    public String[] baseCollectorTypes;
    public String[] baseDataTypes;
    public String[] baseStatsTypes;
    public SortedSet<String>[] baseStatsItems;
    public String key;
    public MtasDataCollector<?, ?> dataCollector;
    public HashMap<MtasDataCollector<?, ?>, SubComponentFunction[]>[] baseFunctionList;
    public Integer[] baseNumbers;
    public Long[] baseMinimumLongs;
    public Long[] baseMaximumLongs;
    public MtasFunctionParserFunction[] baseParsers;
    public String[][] baseFunctionKeys;
    public String[][] baseFunctionExpressions;
    public String[][] baseFunctionTypes;
    public MtasFunctionParserFunction[][] baseFunctionParserFunctions;
    public static final String TYPE_STRING = "string";
    public static final String TYPE_POINTFIELD_WITHOUT_DOCVALUES = "pointfield_without_docvalues";

    @SuppressWarnings("unchecked")
    public ComponentFacet(MtasSpanQuery[] spanQueries, String field, String key,
        String[] baseFields, String[] baseFieldTypes, String[] baseTypes,
        Double[] baseRangeSizes, Double[] baseRangeBases,
        String[] baseSortTypes, String[] baseSortDirections,
        Integer[] baseNumbers, Double[] baseMinimumDoubles,
        Double[] baseMaximumDoubles, String[][] baseFunctionKeys,
        String[][] baseFunctionExpressions, String[][] baseFunctionTypes)
        throws IOException, ParseException {
      this.spanQueries = spanQueries.clone();
      this.key = key;
      this.baseFields = baseFields.clone();
      this.baseFieldTypes = baseFieldTypes.clone();
      this.baseTypes = baseTypes.clone();
      this.baseRangeSizes = baseRangeSizes.clone();
      this.baseRangeBases = baseRangeBases.clone();
      this.baseSortTypes = baseSortTypes.clone();
      this.baseSortDirections = baseSortDirections.clone();
      this.baseNumbers = baseNumbers.clone();
      // compute types
      this.baseMinimumLongs = new Long[baseFields.length];
      this.baseMaximumLongs = new Long[baseFields.length];
      this.baseCollectorTypes = new String[baseFields.length];
      this.baseStatsItems = new SortedSet[baseFields.length];
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
        baseFunctionList[i] = new HashMap<>();
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
            && !CodecUtil.isStatsType(this.baseSortTypes[i])) {
          throw new IOException(
              "unrecognized sortType " + this.baseSortTypes[i]);
        }
        this.baseCollectorTypes[i] = DataCollector.COLLECTOR_TYPE_LIST;
        this.baseStatsItems[i] = CodecUtil.createStatsItems(this.baseTypes[i]);
        this.baseStatsTypes[i] = CodecUtil.createStatsType(baseStatsItems[i],
            this.baseSortTypes[i], new MtasFunctionParserFunctionDefault(1));
      }
      boolean doFunctions;
      doFunctions = baseFunctionKeys != null && baseFunctionExpressions != null
          && baseFunctionTypes != null;
      doFunctions = doFunctions && baseFunctionKeys.length == baseFields.length;
      doFunctions = doFunctions && baseFunctionTypes.length == baseFields.length;
      if (doFunctions) {
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
                      new StringReader(baseFunctionExpressions[i][j]))).parse();
            }
          } else {
            this.baseFunctionKeys[i] = new String[0];
            this.baseFunctionExpressions[i] = new String[0];
            this.baseFunctionTypes[i] = new String[0];
            baseFunctionParserFunctions[i] = new MtasFunctionParserFunction[0];
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
          SortedSet<String>[] subBaseStatsItems = Arrays
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

    public boolean baseParserSumRule() {
      for (int i = 0; i < baseFields.length; i++) {
        if (!baseParsers[i].sumRule()) {
          return false;
        }
      }
      return true;
    }

    public boolean baseParserNeedPositions() {
      for (int i = 0; i < baseFields.length; i++) {
        if (baseParsers[i].needPositions()) {
          return true;
        }
      }
      return false;
    }

  }

  public static class ComponentTermVector implements BasicComponent {
    public String key;
    public String prefix;
    public List<SubComponentDistance> distances;
    public String regexp;
    public String ignoreRegexp;
    public String boundary;
    public boolean full;
    public Set<String> list;
    public Set<String> ignoreList;
    public boolean listRegexp;
    public boolean ignoreListRegexp;
    public List<SubComponentFunction> functions;
    public int number;
    public BytesRef startValue;
    public SubComponentFunction subComponentFunction;
    public boolean boundaryRegistration;
    public String sortType;
    public String sortDirection;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ComponentTermVector(String key, String prefix, String[] distanceKey,
        String[] distanceType, String[] distanceBase, Map[] distanceParameter,
        String[] distanceMinimum, String[] distanceMaximum, String regexp, Boolean full, String type,
        String sortType, String sortDirection, String startValue, int number,
        String[] functionKey, String[] functionExpression,
        String[] functionType, String boundary, String[] list,
        Boolean listRegexp, String ignoreRegexp, String[] ignoreList,
        Boolean ignoreListRegexp) throws IOException, ParseException {
      this.key = key;
      this.prefix = prefix;
      distances = new ArrayList<>();
      if (distanceKey != null && distanceType != null && distanceBase != null
          && distanceParameter != null && distanceMaximum != null) {
        if (distanceKey.length == distanceType.length
            && distanceKey.length == distanceBase.length
            && distanceKey.length == distanceParameter.length
            && distanceKey.length == distanceMaximum.length) {
          for (int i = 0; i < distanceKey.length; i++) {
            SubComponentDistance item = new SubComponentDistance(distanceKey[i],
                distanceType[i], this.prefix, distanceBase[i],
                distanceParameter[i], distanceMinimum[i], distanceMaximum[i]);
            distances.add(item);
          }
        }
      }
      this.regexp = regexp;
      this.full = (full != null && full);
      if (sortType == null) {
        this.sortType = CodecUtil.SORT_TERM;
      } else {
        this.sortType = sortType;
      }
      if (sortDirection == null) {
        if (this.sortType.equals(CodecUtil.SORT_TERM)) {
          this.sortDirection = CodecUtil.SORT_ASC;
        } else {
          this.sortDirection = CodecUtil.SORT_DESC;
        }
      } else {
        this.sortDirection = sortDirection;
      }
      if (list != null && list.length > 0) {
        this.list = new HashSet(Arrays.asList(list));
        this.listRegexp = listRegexp != null ? listRegexp : false;
        this.boundary = null;
        this.number = Integer.MAX_VALUE;
        if (!this.full) {
          this.sortType = CodecUtil.SORT_TERM;
          this.sortDirection = CodecUtil.SORT_ASC;
        }
      } else {
        this.list = null;
        this.listRegexp = false;
        this.startValue = (startValue != null)
            ? new BytesRef(prefix + MtasToken.DELIMITER + startValue) : null;
        if (boundary == null) {
          this.boundary = null;
          if (number < -1) {
            throw new IOException("number should not be " + number);
          } else if (number >= 0) {
            this.number = number;
          } else {
            if (!full) {
              throw new IOException(
                  "number " + number + " only supported for full termvector");
            } else {
              this.number = Integer.MAX_VALUE;
            }
          }
        } else {
          this.boundary = boundary;
          this.number = Integer.MAX_VALUE;
        }
      }
      this.ignoreRegexp = ignoreRegexp;
      if (ignoreList != null && ignoreList.length > 0) {
        this.ignoreList = new HashSet(Arrays.asList(ignoreList));
        this.ignoreListRegexp = ignoreListRegexp != null ? ignoreListRegexp
            : false;
      } else {
        this.ignoreList = null;
        this.ignoreListRegexp = false;
      }
      functions = new ArrayList<>();
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
      if (!this.sortType.equals(CodecUtil.SORT_TERM)
          && !CodecUtil.isStatsType(this.sortType)) {
        throw new IOException("unknown sortType '" + this.sortType + "'");
      } else if (!full && !this.sortType.equals(CodecUtil.SORT_TERM)) {
        if (!(this.sortType.equals(CodecUtil.STATS_TYPE_SUM)
            || this.sortType.equals(CodecUtil.STATS_TYPE_N))) {
          throw new IOException("sortType '" + this.sortType
              + "' only supported with full termVector");
        }
      }
      if (!this.sortType.equals(CodecUtil.SORT_TERM)) {
        if (startValue != null) {
          throw new IOException("startValue '" + startValue
              + "' only supported with termVector sorted on "
              + CodecUtil.SORT_TERM);
        }
      }
      if (!this.sortDirection.equals(CodecUtil.SORT_ASC)
          && !this.sortDirection.equals(CodecUtil.SORT_DESC)) {
        throw new IOException(
            "unrecognized sortDirection '" + this.sortDirection + "'");
      }
      boundaryRegistration = this.boundary != null;
      String segmentRegistration = null;
      if (this.full) {
        this.boundary = null;
        segmentRegistration = null;
      } else if (this.boundary != null) {
        if (this.sortDirection.equals(CodecUtil.SORT_ASC)) {
          segmentRegistration = MtasDataCollector.SEGMENT_BOUNDARY_ASC;
        } else if (this.sortDirection.equals(CodecUtil.SORT_DESC)) {
          segmentRegistration = MtasDataCollector.SEGMENT_BOUNDARY_DESC;
        }
      } else if (!this.sortType.equals(CodecUtil.SORT_TERM)) {
        if (this.sortDirection.equals(CodecUtil.SORT_ASC)) {
          segmentRegistration = MtasDataCollector.SEGMENT_SORT_ASC;
        } else if (this.sortDirection.equals(CodecUtil.SORT_DESC)) {
          segmentRegistration = MtasDataCollector.SEGMENT_SORT_DESC;
        }
      }
      // create main subComponentFunction
      this.subComponentFunction = new SubComponentFunction(
          DataCollector.COLLECTOR_TYPE_LIST, key, type,
          new MtasFunctionParserFunctionDefault(1), this.sortType,
          this.sortDirection, 0, this.number, segmentRegistration, boundary);
    }

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

  public static class ComponentStatus implements BasicComponent {
    public String handler;
    public String name;
    public String key;
    public Integer numberOfDocuments;
    public Integer numberOfSegments;
    public boolean getMtasHandler;
    public boolean getNumberOfDocuments;
    public boolean getNumberOfSegments;

    public ComponentStatus(String name, String key, boolean getMtasHandler,
        boolean getNumberOfDocuments, boolean getNumberOfSegments) {
      this.name = Objects.requireNonNull(name, "no name");
      this.key = key;
      this.getMtasHandler = getMtasHandler;
      this.getNumberOfDocuments = getNumberOfDocuments;
      this.getNumberOfSegments = getNumberOfSegments;
      handler = null;
      numberOfDocuments = null;
      numberOfSegments = null;
    }

  }
  
  public static class ComponentVersion implements BasicComponent {
	  public ComponentVersion() {
	  }
  }

  
  public interface ComponentStats extends BasicComponent {
  }

  public static class ComponentSpan implements ComponentStats {
    public MtasSpanQuery[] queries;
    public String key;
    public String dataType;
    public String statsType;
    public SortedSet<String> statsItems;
    public Long minimumLong;
    public Long maximumLong;
    public MtasDataCollector<?, ?> dataCollector;
    public List<SubComponentFunction> functions;
    public MtasFunctionParserFunction parser;

    public ComponentSpan(MtasSpanQuery[] queries, String key,
        Double minimumDouble, Double maximumDouble, String type,
        String[] functionKey, String[] functionExpression,
        String[] functionType) throws IOException, ParseException {
      this.queries = queries.clone();
      this.key = key;
      functions = new ArrayList<>();
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
        this.minimumLong = minimumDouble.longValue();
      } else {
        this.minimumLong = null;
      }
      if (maximumDouble != null) {
        this.maximumLong = maximumDouble.longValue();
      } else {
        this.maximumLong = null;
      }
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_DATA, dataType, this.statsType,
          this.statsItems, null, null, null, null, null, null);
    }

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

    public boolean functionBasic() {
      if (functions != null) {
        for (SubComponentFunction function : functions) {
          if (!function.statsType.equals(CodecUtil.STATS_BASIC)) {
            return false;
          }
        }
      }
      return true;
    }

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

    public Set<Integer> functionNeedArguments() {
      Set<Integer> list = new HashSet<>();
      if (functions != null) {
        for (SubComponentFunction function : functions) {
          list.addAll(function.parserFunction.needArgument());
        }
      }
      return list;
    }

  }

  public static class ComponentPosition implements ComponentStats {
    public String key;
    public String dataType;
    public String statsType;
    public SortedSet<String> statsItems;
    public Long minimumLong;
    public Long maximumLong;
    public MtasDataCollector<?, ?> dataCollector;

    public ComponentPosition(String key, Double minimumDouble,
        Double maximumDouble, String statsType)
        throws IOException, ParseException {
      this.key = key;
      dataType = CodecUtil.DATA_TYPE_LONG;
      this.statsItems = CodecUtil.createStatsItems(statsType);
      this.statsType = CodecUtil.createStatsType(this.statsItems, null, null);
      if (minimumDouble != null) {
        this.minimumLong = minimumDouble.longValue();
      } else {
        this.minimumLong = null;
      }
      if (maximumDouble != null) {
        this.maximumLong = maximumDouble.longValue();
      } else {
        this.maximumLong = null;
      }
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_DATA, dataType, this.statsType,
          this.statsItems, null, null, null, null, null, null);
    }
  }

  public static class ComponentToken implements ComponentStats {
    public String key;
    public String dataType;
    public String statsType;
    public SortedSet<String> statsItems;
    public Long minimumLong;
    public Long maximumLong;
    public MtasDataCollector<?, ?> dataCollector;

    public ComponentToken(String key, Double minimumDouble,
        Double maximumDouble, String statsType)
        throws IOException, ParseException {
      this.key = key;
      dataType = CodecUtil.DATA_TYPE_LONG;
      this.statsItems = CodecUtil.createStatsItems(statsType);
      this.statsType = CodecUtil.createStatsType(this.statsItems, null, null);
      if (minimumDouble != null) {
        this.minimumLong = minimumDouble.longValue();
      } else {
        this.minimumLong = null;
      }
      if (maximumDouble != null) {
        this.maximumLong = maximumDouble.longValue();
      } else {
        this.maximumLong = null;
      }
      dataCollector = DataCollector.getCollector(
          DataCollector.COLLECTOR_TYPE_DATA, dataType, this.statsType,
          this.statsItems, null, null, null, null, null, null);
    }
  }

  public static class ComponentCollection implements BasicComponent {
    public static final String ACTION_CREATE = "create";
    public static final String ACTION_CHECK = "check";
    public static final String ACTION_LIST = "list";
    public static final String ACTION_POST = "post";
    public static final String ACTION_IMPORT = "import";
    public static final String ACTION_DELETE = "delete";
    public static final String ACTION_EMPTY = "empty";
    public static final String ACTION_GET = "get";

    public String key;
    public String version;
    public String originalVersion;
    public String id;

    private String action;
    private Set<String> fields;
    private HashSet<String> values;

    public ComponentCollection(String key, String action) {
      this.key = key;
      this.action = action;
      this.version = null;
      this.originalVersion = null;
      values = new HashSet<>();
    }

    public void setListVariables() throws IOException {
      if (action.equals(ACTION_LIST)) {
        // do nothing
      } else {
        throw new IOException("not allowed with action " + action);
      }
    }

    public void setCreateVariables(String id, Set<String> fields)
        throws IOException {
      if (action.equals(ACTION_CREATE)) {
        this.id = id;
        this.fields = fields;
      } else {
        throw new IOException("not allowed with action " + action);
      }
    }

    public void setCheckVariables(String id) throws IOException {
      if (action.equals(ACTION_CHECK)) {
        this.id = id;
      } else {
        throw new IOException("not allowed with action " + action);
      }
    }

    public void setGetVariables(String id) throws IOException {
      if (action.equals(ACTION_GET)) {
        this.id = id;
      } else {
        throw new IOException("not allowed with action " + action);
      }
    }

    public void setPostVariables(String id, HashSet<String> values, String originalVersion)
        throws IOException {
      if (action.equals(ACTION_POST)) {
        this.id = id;
        this.values = values;
        this.originalVersion = originalVersion;
      } else {
        throw new IOException("not allowed with action " + action);
      }
    }

    public void setImportVariables(String id, String url, String collection)
        throws IOException {
      if (action.equals(ACTION_IMPORT)) {
        this.id = id;
        StringBuilder importUrlBuffer = new StringBuilder(url);
        importUrlBuffer.append("select");
        importUrlBuffer.append("?q=*:*&rows=0&wt=json");
        importUrlBuffer.append("&mtas=true&mtas.collection=true");
        importUrlBuffer.append("&mtas.collection.0.key=0");
        importUrlBuffer.append("&mtas.collection.0.action=get");
        importUrlBuffer.append(
            "&mtas.collection.0.id=" + URLEncoder.encode(collection, "UTF-8"));
        Map<String, Object> params = getImport(importUrlBuffer.toString());
        try {
          if (params.containsKey("mtas") && params.get("mtas") instanceof Map) {
            Map<String, Object> mtasParams = (Map<String, Object>) params
                .get("mtas");
            if (mtasParams.containsKey("collection")
                && mtasParams.get("collection") instanceof List) {
              List<Object> mtasCollectionList = (List<Object>) mtasParams
                  .get("collection");
              if (mtasCollectionList.size() == 1
                  && mtasCollectionList.get(0) instanceof Map) {
                Map<String, Object> collectionData = (Map<String, Object>) mtasCollectionList
                    .get(0);
                if (collectionData.containsKey("values")
                    && collectionData.get("values") instanceof List) {
                  List<String> valuesList = (List<String>) collectionData
                      .get("values");
                  for (String valueItem : valuesList) {
                    values.add(valueItem);
                  }
                } else {
                  throw new IOException("no values in response");
                }
              } else {
                throw new IOException(
                    "no valid mtas collection item in response");
              }
            } else {
              throw new IOException("no valid mtas collection in response");
            }
          } else {
            throw new IOException("no valid mtas in response");
          }
        } catch (ClassCastException e) {
          throw new IOException("unexpected response", e);
        }
      } else {
        throw new IOException("not allowed with action " + action);
      }
    }

    private Map<String, Object> getImport(String collectionGetUrl)
        throws IOException {
      // get data
      URL url = new URL(collectionGetUrl);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setDoOutput(false);
      connection.setDoInput(true);
      connection.setInstanceFollowRedirects(false);
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Content-Type",
          "application/json; charset=UTF-8");
      connection.setRequestProperty("charset", "utf-8");
      connection.setUseCaches(false);
      // process response
      InputStream is = null;
      try {
        is = connection.getInputStream();
      } catch (IOException ioe) {
        throw new IOException("Couldn't get data from url");
      }
      InputStreamReader in = new InputStreamReader(is, "UTF8");
      Map<String, Object> params = new HashMap<>();
      getParamsFromJSON(params, IOUtils.toString(in));
      connection.disconnect();
      return params;
    }

    public void setDeleteVariables(String id) throws IOException {
      if (action.equals(ACTION_DELETE)) {
        this.id = id;
      } else {
        throw new IOException("not allowed with action " + action);
      }
    }

    public String action() {
      return action;
    }
    
    public String originalVersion() throws IOException {
      if(action.equals(ACTION_POST)) {
        return originalVersion;
      } else {
        throw new IOException("unexpected call for "+action);
      }
    }

    public HashSet<String> values() {
      return values;
    }

    public Set<String> fields() {
      return fields;
    }

    public void addValue(String value) throws IOException {
      if (action.equals(ACTION_CREATE)) {
        if (version == null) {
          values.add(value);
        } else {
          throw new IOException("version already set");
        }
      } else {
        throw new IOException("not allowed for action '" + action + "'");
      }
    }

    private static void getParamsFromJSON(Map<String, Object> params,
        String json) {
      JSONParser parser = new JSONParser(json);
      try {
        Object o = ObjectBuilder.getVal(parser);
        if (!(o instanceof Map))
          return;
        Map<String, Object> map = (Map<String, Object>) o;
        // To make consistent with json.param handling, we should make query
        // params come after json params (i.e. query params should
        // appear to overwrite json params.

        // Solr params are based on String though, so we need to convert
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          String key = entry.getKey();
          Object val = entry.getValue();
          if (params.get(key) != null) {
            continue;
          }

          if (val == null) {
            params.remove(key);
          } else {
            params.put(key, val);
          }
        }

      } catch (Exception e) {
        // ignore parse exceptions at this stage, they may be caused by
        // incomplete
        // macro expansions
        return;
      }

    }

  }

  public static class SubComponentDistance implements Serializable {
    private static final long serialVersionUID = 1L;

    public String key;
    public String type;
    public String base;
    public String prefix;
    public Double minimum;
    public Double maximum;
    public Map<String, String> parameters;
    public transient Distance distance = null;

    private static final String NAME_LEVENSHTEIN = "levenshtein";
    private static final String NAME_DAMERAULEVENSHTEIN = "damerau-levenshtein";
    private static final String NAME_MORSE = "morse";

    public SubComponentDistance(String key, String type, String prefix,
        String base, Map<String, String> parameters, String minimum, String maximum) {
      this.key = key;
      this.prefix = prefix;
      this.type = type;
      this.base = base;
      this.parameters = parameters;
      this.minimum = minimum != null ? Double.parseDouble(minimum) : null;
      this.maximum = maximum != null ? Double.parseDouble(maximum) : null;
    }

    public Distance getDistance() throws IOException {
      if (distance == null) {
        if (type != null) {
          try {
            Constructor<Distance> constructor = (Constructor<Distance>) Class
                .forName("mtas.codec.util.distance." + createClassName(type)
                    + "Distance")
                .getConstructor(String.class, String.class, Double.class, Double.class,
                    Map.class);
            distance = constructor.newInstance(prefix, base, minimum, maximum,
                parameters);
          } catch (ClassNotFoundException | NoSuchMethodException
              | SecurityException | InstantiationException
              | IllegalAccessException | IllegalArgumentException
              | InvocationTargetException e) {
            throw new IllegalStateException(e);
          }
          // distance = new MorseDistance(prefix, base, maximum, parameters);
        } else {
          throw new IOException("unrecognized distance " + type);
        }
      }
      return distance;
    }

    private String createClassName(String type) {
      final char DASH = '-';
      Objects.requireNonNull(type, "Type is obligatory");
      final StringBuilder output = new StringBuilder(type.length());
      boolean lastCharacterWasDash = true;
      boolean thisCharacterWasDash;
      for (final char currentCharacter : type.toCharArray()) {
        thisCharacterWasDash = (currentCharacter == DASH);
        if (!thisCharacterWasDash) {
          if (lastCharacterWasDash) {
            output.append(Character.toTitleCase(currentCharacter));
          } else {
            output.append(currentCharacter);
          }
        }
        lastCharacterWasDash = thisCharacterWasDash;
      }
      return output.toString();
    }

  }

  public static class SubComponentFunction {
    public String key;
    public String expression;
    public String type;
    public MtasFunctionParserFunction parserFunction;
    public String statsType;
    public String dataType;
    public String sortType;
    public String sortDirection;
    public SortedSet<String> statsItems;
    public MtasDataCollector<?, ?> dataCollector;

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
      this.statsType = CodecUtil.createStatsType(statsItems, sortType,
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

  public static class KwicToken {
    public int startPosition;
    public int endPosition;
    public List<MtasTokenString> tokens;

    public KwicToken(Match match, List<MtasTokenString> tokens) {
      startPosition = match.startPosition;
      endPosition = match.endPosition - 1;
      this.tokens = tokens;
    }
  }

  public static class KwicHit {
    public int startPosition;
    public int endPosition;
    public Map<Integer, List<String>> hits;

    public KwicHit(Match match, Map<Integer, List<String>> hits) {
      startPosition = match.startPosition;
      endPosition = match.endPosition - 1;
      this.hits = hits;
    }
  }

  public static class GroupHit {
    private int hash;
    private int hashLeft;
    private int hashHit;
    private int hashRight;
    private String key;
    private String keyLeft;
    private String keyHit;
    private String keyRight;

    public List<String>[] dataHit;
    public List<String>[] dataLeft;
    public List<String>[] dataRight;
    public Set<String>[] missingHit;
    public Set<String>[] missingLeft;
    public Set<String>[] missingRight;
    public Set<String>[] unknownHit;
    public Set<String>[] unknownLeft;
    public Set<String>[] unknownRight;
    public static final String KEY_START = MtasToken.DELIMITER + "grouphit" + MtasToken.DELIMITER;

    private List<MtasTreeHit<String>> sort(List<MtasTreeHit<String>> data) {
      Collections.sort(data, new Comparator<MtasTreeHit<String>>() {
        @Override
        public int compare(MtasTreeHit<String> hit1, MtasTreeHit<String> hit2) {
          int compare = Integer.compare(hit1.additionalId, hit2.additionalId);
          compare = (compare == 0)
              ? Long.compare(hit1.additionalRef, hit2.additionalRef) : compare;
          return compare;
        }
      });
      return data;
    }

    @SuppressWarnings("unchecked")
    public GroupHit(List<MtasTreeHit<String>> list, int start, int end,
        int hitStart, int hitEnd, ComponentGroup group,
        Set<String> knownPrefixes) throws UnsupportedEncodingException {
      // compute dimensions
      int leftRangeStart = start;
      int leftRangeEnd = Math.min(end, hitStart - 1);
      int leftRangeLength = Math.max(0, 1 + leftRangeEnd - leftRangeStart);
      int hitLength = 1 + hitEnd - hitStart;
      int rightRangeStart = Math.max(start, hitEnd + 1);
      int rightRangeEnd = end;
      int rightRangeLength = Math.max(0, 1 + rightRangeEnd - rightRangeStart);
      // create initial arrays
      if (leftRangeLength > 0) {
        keyLeft = "";
        dataLeft = (ArrayList<String>[]) new ArrayList[leftRangeLength];
        missingLeft = (HashSet<String>[]) new HashSet[leftRangeLength];
        unknownLeft = (HashSet<String>[]) new HashSet[leftRangeLength];
        for (int p = 0; p < leftRangeLength; p++) {
          dataLeft[p] = new ArrayList<>();
          missingLeft[p] = new HashSet<>();
          unknownLeft[p] = new HashSet<>();
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
          dataHit[p] = new ArrayList<>();
          missingHit[p] = new HashSet<>();
          unknownHit[p] = new HashSet<>();
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
          dataRight[p] = new ArrayList<>();
          missingRight[p] = new HashSet<>();
          unknownRight[p] = new HashSet<>();
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
          if (group.hitInsideLeft[p - hitStart] != null) {
            missingHit[p - hitStart].addAll(group.hitInsideLeft[p - hitStart]);
          }
        }
      }
      if (group.hitLeft != null) {
        for (int p = hitStart; p <= Math.min(hitEnd,
            hitStart + group.hitLeft.length - 1); p++) {
          if (group.hitLeft[p - hitStart] != null) {
            missingHit[p - hitStart].addAll(group.hitLeft[p - hitStart]);
          }
        }
      }
      if (group.hitInsideRight != null) {
        for (int p = Math.max(hitStart,
            hitEnd - group.hitInsideRight.length + 1); p <= hitEnd; p++) {
          if (group.hitInsideRight[hitEnd - p] != null) {
            missingHit[p - hitStart].addAll(group.hitInsideRight[hitEnd - p]);
          }
        }
      }
      if (group.hitRight != null) {
        for (int p = hitStart; p <= Math.min(hitEnd,
            hitStart + group.hitRight.length - 1); p++) {
          if (group.hitRight[p - hitStart] != null) {
            missingHit[p - hitStart].addAll(group.hitRight[p - hitStart]);
          }
        }
      }
      if (group.left != null) {
        for (int p = 0; p < Math.min(leftRangeLength, group.left.length); p++) {
          if (group.left[p] != null) {
            missingLeft[p].addAll(group.left[p]);
          }
        }
      }
      if (group.hitRight != null) {
        for (int p = 0; p < Math.min(leftRangeLength,
            group.hitRight.length - dataHit.length); p++) {
          if (group.hitRight[p + dataHit.length] != null) {
            missingLeft[p].addAll(group.hitRight[p + dataHit.length]);
          }
        }
      }
      if (group.right != null) {
        for (int p = 0; p < Math.min(rightRangeLength,
            group.right.length); p++) {
          if (group.right[p] != null) {
            missingRight[p].addAll(group.right[p]);
          }
        }
      }
      if (group.hitLeft != null) {
        for (int p = 0; p < Math.min(rightRangeLength,
            group.hitLeft.length - dataHit.length); p++) {
          if (group.hitLeft[p + dataHit.length] != null) {
            missingRight[p].addAll(group.hitLeft[p + dataHit.length]);
          }
        }
      }

      // fill arrays and update missing administration
      List<MtasTreeHit<String>> sortedList = sort(list);
      for (MtasTreeHit<String> hit : sortedList) {
        // inside hit
        if (group.hitInside != null && hit.idData != null
            && group.hitInside.contains(hit.idData)) {
          for (int p = Math.max(hitStart, hit.startPosition); p <= Math
              .min(hitEnd, hit.endPosition); p++) {
            dataHit[p - hitStart].add(hit.refData);
            missingHit[p - hitStart]
                .remove(MtasToken.getPrefixFromValue(hit.refData));
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
            } else if (group.hitLeft != null
                && pHitLeft <= (group.hitLeft.length - 1)
                && group.hitLeft[pHitLeft] != null
                && group.hitLeft[pHitLeft].contains(hit.idData)) {
              // keyHit += hit.refData;
              dataHit[p - hitStart].add(hit.refData);
              missingHit[p - hitStart]
                  .remove(MtasToken.getPrefixFromValue(hit.refData));
            } else if (group.hitInsideRight != null
                && pHitRight <= (group.hitInsideRight.length - 1)
                && group.hitInsideRight[pHitRight] != null
                && group.hitInsideRight[pHitRight].contains(hit.idData)) {
              dataHit[p - hitStart].add(hit.refData);
              missingHit[p - hitStart]
                  .remove(MtasToken.getPrefixFromValue(hit.refData));
            } else if (group.hitRight != null
                && pHitRight <= (group.hitRight.length - 1)
                && group.hitRight[pHitRight] != null
                && group.hitRight[pHitRight].contains(hit.idData)) {
              dataHit[p - hitStart].add(hit.refData);
              missingHit[p - hitStart]
                  .remove(MtasToken.getPrefixFromValue(hit.refData));
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
                dataLeft[hitStart - 1 - p].add(hit.refData);
                missingLeft[hitStart - 1 - p]
                    .remove(MtasToken.getPrefixFromValue(hit.refData));
              } else if (group.hitRight != null
                  && pHitRight <= (group.hitRight.length - 1)
                  && group.hitRight[pHitRight] != null
                  && group.hitRight[pHitRight].contains(hit.idData)) {
                dataLeft[hitStart - 1 - p].add(hit.refData);
                missingLeft[hitStart - 1 - p]
                    .remove(MtasToken.getPrefixFromValue(hit.refData));
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
              } else if (group.hitLeft != null
                  && pHitLeft <= (group.hitLeft.length - 1)
                  && group.hitLeft[pHitLeft] != null
                  && group.hitLeft[pHitLeft].contains(hit.idData)) {
                dataRight[p - rightRangeStart].add(hit.refData);
                missingRight[p - rightRangeStart]
                    .remove(MtasToken.getPrefixFromValue(hit.refData));
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
      keyLeft = dataToString(dataLeft, missingLeft, true);
      keyHit = dataToString(dataHit, missingHit, false);
      keyRight = dataToString(dataRight, missingRight, false);
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

    @Override
    public int hashCode() {
      return hash;
    }

    private boolean dataEquals(List<String>[] d1, List<String>[] d2) {
      List<String> a1;
      List<String> a2;
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
      return dataEquals(dataRight, other.dataRight);
    }

    private String dataToString(List<String>[] data, Set<String>[] missing,
        boolean reverse) throws UnsupportedEncodingException {
      StringBuilder text = null;
      Encoder encoder = Base64.getEncoder();
      String prefix;
      String postfix;
      List<String> dataItem;
      Set<String> missingItem;
      if (data != null && missing != null && data.length == missing.length) {
        for (int i = 0; i < data.length; i++) {
          if (reverse) {
            dataItem = data[(data.length - i - 1)];
            missingItem = missing[(data.length - i - 1)];
          } else {
            dataItem = data[i];
            missingItem = missing[i];
          }
          if (i > 0) {
            text.append(",");
          } else {
            text = new StringBuilder();
          }
          for (int j = 0; j < dataItem.size(); j++) {
            if (j > 0) {
              text.append("&");
            }
            prefix = MtasToken.getPrefixFromValue(dataItem.get(j));
            postfix = MtasToken.getPostfixFromValue(dataItem.get(j));
            text.append(encoder
                .encodeToString(prefix.getBytes(StandardCharsets.UTF_8)));
            if (!postfix.isEmpty()) {
              text.append(".");
              text.append(encoder
                  .encodeToString(postfix.getBytes(StandardCharsets.UTF_8)));
            }
          }
          if (missingItem != null) {
            String[] tmpMissing = missingItem
                .toArray(new String[missingItem.size()]);
            for (int j = 0; j < tmpMissing.length; j++) {
              if (j > 0 || !dataItem.isEmpty()) {
                text.append("&");
              }
              text.append(encoder.encodeToString(
                  ("!" + tmpMissing[j]).getBytes(StandardCharsets.UTF_8)));
            }
          }
        }
      }
      return text != null ? text.toString() : null;
    }

    public String toString() {
      return key;
    }

    private static Map<String, String>[] keyToSubSubObject(String key,
        StringBuilder newKey) {
      if (!key.isEmpty()) {
        newKey.append(" [");
        String prefix;
        String postfix;
        String[] parts = key.split(Pattern.quote("&"));
        Map<String, String>[] result = new HashMap[parts.length];
        Pattern pattern = Pattern.compile("^([^\\.]*)\\.([^\\.]*)$");
        Decoder decoder = Base64.getDecoder();
        Matcher matcher;
        StringBuilder tmpNewKey = null;
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].isEmpty()) {
            result[i] = null;
          } else {
            HashMap<String, String> subResult = new HashMap<>();
            matcher = pattern.matcher(parts[i]);
            if (tmpNewKey != null) {
              tmpNewKey.append(" & ");
            } else {
              tmpNewKey = new StringBuilder();
            }
            if (matcher.matches()) {
              prefix = new String(
                  decoder.decode(
                      matcher.group(1).getBytes(StandardCharsets.UTF_8)),
                  StandardCharsets.UTF_8);
              postfix = new String(
                  decoder.decode(
                      matcher.group(2).getBytes(StandardCharsets.UTF_8)),
                  StandardCharsets.UTF_8);
              tmpNewKey.append(prefix.replace("=", "\\="));
              tmpNewKey.append("=\"" + postfix.replace("\"", "\\\"") + "\"");
              subResult.put("prefix", prefix);
              subResult.put("value", postfix);
            } else {
              prefix = new String(
                  decoder.decode(parts[i].getBytes(StandardCharsets.UTF_8)),
                  StandardCharsets.UTF_8);
              tmpNewKey.append(prefix.replace("=", "\\="));
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

    private static Map<Integer, Map<String, String>[]> keyToSubObject(
        String key, StringBuilder newKey) {
      Map<Integer, Map<String, String>[]> result = new HashMap<>();
      if (key == null || key.trim().isEmpty()) {
        return null;
      } else {
        String[] parts = key.split(Pattern.quote(","), -1);
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

    public static Map<String, Map<Integer, Map<String, String>[]>> keyToObject(
        String key, StringBuilder newKey) {
      if (key.startsWith(KEY_START)) {
        String content = key.substring(KEY_START.length());
        StringBuilder keyLeft = new StringBuilder("");
        StringBuilder keyHit = new StringBuilder("");
        StringBuilder keyRight = new StringBuilder("");
        Map<String, Map<Integer, Map<String, String>[]>> result = new HashMap<>();
        Map<Integer, Map<String, String>[]> resultLeft = null;
        Map<Integer, Map<String, String>[]> resultHit = null;
        Map<Integer, Map<String, String>[]> resultRight = null;
        String[] parts = content.split(Pattern.quote("|"), -1);
        if (parts.length == 3) {
          resultLeft = keyToSubObject(parts[0].trim(), keyLeft);
          resultHit = keyToSubObject(parts[1].trim(), keyHit);
          resultRight = keyToSubObject(parts[2].trim(), keyRight);
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

  public static class ListToken {
    public Integer docId;
    public Integer docPosition;
    public int startPosition;
    public int endPosition;
    public List<MtasTokenString> tokens;

    public ListToken(Integer docId, Integer docPosition, Match match,
        List<MtasTokenString> tokens) {
      this.docId = docId;
      this.docPosition = docPosition;
      startPosition = match.startPosition;
      endPosition = match.endPosition - 1;
      this.tokens = tokens;
    }
  }

  public static class ListHit {
    public Integer docId;
    public Integer docPosition;
    public int startPosition;
    public int endPosition;
    public Map<Integer, List<String>> hits;

    public ListHit(Integer docId, Integer docPosition, Match match,
        Map<Integer, List<String>> hits) {
      this.docId = docId;
      this.docPosition = docPosition;
      startPosition = match.startPosition;
      endPosition = match.endPosition - 1;
      this.hits = hits;
    }
  }

  public static class Match {
    public int startPosition;
    public int endPosition;

    public Match(int startPosition, int endPosition) {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      final Match that = (Match) obj;
      return startPosition == that.startPosition
          && endPosition == that.endPosition;
    }

    @Override
    public int hashCode() {
      int h = this.getClass().getSimpleName().hashCode();
      h = (h * 5) ^ startPosition;
      h = (h * 7) ^ endPosition;
      return h;
    }
  }
}
