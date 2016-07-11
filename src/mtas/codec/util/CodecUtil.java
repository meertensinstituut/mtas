package mtas.codec.util;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mtas.analysis.token.MtasToken;
import mtas.codec.MtasCodecPostingsFormat;
import mtas.parser.function.util.MtasFunctionParserFunction;
import mtas.codec.util.CodecComponent.ComponentField;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.store.IndexInput;

public class CodecUtil {

  public static final String STATS_TYPE_GEOMETRICMEAN = "geometricmean";
  public static final String STATS_TYPE_KURTOSIS = "kurtosis";
  public static final String STATS_TYPE_MAX = "max";
  public static final String STATS_TYPE_MEAN = "mean";
  public static final String STATS_TYPE_MIN = "min";
  public static final String STATS_TYPE_N = "n";
  public static final String STATS_TYPE_MEDIAN = "median";
  public static final String STATS_TYPE_POPULATIONVARIANCE = "populationvariance";
  public static final String STATS_TYPE_QUADRATICMEAN = "quadraticmean";
  public static final String STATS_TYPE_SKEWNESS = "skewness";
  public static final String STATS_TYPE_STANDARDDEVIATION = "standarddeviation";
  public static final String STATS_TYPE_SUM = "sum";
  public static final String STATS_TYPE_SUMSQ = "sumsq";
  public static final String STATS_TYPE_SUMOFLOGS = "sumoflogs";
  public static final String STATS_TYPE_VARIANCE = "variance";

  public static final String STATS_TYPE_ALL = "all";

  public static final String STATS_FUNCTION_DISTRIBUTION = "distribution";

  public static final String SORT_TERM = "term";
  public static final String SORT_ASC = "asc";
  public static final String SORT_DESC = "desc";

  public static final List<String> STATS_FUNCTIONS = Arrays
      .asList(STATS_FUNCTION_DISTRIBUTION);

  public static final List<String> STATS_TYPES = Arrays.asList(
      STATS_TYPE_GEOMETRICMEAN, STATS_TYPE_KURTOSIS, STATS_TYPE_MAX,
      STATS_TYPE_MEAN, STATS_TYPE_MIN, STATS_TYPE_N, STATS_TYPE_MEDIAN,
      STATS_TYPE_POPULATIONVARIANCE, STATS_TYPE_QUADRATICMEAN,
      STATS_TYPE_SKEWNESS, STATS_TYPE_STANDARDDEVIATION, STATS_TYPE_SUM,
      STATS_TYPE_SUMSQ, STATS_TYPE_SUMOFLOGS, STATS_TYPE_VARIANCE);

  public static final List<String> STATS_BASIC_TYPES = Arrays
      .asList(STATS_TYPE_N, STATS_TYPE_SUM, STATS_TYPE_MEAN);

  public static final List<String> STATS_ADVANCED_TYPES = Arrays.asList(
      STATS_TYPE_MAX, STATS_TYPE_MIN, STATS_TYPE_SUMSQ, STATS_TYPE_SUMOFLOGS,
      STATS_TYPE_GEOMETRICMEAN, STATS_TYPE_STANDARDDEVIATION,
      STATS_TYPE_VARIANCE, STATS_TYPE_POPULATIONVARIANCE,
      STATS_TYPE_QUADRATICMEAN);

  public static final List<String> STATS_FULL_TYPES = Arrays
      .asList(STATS_TYPE_KURTOSIS, STATS_TYPE_MEDIAN, STATS_TYPE_SKEWNESS);

  public static final String STATS_BASIC = "basic";
  public static final String STATS_ADVANCED = "advanced";
  public static final String STATS_FULL = "full";

  public static final String DATA_TYPE_LONG = "long";
  public static final String DATA_TYPE_DOUBLE = "double";
  
  private static Pattern fpStatsItems = Pattern.compile("(([^\\(,]+)(\\([^\\)]*\\))?)");
  private static Pattern fpStatsFunctionItems = Pattern.compile("(([^\\(,]+)(\\(([^\\)]*)\\)))");

  public static boolean isSinglePositionPrefix(FieldInfo fieldInfo,
      String prefix) throws IOException {
    if (fieldInfo == null) {
      throw new IOException("no fieldInfo");
    } else {
      String info = fieldInfo.getAttribute(
          MtasCodecPostingsFormat.MTAS_FIELDINFO_ATTRIBUTE_PREFIX_SINGLE_POSITION);
      if (info == null) {
        throw new IOException("no "
            + MtasCodecPostingsFormat.MTAS_FIELDINFO_ATTRIBUTE_PREFIX_SINGLE_POSITION);
      } else {
        return Arrays.asList(info.split(Pattern.quote(MtasToken.DELIMITER)))
            .contains(prefix);
      }
    }
  }

  public static String termValue(String term) {
    int i = term.indexOf(MtasToken.DELIMITER);
    String value = null;
    if (i >= 0) {
      value = term.substring((i + MtasToken.DELIMITER.length()));
      value = (value.length() > 0) ? value : null;
    }
    return (value == null) ? null : value.replace("\u0000", "");
  }

  public static String termPrefix(String term) {
    int i = term.indexOf(MtasToken.DELIMITER);
    String prefix = term;
    if (i >= 0) {
      prefix = term.substring(0, i);
    }
    return (prefix == null) ? null : prefix.replace("\u0000", "");
  } 
  
  public static String termPrefixValue(String term) {
    return (term==null)?null:term.replace("\u0000", "");
  }

  public static void collect(String field, IndexSearcher searcher,
      IndexReader rawReader, ArrayList<Integer> fullDocList,
      ArrayList<Integer> fullDocSet, ComponentField fieldStats)
      throws IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, IOException {
    if (fieldStats != null) {
      IndexReader reader = searcher.getIndexReader();
      HashMap<SpanQuery, SpanWeight> spansQueryWeight = new HashMap<SpanQuery, SpanWeight>();
      // only if spanQueryList is not empty
      if (fieldStats.spanQueryList.size() > 0) {
        for (SpanQuery sq : fieldStats.spanQueryList) {
          spansQueryWeight.put(sq,
              ((SpanQuery) sq.rewrite(reader)).createWeight(searcher, false));
        }
      }
      // collect
      CodecCollector.collect(field, searcher, reader, rawReader, fullDocList,
          fullDocSet, fieldStats, spansQueryWeight);
    }
  }

  static TreeSet<String> createStatsItems(String statsType) throws IOException {
    TreeSet<String> statsItems = new TreeSet<String>();
    TreeSet<String> functionItems = new TreeSet<String>();
    if (statsType != null) {      
      Matcher m = fpStatsItems.matcher(statsType.trim());
      while (m.find()) {
        String tmpStatsItem = m.group(2).trim();
        if (STATS_TYPES.contains(tmpStatsItem)) {
          statsItems.add(tmpStatsItem);
        } else if (tmpStatsItem.equals(STATS_TYPE_ALL)) {
          for (String type : STATS_TYPES) {
            statsItems.add(type);
          }
        } else if (STATS_FUNCTIONS.contains(tmpStatsItem)) {
          if(m.group(3)==null) {
            throw new IOException(
                "'" + tmpStatsItem + "' should be called as '" + tmpStatsItem + "()' with an optional argument");
          } else {
            functionItems.add(m.group(1).trim());
          }  
        } else {
          throw new IOException(
              "unknown statsType '" + tmpStatsItem + "'");
        }
      }
    }
    if (statsItems.size() == 0) {
      statsItems.add(STATS_TYPE_SUM);
      statsItems.add(STATS_TYPE_N);
      statsItems.add(STATS_TYPE_MEAN);
    }
    if (functionItems.size() > 0) {
      statsItems.addAll(functionItems);
    }
    return statsItems;
  }

  static String createStatsType(TreeSet<String> statsItems, String sortType,
      MtasFunctionParserFunction functionParser) {
    String statsType = STATS_BASIC;
    for (String statsItem : statsItems) {
      if (STATS_FULL_TYPES.contains(statsItem)) {
        statsType = STATS_FULL;
        break;
      } else if (STATS_ADVANCED_TYPES.contains(statsItem)) {
        statsType = STATS_ADVANCED;
      } else if (statsType != STATS_ADVANCED
          && STATS_BASIC_TYPES.contains(statsItem)) {
        statsType = STATS_BASIC;
      } else {
        Matcher m = fpStatsFunctionItems.matcher(statsItem.trim());
        if(m.find()) {
          if(STATS_FUNCTIONS.contains(m.group(2).trim())) {
            statsType = STATS_FULL;
            break;
          } 
        }
      }
    }
    if (sortType != null && STATS_TYPES.contains(sortType)) {
      if (STATS_FULL_TYPES.contains(sortType)) {
        statsType = STATS_FULL;
      } else if (STATS_ADVANCED_TYPES.contains(sortType)) {
        statsType = (statsType == null || statsType != STATS_FULL)
            ? STATS_ADVANCED : statsType;
      } else if (STATS_BASIC_TYPES.contains(sortType)) {
        statsType = (statsType == null) ? STATS_BASIC : statsType;
      }
    }
    return statsType;
  }

}
