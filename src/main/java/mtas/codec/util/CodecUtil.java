package mtas.codec.util;

import mtas.analysis.token.MtasToken;
import mtas.codec.MtasCodecPostingsFormat;
import mtas.codec.util.CodecComponent.ComponentCollection;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.parser.function.util.MtasFunctionParserFunction;
import mtas.search.spans.util.MtasSpanQuery;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  private static final List<String> STATS_FUNCTIONS = Collections.singletonList(STATS_FUNCTION_DISTRIBUTION);

  private static final List<String> STATS_TYPES = Arrays.asList(
      STATS_TYPE_GEOMETRICMEAN, STATS_TYPE_KURTOSIS, STATS_TYPE_MAX,
      STATS_TYPE_MEAN, STATS_TYPE_MIN, STATS_TYPE_N, STATS_TYPE_MEDIAN,
      STATS_TYPE_POPULATIONVARIANCE, STATS_TYPE_QUADRATICMEAN,
      STATS_TYPE_SKEWNESS, STATS_TYPE_STANDARDDEVIATION, STATS_TYPE_SUM,
      STATS_TYPE_SUMSQ, STATS_TYPE_SUMOFLOGS, STATS_TYPE_VARIANCE);

  private static final List<String> STATS_BASIC_TYPES = Arrays
      .asList(STATS_TYPE_N, STATS_TYPE_SUM, STATS_TYPE_MEAN);

  private static final List<String> STATS_ADVANCED_TYPES = Arrays.asList(
      STATS_TYPE_MAX, STATS_TYPE_MIN, STATS_TYPE_SUMSQ, STATS_TYPE_SUMOFLOGS,
      STATS_TYPE_GEOMETRICMEAN, STATS_TYPE_STANDARDDEVIATION,
      STATS_TYPE_VARIANCE, STATS_TYPE_POPULATIONVARIANCE,
      STATS_TYPE_QUADRATICMEAN);

  private static final List<String> STATS_FULL_TYPES = Arrays
      .asList(STATS_TYPE_KURTOSIS, STATS_TYPE_MEDIAN, STATS_TYPE_SKEWNESS);

  public static final String STATS_BASIC = "basic";

  public static final String STATS_ADVANCED = "advanced";

  public static final String STATS_FULL = "full";

  public static final String DATA_TYPE_LONG = "long";

  public static final String DATA_TYPE_DOUBLE = "double";

  private static Pattern fpStatsItems = Pattern
      .compile("(([^\\(,]+)(\\([^\\)]*\\))?)");

  private static Pattern fpStatsFunctionItems = Pattern
      .compile("(([^\\(,]+)(\\(([^\\)]*)\\)))");

  private CodecUtil() {
  }

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
    return prefix.replace("\u0000", "");
  }

  public static String termPrefixValue(String term) {
    return (term == null) ? null : term.replace("\u0000", "");
  }

  public static void collectField(String field, IndexSearcher searcher,
      IndexReader rawReader, ArrayList<Integer> fullDocList,
      ArrayList<Integer> fullDocSet, ComponentField fieldStats, Status status)
      throws IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, IOException {
    if (fieldStats != null) {
      IndexReader reader = searcher.getIndexReader();
      HashMap<MtasSpanQuery, SpanWeight> spansQueryWeight = new HashMap<>();
      // only if spanQueryList is not empty
      if (fieldStats.spanQueryList.size() > 0) {
        final float boost = 0;
        for (MtasSpanQuery sq : fieldStats.spanQueryList) {
          spansQueryWeight.put(sq, sq.rewrite(reader)
                                     .createWeight(searcher, false, boost));
        }
      }
      // collect
      CodecCollector.collectField(field, searcher, reader, rawReader,
          fullDocList, fullDocSet, fieldStats, spansQueryWeight, status);
    }
  }

  public static void collectCollection(IndexReader reader,
      List<Integer> fullDocSet, ComponentCollection collectionInfo)
      throws IOException {
    if (collectionInfo != null) {
      CodecCollector.collectCollection(reader, fullDocSet, collectionInfo);
    }
  }

  static SortedSet<String> createStatsItems(String statsType)
      throws IOException {
    SortedSet<String> statsItems = new TreeSet<>();
    SortedSet<String> functionItems = new TreeSet<>();
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
          if (m.group(3) == null) {
            throw new IOException("'" + tmpStatsItem + "' should be called as '"
                + tmpStatsItem + "()' with an optional argument");
          } else {
            functionItems.add(m.group(1).trim());
          }
        } else {
          throw new IOException("unknown statsType '" + tmpStatsItem + "'");
        }
      }
    }
    if (statsItems.size() == 0 && functionItems.size() == 0) {
      statsItems.add(STATS_TYPE_SUM);
      statsItems.add(STATS_TYPE_N);
      statsItems.add(STATS_TYPE_MEAN);
    }
    if (functionItems.size() > 0) {
      statsItems.addAll(functionItems);
    }
    return statsItems;
  }

  static String createStatsType(Set<String> statsItems, String sortType,
      MtasFunctionParserFunction functionParser) {
    String statsType = STATS_BASIC;
    for (String statsItem : statsItems) {
      if (STATS_FULL_TYPES.contains(statsItem)) {
        statsType = STATS_FULL;
        break;
      } else if (STATS_ADVANCED_TYPES.contains(statsItem)) {
        statsType = STATS_ADVANCED;
      } else if (!Objects.equals(statsType, STATS_ADVANCED)
          && STATS_BASIC_TYPES.contains(statsItem)) {
        statsType = STATS_BASIC;
      } else {
        Matcher m = fpStatsFunctionItems.matcher(statsItem.trim());
        if (m.find()) {
          if (STATS_FUNCTIONS.contains(m.group(2).trim())) {
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
        statsType = (statsType == null || !Objects.equals(statsType, STATS_FULL))
            ? STATS_ADVANCED : statsType;
      }
    }
    return statsType;
  }

  public static boolean isStatsType(String type) {
    return STATS_TYPES.contains(type);
  }
}
