package mtas.codec.util.collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import mtas.codec.util.CodecUtil;
import mtas.codec.util.DataCollector.MtasDataCollector;

/**
 * The Class MtasDataItemFull.
 *
 * @param <T1> the generic type
 * @param <T2> the generic type
 */
abstract class MtasDataItemFull<T1 extends Number, T2 extends Number>
    extends MtasDataItem<T1> implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The full values. */
  public T1[] fullValues;

  /** The operations. */
  protected MtasDataOperations<T1, T2> operations;

  /** The stats. */
  protected DescriptiveStatistics stats = null;

  /** The fp stats function items. */
  private Pattern fpStatsFunctionItems = Pattern
      .compile("(([^\\(,]+)(\\(([^\\)]*)\\))?)");

  /**
   * Instantiates a new mtas data item full.
   *
   * @param value the value
   * @param sub the sub
   * @param statsItems the stats items
   * @param sortType the sort type
   * @param sortDirection the sort direction
   * @param errorNumber the error number
   * @param errorList the error list
   * @param operations the operations
   */
  public MtasDataItemFull(T1[] value, MtasDataCollector<?, ?> sub,
      TreeSet<String> statsItems, String sortType, String sortDirection,
      int errorNumber, HashMap<String, Integer> errorList,
      MtasDataOperations<T1, T2> operations, int sourceNumber) {
    super(sub, statsItems, sortType, sortDirection, errorNumber, errorList, sourceNumber);
    this.fullValues = value;
    this.operations = operations;
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataItem#add(mtas.codec.util.
   * DataCollector.MtasDataItem)
   */
  @Override
  public void add(MtasDataItem<T1> newItem) throws IOException {
    if (newItem instanceof MtasDataItemFull) {
      MtasDataItemFull<T1, T2> newTypedItem = (MtasDataItemFull<T1, T2>) newItem;
      T1[] tmpValue = operations
          .createVector1(fullValues.length + newTypedItem.fullValues.length);
      System.arraycopy(fullValues, 0, tmpValue, 0, fullValues.length);
      System.arraycopy(newTypedItem.fullValues, 0, tmpValue,
          fullValues.length, newTypedItem.fullValues.length);
      fullValues = tmpValue;
    } else {
      throw new IOException("can only add MtasDataItemFull");
    }
  }

  /**
   * Creates the stats.
   */
  protected void createStats() {
    if (stats == null) {
      stats = new DescriptiveStatistics();
      for (T1 value : fullValues) {
        stats.addValue(value.doubleValue());
      }
    }
  }

  /**
   * Gets the distribution.
   *
   * @param arguments the arguments
   * @return the distribution
   */
  abstract protected HashMap<String, Object> getDistribution(
      String arguments);

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataItem#rewrite()
   */
  @Override
  public Map<String, Object> rewrite(boolean showDebugInfo) throws IOException {
    createStats();
    Map<String, Object> response = new HashMap<String, Object>();
    for (String statsItem : statsItems) {
      if (statsItem.equals(CodecUtil.STATS_TYPE_SUM)) {
        response.put(statsItem, stats.getSum());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_N)) {
        response.put(statsItem, stats.getN());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_MAX)) {
        response.put(statsItem, stats.getMax());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_MIN)) {
        response.put(statsItem, stats.getMin());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_SUMSQ)) {
        response.put(statsItem, stats.getSumsq());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_SUMOFLOGS)) {
        response.put(statsItem,
            stats.getN() * Math.log(stats.getGeometricMean()));
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_MEAN)) {
        response.put(statsItem, stats.getMean());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_GEOMETRICMEAN)) {
        response.put(statsItem, stats.getGeometricMean());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_STANDARDDEVIATION)) {
        response.put(statsItem, stats.getStandardDeviation());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_VARIANCE)) {
        response.put(statsItem, stats.getVariance());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_POPULATIONVARIANCE)) {
        response.put(statsItem, stats.getPopulationVariance());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_QUADRATICMEAN)) {
        response.put(statsItem, stats.getQuadraticMean());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_KURTOSIS)) {
        response.put(statsItem, stats.getKurtosis());
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_MEDIAN)) {
        response.put(statsItem, stats.getPercentile(50));
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_SKEWNESS)) {
        response.put(statsItem, stats.getSkewness());
      } else {
        Matcher m = fpStatsFunctionItems.matcher(statsItem);
        if (m.find()) {
          String function = m.group(2).trim();
          if (function.equals(CodecUtil.STATS_FUNCTION_DISTRIBUTION)) {
            response.put(statsItem, getDistribution(m.group(4)));
          } else {
            response.put(statsItem, "test");
          }
        } else {
          response.put(statsItem, "niet");
        }
      }
    }
    if (errorNumber > 0) {
      Map<String, Object> errorResponse = new HashMap<String, Object>();
      for (Entry<String, Integer> entry : errorList.entrySet()) {
        errorResponse.put(entry.getKey(), entry.getValue());
      }
      response.put("errorNumber", errorNumber);
      response.put("errorList", errorResponse);
    }
    if(showDebugInfo) {
      response.put("sourceNumber", sourceNumber);    
      response.put("stats", "full");
    }  
    return response;
  }




}

