package mtas.codec.util.collector;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.ArrayUtils;
import mtas.codec.util.CodecUtil;

/**
 * The Class MtasDataItemLongFull.
 */
class MtasDataItemLongFull extends MtasDataItemFull<Long, Double> {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The fp argument. */
  private static Pattern fpArgument = Pattern.compile("([^=,]+)=([^,]*)");

  /**
   * Instantiates a new mtas data item long full.
   *
   * @param value
   *          the value
   * @param sub
   *          the sub
   * @param statsItems
   *          the stats items
   * @param sortType
   *          the sort type
   * @param sortDirection
   *          the sort direction
   * @param errorNumber
   *          the error number
   * @param errorList
   *          the error list
   * @param sourceNumber
   *          the source number
   */
  public MtasDataItemLongFull(long[] value, MtasDataCollector<?, ?> sub,
      TreeSet<String> statsItems, String sortType, String sortDirection,
      int errorNumber, HashMap<String, Integer> errorList, int sourceNumber) {
    super(ArrayUtils.toObject(value), sub, statsItems, sortType, sortDirection,
        errorNumber, errorList, new MtasDataLongOperations(), sourceNumber);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.codec.util.DataCollector.MtasDataItemFull#getDistribution(java.lang.
   * String)
   */
  @Override
  protected HashMap<String, Object> getDistribution(String argument) {
    HashMap<String, Object> result = new LinkedHashMap<String, Object>();
    Long start = null, end = null, step = null;
    Integer number = null;
    if (argument != null) {
      Matcher m = fpArgument.matcher(argument);
      // get settings
      while (m.find()) {
        if (m.group(1).trim().equals("start")) {
          start = Long.parseLong(m.group(2));
        } else if (m.group(1).trim().equals("end")) {
          end = Long.parseLong(m.group(2));
        } else if (m.group(1).trim().equals("step")) {
          step = Long.parseLong(m.group(2));
        } else if (m.group(1).trim().equals("number")) {
          number = Integer.parseInt(m.group(2));
        }
      }
    }
    // always exactly one of (positive) number and (positive) step, other null
    if ((number == null || number < 1) && (step == null || step < 1)) {
      number = 10;
      step = null;
    } else if (step != null && step < 1) {
      step = null;
    } else if (number != null && number < 1) {
      number = null;
    } else if (step != null) {
      number = null;
    }
    // sanity checks start/end
    createStats();
    long tmpStart = Double.valueOf(Math.floor(stats.getMin())).longValue();
    long tmpEnd = Double.valueOf(Math.ceil(stats.getMax())).longValue();
    if (start != null && end != null && start > end) {
      return null;
    } else if (start != null && start > tmpEnd) {
      return null;
    } else if (end != null && end < tmpStart) {
      return null;
    }
    // check start and end
    if (start == null && end == null) {
      if (step == null) {
        step = -Math.floorDiv((tmpStart - tmpEnd - 1), number);
      }
      number = Long.valueOf(-Math.floorDiv((tmpStart - tmpEnd - 1), step))
          .intValue();
      start = tmpStart;
      end = start + (number * step);
    } else if (start == null) {
      if (step == null) {
        step = -Math.floorDiv((tmpStart - end - 1), number);
      }
      number = Long.valueOf(-Math.floorDiv((tmpStart - end - 1), step))
          .intValue();
      start = end - (number * step);
    } else if (end == null) {
      if (step == null) {
        step = -Math.floorDiv((start - tmpEnd - 1), number);
      }
      number = Long.valueOf(-Math.floorDiv((start - tmpEnd - 1), step))
          .intValue();
      end = start + (number * step);
    } else {
      if (step == null) {
        step = -Math.floorDiv((start - end - 1), number);
      }
      number = Long.valueOf(-Math.floorDiv((start - end - 1), step)).intValue();
    }
    long[] list = new long[number];
    for (Long v : fullValues) {
      if (v >= start && v <= end) {
        int i = Long.valueOf(Math.floorDiv((v - start), step)).intValue();
        list[i]++;
      }
    }
    for (int i = 0; i < number; i++) {
      Long l = start + i * step;
      Long r = Math.min(end, l + step - 1);
      String key;
      if (step > 1 && r > l) {
        key = "[" + String.valueOf(l) + "," + String.valueOf(r) + "]";
      } else {
        key = "[" + String.valueOf(l) + "]";
      }
      result.put(key, list[i]);
    }
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public int compareTo(MtasDataItem<Long, Double> o) {
    int compare = 0;
    if (o instanceof MtasDataItemLongFull) {
      MtasDataItemLongFull to = (MtasDataItemLongFull) o;
      NumberComparator c1 = getComparableValue();
      NumberComparator c2 = to.getComparableValue();
      compare = (c1 != null && c2 != null) ? c1.compareTo(c2.getValue()) : 0;
    }
    return sortDirection.equals(CodecUtil.SORT_DESC) ? -1 * compare : compare;
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.collector.MtasDataItem#getCompareValue1()
   */
  @Override
  public NumberComparator<Long> getCompareValue1() {
    createStats();
    switch (sortType) {
    case CodecUtil.STATS_TYPE_SUM:
      return new NumberComparator<Long>(Math.round(stats.getSum()));
    case CodecUtil.STATS_TYPE_MAX:
      return new NumberComparator<Long>(Math.round(stats.getMax()));
    case CodecUtil.STATS_TYPE_MIN:
      return new NumberComparator<Long>(Math.round(stats.getMin()));
    case CodecUtil.STATS_TYPE_SUMSQ:
      return new NumberComparator<Long>(Math.round(stats.getSumsq()));
    default:
      return null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.collector.MtasDataItem#getCompareValue2()
   */
  @Override
  public NumberComparator<Double> getCompareValue2() {
    createStats();
    switch (sortType) {
    case CodecUtil.STATS_TYPE_SUMOFLOGS:
      return new NumberComparator<Double>(
          stats.getN() * Math.log(stats.getGeometricMean()));
    case CodecUtil.STATS_TYPE_MEAN:
      return new NumberComparator<Double>(stats.getMean());
    case CodecUtil.STATS_TYPE_GEOMETRICMEAN:
      return new NumberComparator<Double>(stats.getGeometricMean());
    case CodecUtil.STATS_TYPE_STANDARDDEVIATION:
      return new NumberComparator<Double>(stats.getStandardDeviation());
    case CodecUtil.STATS_TYPE_VARIANCE:
      return new NumberComparator<Double>(stats.getVariance());
    case CodecUtil.STATS_TYPE_POPULATIONVARIANCE:
      return new NumberComparator<Double>(stats.getPopulationVariance());
    case CodecUtil.STATS_TYPE_QUADRATICMEAN:
      return new NumberComparator<Double>(stats.getQuadraticMean());
    case CodecUtil.STATS_TYPE_KURTOSIS:
      return new NumberComparator<Double>(stats.getKurtosis());
    case CodecUtil.STATS_TYPE_MEDIAN:
      return new NumberComparator<Double>(stats.getPercentile(50));
    case CodecUtil.STATS_TYPE_SKEWNESS:
      return new NumberComparator<Double>(stats.getSkewness());
    default:
      return null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    return this.getClass().getSimpleName() + "[" + fullValues.length + "]";
  }

}
