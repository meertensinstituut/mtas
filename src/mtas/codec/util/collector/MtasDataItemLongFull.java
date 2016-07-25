package mtas.codec.util.collector;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.ArrayUtils;
import mtas.codec.util.CodecUtil;
import mtas.codec.util.DataCollector.MtasDataCollector;

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
   * @param value the value
   * @param sub the sub
   * @param statsItems the stats items
   * @param sortType the sort type
   * @param sortDirection the sort direction
   * @param errorNumber the error number
   * @param errorList the error list
   */
  public MtasDataItemLongFull(long[] value, MtasDataCollector<?, ?> sub,
      TreeSet<String> statsItems, String sortType, String sortDirection,
      int errorNumber, HashMap<String, Integer> errorList) {
    super(ArrayUtils.toObject(value), sub, statsItems, sortType, sortDirection,
        errorNumber, errorList, new MtasDataLongOperations());
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

  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(MtasDataItem<Long> o) {
    int compare = 0;
    if (o instanceof MtasDataItemLongFull) {
      MtasDataItemLongFull to = (MtasDataItemLongFull) o;
      createStats();
      to.createStats();
      if (sortType.equals(CodecUtil.STATS_TYPE_N)) {
        compare = Long.valueOf(stats.getN()).compareTo(to.stats.getN());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_SUM)) {
        compare = Double.valueOf(stats.getSum()).compareTo(to.stats.getSum());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_MAX)) {
        compare = Double.valueOf(stats.getMax()).compareTo(to.stats.getMax());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_MIN)) {
        compare = Double.valueOf(stats.getMin()).compareTo(to.stats.getMin());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_SUMSQ)) {
        compare = Double.valueOf(stats.getSumsq())
            .compareTo(to.stats.getSumsq());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_SUMOFLOGS)) {
        compare = Double
            .valueOf(stats.getN() * Math.log(stats.getGeometricMean()))
            .compareTo(to.stats.getN() * Math.log(to.stats.getGeometricMean()));
      } else if (sortType.equals(CodecUtil.STATS_TYPE_MEAN)) {
        compare = Double.valueOf(stats.getMean()).compareTo(to.stats.getMean());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_GEOMETRICMEAN)) {
        compare = Double.valueOf(stats.getGeometricMean())
            .compareTo(to.stats.getGeometricMean());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_STANDARDDEVIATION)) {
        compare = Double.valueOf(stats.getStandardDeviation())
            .compareTo(to.stats.getStandardDeviation());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_VARIANCE)) {
        compare = Double.valueOf(stats.getVariance())
            .compareTo(to.stats.getVariance());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_POPULATIONVARIANCE)) {
        compare = Double.valueOf(stats.getPopulationVariance())
            .compareTo(to.stats.getPopulationVariance());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_QUADRATICMEAN)) {
        compare = Double.valueOf(stats.getQuadraticMean())
            .compareTo(to.stats.getQuadraticMean());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_KURTOSIS)) {
        compare = Double.valueOf(stats.getKurtosis())
            .compareTo(to.stats.getKurtosis());
      } else if (sortType.equals(CodecUtil.STATS_TYPE_MEDIAN)) {
        compare = Double.valueOf(stats.getPercentile(50))
            .compareTo(to.stats.getPercentile(50));
      } else if (sortType.equals(CodecUtil.STATS_TYPE_SKEWNESS)) {
        compare = Double.valueOf(stats.getSkewness())
            .compareTo(to.stats.getSkewness());
      }
    }
    return sortDirection.equals(CodecUtil.SORT_DESC) ? -1 * compare : compare;
  }

}
