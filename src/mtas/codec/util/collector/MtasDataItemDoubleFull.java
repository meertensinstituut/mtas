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
 * The Class MtasDataItemDoubleFull.
 */
public class MtasDataItemDoubleFull
    extends MtasDataItemFull<Double, Double> {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The fp argument. */
  private static Pattern fpArgument = Pattern.compile("([^=,]+)=([^,]*)");

  /**
   * Instantiates a new mtas data item double full.
   *
   * @param value the value
   * @param sub the sub
   * @param statsItems the stats items
   * @param sortType the sort type
   * @param sortDirection the sort direction
   * @param errorNumber the error number
   * @param errorList the error list
   */
  public MtasDataItemDoubleFull(double[] value, MtasDataCollector<?, ?> sub,
      TreeSet<String> statsItems, String sortType, String sortDirection,
      int errorNumber, HashMap<String, Integer> errorList, int sourceNumber) {
    super(ArrayUtils.toObject(value), sub, statsItems, sortType,
        sortDirection, errorNumber, errorList,
        new MtasDataDoubleOperations(), sourceNumber);
  }

  /**
   * Gets the number of decimals.
   *
   * @param ds the ds
   * @return the number of decimals
   */
  private int getNumberOfDecimals(String ds) {
    if (!ds.contains(".")) {
      return 0;
    } else {
      return (ds.length() - ds.indexOf(".") - 1);
    }
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
    Double start = null, end = null, step = null;
    Integer d = null, number = null;
    if (argument != null) {
      Matcher m = fpArgument.matcher(argument);
      // get settings
      while (m.find()) {
        if (m.group(1).trim().equals("start")) {
          start = Double.parseDouble(m.group(2));
          d = (d == null) ? getNumberOfDecimals(m.group(2))
              : Math.max(d, getNumberOfDecimals(m.group(2)));
        } else if (m.group(1).trim().equals("end")) {
          end = Double.parseDouble(m.group(2));
          d = (d == null) ? getNumberOfDecimals(m.group(2))
              : Math.max(d, getNumberOfDecimals(m.group(2)));
        } else if (m.group(1).trim().equals("step")) {
          step = Double.parseDouble(m.group(2));
          d = (d == null) ? getNumberOfDecimals(m.group(2))
              : Math.max(d, getNumberOfDecimals(m.group(2)));
        } else if (m.group(1).trim().equals("number")) {
          number = Integer.parseInt(m.group(2));
        }
      }
    }
    // always exactly one of (positive) number and (positive) step, other null
    if ((number == null || number < 1) && (step == null || step <= 0)) {
      number = 10;
      step = null;
    } else if (step != null && step <= 0) {
      step = null;
    } else if (number != null && number < 1) {
      number = null;
    } else if (step != null) {
      number = null;
    }
    // sanity checks start/end
    createStats();
    double tmpStart = stats.getMin();
    double tmpEnd = stats.getMax();
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
        step = (tmpEnd - tmpStart) / number;
      }
      number = Double.valueOf(Math.ceil((tmpEnd - tmpStart) / step))
          .intValue();
      start = tmpStart;
      end = start + (number * step);
    } else if (start == null) {
      if (step == null) {
        step = (end - tmpStart) / number;
      }
      number = Double.valueOf(Math.ceil((end - tmpStart) / step)).intValue();
      start = end - (number * step);
    } else if (end == null) {
      if (step == null) {
        step = (tmpEnd - start) / number;
      }
      number = Double.valueOf(Math.ceil((tmpEnd - start) / step)).intValue();
      end = start + (number * step);
    } else {
      if (step == null) {
        step = (end - start) / number;
      }
      number = Double.valueOf(Math.ceil((end - start) / step)).intValue();
    }
    // round step to agreeable format and recompute number
    int tmpD = Double
        .valueOf(Math.max(0, 1 + Math.ceil(-1 * Math.log10(step))))
        .intValue();
    d = (d == null) ? tmpD : Math.max(d, tmpD);
    double tmp = Math.pow(10.0, d);
    step = Math.round(step * tmp) / tmp;
    number = Double.valueOf(Math.ceil((end - start) / step)).intValue();

    // compute distribution
    long[] list = new long[number];
    for (Double v : fullValues) {
      if (v >= start && v <= end) {
        int i = Math.min(
            Double.valueOf(Math.floor((v - start) / step)).intValue(),
            (number - 1));
        list[i]++;
      }
    }
    Double l, r;
    String ls, rs;
    for (int i = 0; i < number; i++) {
      l = start + i * step;
      r = Math.min(end, l + step);
      ls = String.format("%." + d + "f", l);
      rs = String.format("%." + d + "f", r);
      String key = "[" + ls + "," + rs
          + ((i == (number - 1) && r >= tmpEnd && l <= tmpEnd) ? "]" : ")");
      result.put(key, list[i]);
    }
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(MtasDataItem<Double> o) {
    int compare = 0;
    if (o instanceof MtasDataItemDoubleFull) {
      MtasDataItemDoubleFull to = (MtasDataItemDoubleFull) o;
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
