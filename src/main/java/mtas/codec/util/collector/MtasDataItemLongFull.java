package mtas.codec.util.collector;

import mtas.codec.util.CodecUtil;
import org.apache.commons.lang.ArrayUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class MtasDataItemLongFull extends MtasDataItemFull<Long, Double> {
  private static final long serialVersionUID = 1L;

  private static Pattern fpArgument = Pattern.compile("([^=,]+)=([^,]*)");

  public MtasDataItemLongFull(long[] value, MtasDataCollector<?, ?> sub,
      Set<String> statsItems, String sortType, String sortDirection,
      int errorNumber, Map<String, Integer> errorList, int sourceNumber) {
    super(ArrayUtils.toObject(value), sub, statsItems, sortType, sortDirection,
        errorNumber, errorList, new MtasDataLongOperations(), sourceNumber);
  }

  @Override
  protected HashMap<String, Object> getDistribution(String argument) {
    HashMap<String, Object> result = new LinkedHashMap<>();
    Long start = null;
    Long end = null;
    Long step = null;
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
        key = "[" + l + "," + r + "]";
      } else {
        key = "[" + l + "]";
      }
      result.put(key, list[i]);
    }
    return result;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public int compareTo(MtasDataItem<Long, Double> o) {
    int compare = 0;
    if (o instanceof MtasDataItemLongFull) {
      MtasDataItemLongFull to = (MtasDataItemLongFull) o;
      MtasDataItemNumberComparator c1 = getComparableValue();
      MtasDataItemNumberComparator c2 = to.getComparableValue();
      compare = (c1 != null && c2 != null) ? c1.compareTo(c2.getValue()) : 0;
    }
    return sortDirection.equals(CodecUtil.SORT_DESC) ? -1 * compare : compare;
  }

  @Override
  public MtasDataItemNumberComparator<Long> getCompareValue1() {
    createStats();
    switch (sortType) {
    case CodecUtil.STATS_TYPE_SUM:
      return new MtasDataItemNumberComparator<>(Math.round(stats.getSum()),
          sortDirection);
    case CodecUtil.STATS_TYPE_MAX:
      return new MtasDataItemNumberComparator<>(Math.round(stats.getMax()),
          sortDirection);
    case CodecUtil.STATS_TYPE_MIN:
      return new MtasDataItemNumberComparator<>(Math.round(stats.getMin()),
          sortDirection);
    case CodecUtil.STATS_TYPE_SUMSQ:
      return new MtasDataItemNumberComparator<>(Math.round(stats.getSumsq()),
          sortDirection);
    default:
      return null;
    }
  }

  @Override
  public MtasDataItemNumberComparator<Double> getCompareValue2() {
    createStats();
    switch (sortType) {
    case CodecUtil.STATS_TYPE_SUMOFLOGS:
      return new MtasDataItemNumberComparator<>(
          stats.getN() * Math.log(stats.getGeometricMean()), sortDirection);
    case CodecUtil.STATS_TYPE_MEAN:
      return new MtasDataItemNumberComparator<>(stats.getMean(), sortDirection);
    case CodecUtil.STATS_TYPE_GEOMETRICMEAN:
      return new MtasDataItemNumberComparator<>(stats.getGeometricMean(),
          sortDirection);
    case CodecUtil.STATS_TYPE_STANDARDDEVIATION:
      return new MtasDataItemNumberComparator<>(stats.getStandardDeviation(),
          sortDirection);
    case CodecUtil.STATS_TYPE_VARIANCE:
      return new MtasDataItemNumberComparator<>(stats.getVariance(),
          sortDirection);
    case CodecUtil.STATS_TYPE_POPULATIONVARIANCE:
      return new MtasDataItemNumberComparator<>(stats.getPopulationVariance(),
          sortDirection);
    case CodecUtil.STATS_TYPE_QUADRATICMEAN:
      return new MtasDataItemNumberComparator<>(
          Math.sqrt(stats.getSumsq() / stats.getN()), sortDirection);
    case CodecUtil.STATS_TYPE_KURTOSIS:
      return new MtasDataItemNumberComparator<>(stats.getKurtosis(),
          sortDirection);
    case CodecUtil.STATS_TYPE_MEDIAN:
      return new MtasDataItemNumberComparator<>(stats.getPercentile(50),
          sortDirection);
    case CodecUtil.STATS_TYPE_SKEWNESS:
      return new MtasDataItemNumberComparator<>(stats.getSkewness(),
          sortDirection);
    default:
      return null;
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + "[" + fullValues.length + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MtasDataItemLongFull that = (MtasDataItemLongFull) obj;
    MtasDataItemNumberComparator<?> c1 = getComparableValue();
    MtasDataItemNumberComparator<?> c2 = that.getComparableValue();
    return (c1 != null && c2 != null && c1.equals(c2));
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ getComparableValue().hashCode();
    return h;
  }
}
