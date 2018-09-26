package mtas.codec.util.collector;

import mtas.codec.util.CodecUtil;

import java.util.Map;
import java.util.Set;

public class MtasDataItemDoubleAdvanced extends MtasDataItemAdvanced<Double, Double> {
  private static final long serialVersionUID = 1L;

  public MtasDataItemDoubleAdvanced(Double valueSum, Double valueSumOfLogs,
      Double valueSumOfSquares, Double valueMin, Double valueMax, long valueN,
      MtasDataCollector<?, ?> sub, Set<String> statsItems, String sortType,
      String sortDirection, int errorNumber, Map<String, Integer> errorList,
      int sourceNumber) {
    super(valueSum, valueSumOfLogs, valueSumOfSquares, valueMin, valueMax,
        valueN, sub, statsItems, sortType, sortDirection, errorNumber,
        errorList, new MtasDataDoubleOperations(), sourceNumber);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public int compareTo(MtasDataItem<Double, Double> o) {
    int compare = 0;
    if (o instanceof MtasDataItemDoubleAdvanced) {
      MtasDataItemDoubleAdvanced to = (MtasDataItemDoubleAdvanced) o;
      MtasDataItemNumberComparator c1 = getComparableValue();
      MtasDataItemNumberComparator c2 = to.getComparableValue();
      compare = (c1 != null && c2 != null) ? c1.compareTo(c2.getValue()) : 0;
    }
    return sortDirection.equals(CodecUtil.SORT_DESC) ? -1 * compare : compare;
  }

  @Override
  public MtasDataItemNumberComparator<Double> getCompareValue1() {
    switch (sortType) {
    case CodecUtil.STATS_TYPE_SUM:
      return new MtasDataItemNumberComparator<>(valueSum, sortDirection);
    case CodecUtil.STATS_TYPE_MAX:
      return new MtasDataItemNumberComparator<>(valueMax, sortDirection);
    case CodecUtil.STATS_TYPE_MIN:
      return new MtasDataItemNumberComparator<>(valueMin, sortDirection);
    case CodecUtil.STATS_TYPE_SUMSQ:
      return new MtasDataItemNumberComparator<>(valueSumOfSquares,
          sortDirection);
    default:
      return null;
    }
  }

  public MtasDataItemNumberComparator<Double> getCompareValue2() {
    switch (sortType) {
    case CodecUtil.STATS_TYPE_SUMOFLOGS:
      return new MtasDataItemNumberComparator<>(valueSumOfLogs, sortDirection);
    case CodecUtil.STATS_TYPE_MEAN:
    case CodecUtil.STATS_TYPE_GEOMETRICMEAN:
    case CodecUtil.STATS_TYPE_STANDARDDEVIATION:
    case CodecUtil.STATS_TYPE_VARIANCE:
    case CodecUtil.STATS_TYPE_POPULATIONVARIANCE:
    case CodecUtil.STATS_TYPE_QUADRATICMEAN:
      return new MtasDataItemNumberComparator<>(getValue(sortType),
          sortDirection);
    default:
      return null;
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + "[" + valueSum + "," + valueN + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MtasDataItemDoubleAdvanced that = (MtasDataItemDoubleAdvanced) obj;
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
