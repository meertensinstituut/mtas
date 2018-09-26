package mtas.codec.util.collector;

import mtas.codec.util.CodecUtil;

import java.util.Map;
import java.util.Set;

class MtasDataItemLongBasic extends MtasDataItemBasic<Long, Double> {
  private static final long serialVersionUID = 1L;

  public MtasDataItemLongBasic(Long valueSum, long valueN,
      MtasDataCollector<?, ?> sub, Set<String> statsItems, String sortType,
      String sortDirection, int errorNumber, Map<String, Integer> errorList,
      int sourceNumber) {
    super(valueSum, valueN, sub, statsItems, sortType, sortDirection,
        errorNumber, errorList, new MtasDataLongOperations(), sourceNumber);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public int compareTo(MtasDataItem<Long, Double> o) {
    int compare = 0;
    if (o instanceof MtasDataItemLongBasic) {
      MtasDataItemLongBasic to = (MtasDataItemLongBasic) o;
      MtasDataItemNumberComparator c1 = getComparableValue();
      MtasDataItemNumberComparator c2 = to.getComparableValue();
      compare = (c1 != null && c2 != null) ? c1.compareTo(c2.getValue()) : 0;
    }
    return sortDirection.equals(CodecUtil.SORT_DESC) ? -1 * compare : compare;
  }

  @Override
  public MtasDataItemNumberComparator<Long> getCompareValue1() {
    switch (sortType) {
    case CodecUtil.STATS_TYPE_N:
      return new MtasDataItemNumberComparator<Long>(valueN, sortDirection);
    case CodecUtil.STATS_TYPE_SUM:
      return new MtasDataItemNumberComparator<Long>(valueSum, sortDirection);
    default:
      return null;
    }
  }

  @Override
  public MtasDataItemNumberComparator<Double> getCompareValue2() {
    switch (sortType) {
    case CodecUtil.STATS_TYPE_MEAN:
      return new MtasDataItemNumberComparator<Double>(getValue(sortType),
          sortDirection);
    default:
      return null;
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + "[" + valueSum + "," + valueN
        + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MtasDataItemLongBasic that = (MtasDataItemLongBasic) obj;
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
