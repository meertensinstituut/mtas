package mtas.codec.util.collector;

import mtas.codec.util.CodecUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

abstract class MtasDataItemBasic<T1 extends Number & Comparable<T1>, T2 extends Number & Comparable<T2>>
    extends MtasDataItem<T1, T2> implements Serializable {
  private static final long serialVersionUID = 1L;

  protected T1 valueSum;
  protected Long valueN;
  protected MtasDataOperations<T1, T2> operations;

  public MtasDataItemBasic(T1 valueSum, long valueN,
      MtasDataCollector<?, ?> sub, Set<String> statsItems, String sortType,
      String sortDirection, int errorNumber, Map<String, Integer> errorList,
      MtasDataOperations<T1, T2> operations, int sourceNumber) {
    super(sub, statsItems, sortType, sortDirection, errorNumber, errorList,
        sourceNumber);
    this.valueSum = valueSum;
    this.valueN = valueN;
    this.operations = operations;
  }

  @Override
  public void add(MtasDataItem<T1, T2> newItem) throws IOException {
    if (newItem instanceof MtasDataItemBasic) {
      MtasDataItemBasic<T1, T2> newTypedItem = (MtasDataItemBasic<T1, T2>) newItem;
      this.valueSum = operations.add11(this.valueSum, newTypedItem.valueSum);
      this.valueN += newTypedItem.valueN;
      recomputeComparableSortValue = true;
    } else {
      throw new IOException("can only add MtasDataItemBasic");
    }
  }

  @Override
  public Map<String, Object> rewrite(boolean showDebugInfo) throws IOException {
    Map<String, Object> response = new HashMap<>();
    for (String statsItem : getStatsItems()) {
      if (statsItem.equals(CodecUtil.STATS_TYPE_SUM)) {
        response.put(statsItem, valueSum);
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_N)) {
        response.put(statsItem, valueN);
      } else if (statsItem.equals(CodecUtil.STATS_TYPE_MEAN)) {
        response.put(statsItem, getValue(statsItem));
      } else {
        response.put(statsItem, null);
      }
    }
    if (errorNumber > 0) {
      Map<String, Object> errorResponse = new HashMap<String, Object>();
      for (Entry<String, Integer> entry : getErrorList().entrySet()) {
        errorResponse.put(entry.getKey(), entry.getValue());
      }
      response.put("errorNumber", errorNumber);
      response.put("errorList", errorResponse);
    }
    if (showDebugInfo) {
      response.put("sourceNumber", sourceNumber);
      response.put("stats", "basic");
    }
    return response;
  }

  protected T2 getValue(String statsType) {
    if (statsType.equals(CodecUtil.STATS_TYPE_MEAN)) {
      return operations.divide1(valueSum, valueN);
    } else {
      return null;
    }
  }

  public final int getCompareValueType() throws IOException {
    switch (sortType) {
    case CodecUtil.STATS_TYPE_N:
      return 0;
    case CodecUtil.STATS_TYPE_SUM:
      return 1;
    case CodecUtil.STATS_TYPE_MEAN:
      return 2;
    default:
      throw new IOException("sortType " + sortType + " not supported");
    }
  }

  public final MtasDataItemNumberComparator<Long> getCompareValue0() {
    switch (sortType) {
    case CodecUtil.STATS_TYPE_N:
      return new MtasDataItemNumberComparator<Long>(valueN, sortDirection);
    default:
      return null;
    }
  }
}
