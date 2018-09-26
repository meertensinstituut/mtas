package mtas.codec.util.collector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public abstract class MtasDataItem<T1 extends Number & Comparable<T1>, T2 extends Number & Comparable<T2>>
    implements Serializable, Comparable<MtasDataItem<T1, T2>> {

  private static Log log = LogFactory.getLog(MtasDataItem.class);

  private static final long serialVersionUID = 1L;

  protected MtasDataCollector<?, ?> sub;

  private Set<String> statsItems;

  protected String sortType;
  protected String sortDirection;
  protected int errorNumber;

  private Map<String, Integer> errorList;

  protected MtasDataItemNumberComparator<?> comparableSortValue;
  protected boolean recomputeComparableSortValue;
  protected int sourceNumber;

  public MtasDataItem(MtasDataCollector<?, ?> sub, Set<String> statsItems,
      String sortType, String sortDirection, int errorNumber,
      Map<String, Integer> errorList, int sourceNumber) {
    this.sub = sub;
    this.statsItems = statsItems;
    this.sortType = sortType;
    this.sortDirection = sortDirection;
    this.errorNumber = errorNumber;
    this.errorList = errorList;
    this.sourceNumber = sourceNumber;
    this.comparableSortValue = null;
    this.recomputeComparableSortValue = true;
  }

  public abstract void add(MtasDataItem<T1, T2> newItem) throws IOException;

  public abstract Map<String, Object> rewrite(boolean showDebugInfo)
      throws IOException;

  public MtasDataCollector getSub() {
    return sub;
  }

  protected abstract int getCompareValueType() throws IOException;

  protected final Set<String> getStatsItems() {
    return statsItems;
  }

  protected final Map<String, Integer> getErrorList() {
    return errorList;
  }

  private void computeComparableValue() {
    recomputeComparableSortValue = false;
    try {
      int type = getCompareValueType();
      switch (type) {
      case 0:
        comparableSortValue = getCompareValue0();
        break;
      case 1:
        comparableSortValue = getCompareValue1();
        break;
      case 2:
        comparableSortValue = getCompareValue2();
        break;
      default:
        comparableSortValue = null;
        break;
      }
    } catch (IOException e) {
      log.debug(e);
      comparableSortValue = null;
    }
  }

  protected final MtasDataItemNumberComparator getComparableValue() {
    if (recomputeComparableSortValue) {
      computeComparableValue();
    }
    return comparableSortValue;
  }

  protected abstract MtasDataItemNumberComparator<Long> getCompareValue0();

  protected abstract MtasDataItemNumberComparator<T1> getCompareValue1();

  protected abstract MtasDataItemNumberComparator<T2> getCompareValue2();
}
