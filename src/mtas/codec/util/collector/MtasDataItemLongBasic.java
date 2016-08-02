package mtas.codec.util.collector;

import java.util.HashMap;
import java.util.TreeSet;
import mtas.codec.util.CodecUtil;
import mtas.codec.util.DataCollector.MtasDataCollector;

/**
 * The Class MtasDataItemLongBasic.
 */
class MtasDataItemLongBasic
    extends MtasDataItemBasic<Long, Double> {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /**
   * Instantiates a new mtas data item long basic.
   *
   * @param valueSum the value sum
   * @param valueN the value n
   * @param sub the sub
   * @param statsItems the stats items
   * @param sortType the sort type
   * @param sortDirection the sort direction
   * @param errorNumber the error number
   * @param errorList the error list
   */
  public MtasDataItemLongBasic(long valueSum, long valueN,
      MtasDataCollector<?, ?> sub, TreeSet<String> statsItems,
      String sortType, String sortDirection, int errorNumber,
      HashMap<String, Integer> errorList, int sourceNumber) {
    super(valueSum, valueN, sub, statsItems, sortType, sortDirection,
        errorNumber, errorList, new MtasDataLongOperations(), sourceNumber);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(MtasDataItem<Long> o) {
    int compare = 0;
    if (o instanceof MtasDataItemLongBasic) {
      MtasDataItemLongBasic to = (MtasDataItemLongBasic) o;
      if (sortType.equals(CodecUtil.STATS_TYPE_N)) {
        compare = valueN.compareTo(to.valueN);
      } else if (sortType.equals(CodecUtil.STATS_TYPE_SUM)) {
        compare = valueSum.compareTo(to.valueSum);
      } else if (sortType.equals(CodecUtil.STATS_TYPE_MEAN)) {
        compare = getValue(sortType).compareTo(to.getValue(sortType));
      }
    }
    return sortDirection.equals(CodecUtil.SORT_DESC) ? -1 * compare : compare;
  }

}

