package mtas.codec.util.collector;

import java.util.HashMap;
import java.util.TreeSet;
import mtas.codec.util.CodecUtil;
import mtas.codec.util.DataCollector.MtasDataCollector;

/**
 * The Class MtasDataItemLongAdvanced.
 */
class MtasDataItemLongAdvanced
    extends MtasDataItemAdvanced<Long, Double> {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /**
   * Instantiates a new mtas data item long advanced.
   *
   * @param valueSum the value sum
   * @param valueSumOfLogs the value sum of logs
   * @param valueSumOfSquares the value sum of squares
   * @param valueMin the value min
   * @param valueMax the value max
   * @param valueN the value n
   * @param sub the sub
   * @param statsItems the stats items
   * @param sortType the sort type
   * @param sortDirection the sort direction
   * @param errorNumber the error number
   * @param errorList the error list
   */
  public MtasDataItemLongAdvanced(long valueSum, double valueSumOfLogs,
      long valueSumOfSquares, long valueMin, long valueMax, long valueN,
      MtasDataCollector<?, ?> sub, TreeSet<String> statsItems,
      String sortType, String sortDirection, int errorNumber,
      HashMap<String, Integer> errorList, int sourceNumber) {
    super(valueSum, valueSumOfLogs, valueSumOfSquares, valueMin, valueMax,
        valueN, sub, statsItems, sortType, sortDirection, errorNumber,
        errorList, new MtasDataLongOperations(), sourceNumber);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(MtasDataItem<Long> o) {
    int compare = 0;
    if (o instanceof MtasDataItemLongAdvanced) {
      MtasDataItemLongAdvanced to = (MtasDataItemLongAdvanced) o;
      if (sortType.equals(CodecUtil.STATS_TYPE_N)) {
        compare = valueN.compareTo(to.valueN);
      } else if (sortType.equals(CodecUtil.STATS_TYPE_SUM)) {
        compare = valueSum.compareTo(to.valueSum);
      } else if (sortType.equals(CodecUtil.STATS_TYPE_MAX)) {
        compare = valueMax.compareTo(to.valueMax);
      } else if (sortType.equals(CodecUtil.STATS_TYPE_MIN)) {
        compare = valueMin.compareTo(to.valueMin);
      } else if (sortType.equals(CodecUtil.STATS_TYPE_SUMSQ)) {
        compare = valueSumOfSquares.compareTo(to.valueSumOfSquares);
      } else if (sortType.equals(CodecUtil.STATS_TYPE_SUMOFLOGS)) {
        compare = valueSumOfLogs.compareTo(to.valueSumOfLogs);
      } else if (sortType.equals(CodecUtil.STATS_TYPE_MEAN)) {
        compare = getValue(sortType).compareTo(to.getValue(sortType));
      } else if (sortType.equals(CodecUtil.STATS_TYPE_GEOMETRICMEAN)) {
        compare = getValue(sortType).compareTo(to.getValue(sortType));
      } else if (sortType.equals(CodecUtil.STATS_TYPE_STANDARDDEVIATION)) {
        compare = getValue(sortType).compareTo(to.getValue(sortType));
      } else if (sortType.equals(CodecUtil.STATS_TYPE_VARIANCE)) {
        compare = getValue(sortType).compareTo(to.getValue(sortType));
      } else if (sortType.equals(CodecUtil.STATS_TYPE_POPULATIONVARIANCE)) {
        compare = getValue(sortType).compareTo(to.getValue(sortType));
      } else if (sortType.equals(CodecUtil.STATS_TYPE_QUADRATICMEAN)) {
        compare = getValue(sortType).compareTo(to.getValue(sortType));
      }
    }
    return sortDirection.equals(CodecUtil.SORT_DESC) ? -1 * compare : compare;
  }
}

