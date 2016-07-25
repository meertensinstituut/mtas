package mtas.codec.util.collector;

import java.io.IOException;
import java.util.Collections;
import java.util.TreeSet;
import org.apache.commons.lang.ArrayUtils;
import mtas.codec.util.CodecUtil;
import mtas.codec.util.DataCollector.MtasDataCollector;

/**
 * The Class MtasDataLongFull.
 */
public class MtasDataLongFull
    extends MtasDataFull<Long, Double, MtasDataItemLongFull> {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /**
   * Instantiates a new mtas data long full.
   *
   * @param collectorType the collector type
   * @param statsItems the stats items
   * @param sortType the sort type
   * @param sortDirection the sort direction
   * @param start the start
   * @param number the number
   * @param subCollectorTypes the sub collector types
   * @param subDataTypes the sub data types
   * @param subStatsTypes the sub stats types
   * @param subStatsItems the sub stats items
   * @param subSortTypes the sub sort types
   * @param subSortDirections the sub sort directions
   * @param subStart the sub start
   * @param subNumber the sub number
   * @param segmentRegistration the segment registration
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasDataLongFull(String collectorType, TreeSet<String> statsItems,
      String sortType, String sortDirection, Integer start, Integer number,
      String[] subCollectorTypes, String[] subDataTypes,
      String[] subStatsTypes, TreeSet<String>[] subStatsItems,
      String[] subSortTypes, String[] subSortDirections, Integer[] subStart,
      Integer[] subNumber, boolean segmentRegistration) throws IOException {
    super(collectorType, CodecUtil.DATA_TYPE_LONG, statsItems, sortType,
        sortDirection, start, number, subCollectorTypes, subDataTypes,
        subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
        subStart, subNumber, new MtasDataLongOperations(),
        segmentRegistration);
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#getItem(int)
   */
  @Override
  protected MtasDataItemLongFull getItem(int i) {
    return new MtasDataItemLongFull(ArrayUtils.toPrimitive(fullValueList[i]),
        hasSub() ? subCollectorListNextLevel[i] : null, statsItems, sortType,
        sortDirection, errorNumber[i], errorList[i]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long, long)
   */
  @Override
  public MtasDataCollector<?, ?> add(long valueSum, long valueN)
      throws IOException {
    throw new IOException("not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long[], int)
   */
  @Override
  public MtasDataCollector<?, ?> add(long[] values, int number)
      throws IOException {
    MtasDataCollector<?, ?> dataCollector = add();
    setValue(newCurrentPosition, ArrayUtils.toObject(values), number,
        newCurrentExisting);
    return dataCollector;
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double, long)
   */
  @Override
  public MtasDataCollector<?, ?> add(double valueSum, long valueN)
      throws IOException {
    throw new IOException("not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double[], int)
   */
  @Override
  public MtasDataCollector<?, ?> add(double[] values, int number)
      throws IOException {
    MtasDataCollector<?, ?> dataCollector = add();
    Long[] newValues = new Long[number];
    for (int i = 0; i < values.length; i++)
      newValues[i] = Double.valueOf(values[i]).longValue();
    setValue(newCurrentPosition, newValues, number, newCurrentExisting);
    return dataCollector;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[],
   * long, long)
   */
  @Override
  public MtasDataCollector<?, ?>[] add(String[] keys, long valueSum,
      long valueN) throws IOException {
    throw new IOException("not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[],
   * long[], int)
   */
  @Override
  public MtasDataCollector<?, ?>[] add(String[] keys, long[] values,
      int number) throws IOException {
    if (keys != null && keys.length > 0) {
      MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector<?, ?>[keys.length];
      for (int i = 0; i < keys.length; i++) {
        subCollectors[i] = add(keys[i]);
        setValue(newCurrentPosition, ArrayUtils.toObject(values), number,
            newCurrentExisting);
      }
      return subCollectors;
    } else {
      return null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[],
   * double, long)
   */
  @Override
  public MtasDataCollector<?, ?>[] add(String[] keys, double valueSum,
      long valueN) throws IOException {
    throw new IOException("not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[],
   * double[], int)
   */
  @Override
  public MtasDataCollector<?, ?>[] add(String[] keys, double[] values,
      int number) throws IOException {
    if (keys != null && keys.length > 0) {
      Long[] newValues = new Long[number];
      for (int i = 0; i < values.length; i++)
        newValues[i] = Double.valueOf(values[i]).longValue();
      MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector<?, ?>[keys.length];
      for (int i = 0; i < keys.length; i++) {
        subCollectors[i] = add(keys[i]);
        setValue(newCurrentPosition, newValues, number, newCurrentExisting);
      }
      return subCollectors;
    } else {
      return null;
    }
  }

  /* (non-Javadoc)
   * @see mtas.codec.util.DataCollector.MtasDataCollector#compareForComputingSegment(java.lang.Number, java.lang.Number)
   */
  @Override
  protected boolean compareForComputingSegment(Long value, Long boundary) {
    return value >= boundary;
  }

  /* (non-Javadoc)
   * @see mtas.codec.util.DataCollector.MtasDataCollector#minimumForComputingSegment(java.lang.Number, java.lang.Number)
   */
  @Override
  protected Long minimumForComputingSegment(Long value, Long boundary) {
    return Math.min(value, boundary);
  }

  /* (non-Javadoc)
   * @see mtas.codec.util.DataCollector.MtasDataCollector#minimumForComputingSegment()
   */
  @Override
  protected Long minimumForComputingSegment() {
    return Collections.min(segmentValueMaxList);
  }

  /* (non-Javadoc)
   * @see mtas.codec.util.DataCollector.MtasDataCollector#boundaryForComputingSegment()
   */
  @Override
  protected Long boundaryForComputingSegment() {
    Long boundary = boundaryForSegment();
    long correctionBoundary = 0;
    for (String otherSegmentName : segmentValueMaxListMin.keySet()) {
      if (!otherSegmentName.equals(segmentName)) {
        Long otherBoundary = segmentValueBoundary.get(otherSegmentName);
        if (otherBoundary != null) {
          correctionBoundary += Math.max(0, otherBoundary - boundary);
        }
      }
    }
    return boundary + correctionBoundary;
  }

  /* (non-Javadoc)
   * @see mtas.codec.util.DataCollector.MtasDataCollector#boundaryForSegment()
   */
  @Override
  protected Long boundaryForSegment() {
    long thisMin = segmentValueMaxListMin.get(segmentName);
    Long boundary = Math.floorDiv(thisMin, segmentNumber);
    return boundary;
  }

}
