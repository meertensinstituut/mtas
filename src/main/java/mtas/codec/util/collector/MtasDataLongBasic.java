package mtas.codec.util.collector;

import mtas.codec.util.CodecUtil;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;

public class MtasDataLongBasic extends MtasDataBasic<Long, Double> {
  private static final long serialVersionUID = 1L;

  public MtasDataLongBasic(String collectorType, SortedSet<String> statsItems,
      String sortType, String sortDirection, Integer start, Integer number,
      String[] subCollectorTypes, String[] subDataTypes, String[] subStatsTypes,
      SortedSet<String>[] subStatsItems, String[] subSortTypes,
      String[] subSortDirections, Integer[] subStart, Integer[] subNumber,
      String segmentRegistration, String boundary) throws IOException {
    super(collectorType, CodecUtil.DATA_TYPE_LONG, statsItems, sortType,
        sortDirection, start, number, subCollectorTypes, subDataTypes,
        subStatsTypes, subStatsItems, subSortTypes, subSortDirections, subStart,
        subNumber, new MtasDataLongOperations(), segmentRegistration, boundary);
  }

  @Override
  protected MtasDataItemLongBasic getItem(int i) {
    if (i < 0 || i >= size) {
      return null;
    }
    return new MtasDataItemLongBasic(basicValueSumList[i], basicValueNList[i],
      hasSub() ? subCollectorListNextLevel[i] : null, getStatsItems(),
      sortType, sortDirection, errorNumber[i], errorList[i],
      sourceNumberList[i]);
  }

  @Override
  public MtasDataCollector<?, ?> add(long valueSum, long valueN)
      throws IOException {
    MtasDataCollector<?, ?> dataCollector = add(false);
    setValue(newCurrentPosition, valueSum, valueN, newCurrentExisting);
    return dataCollector;
  }

  @Override
  public MtasDataCollector<?, ?> add(long[] values, int number)
      throws IOException {
    MtasDataCollector<?, ?> dataCollector = add(false);
    setValue(newCurrentPosition, ArrayUtils.toObject(values), number,
        newCurrentExisting);
    return dataCollector;
  }

  @Override
  public MtasDataCollector<?, ?> add(double valueSum, long valueN)
      throws IOException {
    MtasDataCollector<?, ?> dataCollector = add(false);
    setValue(newCurrentPosition, Double.valueOf(valueSum).longValue(), valueN,
        newCurrentExisting);
    return dataCollector;
  }

  @Override
  public MtasDataCollector<?, ?> add(double[] values, int number)
      throws IOException {
    MtasDataCollector<?, ?> dataCollector = add(false);
    Long[] newValues = new Long[number];
    for (int i = 0; i < values.length; i++)
      newValues[i] = Double.valueOf(values[i]).longValue();
    setValue(newCurrentPosition, newValues, number, newCurrentExisting);
    return dataCollector;
  }

  @Override
  public MtasDataCollector<?, ?> add(String key, long valueSum, long valueN)
      throws IOException {
    if (key == null) {
      return null;
    }
    MtasDataCollector<?, ?> subCollector = add(key, false);
    setValue(newCurrentPosition, valueSum, valueN, newCurrentExisting);
    return subCollector;
  }

  @Override
  public MtasDataCollector<?, ?> add(String key, long[] values, int number)
      throws IOException {
    if (key == null) {
      return null;
    }
    MtasDataCollector<?, ?> subCollector = add(key, false);
    setValue(newCurrentPosition, ArrayUtils.toObject(values), number,
      newCurrentExisting);
    return subCollector;
  }

  @Override
  public MtasDataCollector<?, ?> add(String key, double valueSum, long valueN)
      throws IOException {
    if (key == null) {
      return null;
    }
    MtasDataCollector<?, ?> subCollector = add(key, false);
    setValue(newCurrentPosition, Double.valueOf(valueSum).longValue(), valueN,
      newCurrentExisting);
    return subCollector;
  }

  @Override
  public MtasDataCollector<?, ?> add(String key, double[] values, int number)
      throws IOException {
    if (key == null) {
      return null;
    }
    Long[] newValues = new Long[number];
    for (int i = 0; i < values.length; i++)
      newValues[i] = Double.valueOf(values[i]).longValue();
    MtasDataCollector<?, ?> subCollector = add(key, false);
    setValue(newCurrentPosition, newValues, number, newCurrentExisting);
    return subCollector;
  }

  @Override
  protected boolean compareWithBoundary(Long value, Long boundary)
      throws IOException {
    switch (segmentRegistration) {
      case SEGMENT_SORT_ASC:
      case SEGMENT_BOUNDARY_ASC:
        return value <= boundary;
      case SEGMENT_SORT_DESC:
      case SEGMENT_BOUNDARY_DESC:
        return value >= boundary;
      default:
        throw new IOException(
          "can't compare for segmentRegistration " + segmentRegistration);
    }
  }

  @Override
  protected Long lastForComputingSegment(Long value, Long boundary)
      throws IOException {
    switch (segmentRegistration) {
      case SEGMENT_SORT_ASC:
      case SEGMENT_BOUNDARY_ASC:
        return Math.max(value, boundary);
      case SEGMENT_SORT_DESC:
      case SEGMENT_BOUNDARY_DESC:
        return Math.min(value, boundary);
      default:
        throw new IOException(
          "can't compute last for segmentRegistration " + segmentRegistration);
    }
  }

  @Override
  protected Long lastForComputingSegment() throws IOException {
    switch (segmentRegistration) {
      case SEGMENT_SORT_ASC:
      case SEGMENT_BOUNDARY_ASC:
        return Collections.max(segmentValueTopList);
      case SEGMENT_SORT_DESC:
      case SEGMENT_BOUNDARY_DESC:
        return Collections.min(segmentValueTopList);
      default:
        throw new IOException(
          "can't compute last for segmentRegistration " + segmentRegistration);
    }
  }

  @Override
  protected Long boundaryForSegmentComputing(String segmentName) throws IOException {
    if (!segmentRegistration.equals(SEGMENT_SORT_ASC) && !segmentRegistration.equals(SEGMENT_SORT_DESC)) {
      throw new IOException("can't compute boundary for segmentRegistration "
        + segmentRegistration);
    }

    Long boundary = boundaryForSegment(segmentName);
    if (boundary == null) {
      return null;
    }
    if (segmentRegistration.equals(SEGMENT_SORT_DESC)) {
      long correctionBoundary = 0;
      for (String otherSegmentName : segmentValueTopListLast.keySet()) {
        if (!otherSegmentName.equals(segmentName)) {
          Long otherBoundary = segmentValuesBoundary.get(otherSegmentName);
          if (otherBoundary != null) {
            correctionBoundary += Math.max(0, otherBoundary - boundary);
          }
        }
      }
      return boundary + correctionBoundary;
    } else {
      return boundary;
    }
  }

  @Override
  protected Long boundaryForSegment(String segmentName) throws IOException {
    if (!segmentRegistration.equals(SEGMENT_SORT_ASC)
      && !segmentRegistration.equals(SEGMENT_SORT_DESC)) {
      throw new IOException("can't compute boundary for segmentRegistration " + segmentRegistration);
    }
    Long thisLast = segmentValueTopListLast.get(segmentName);
    if (thisLast == null) {
      return null;
    } else if (segmentRegistration.equals(SEGMENT_SORT_ASC)) {
      return thisLast * segmentNumber;
    } else {
      return Math.floorDiv(thisLast, segmentNumber);
    }
  }

  @Override
  protected Long stringToBoundary(String boundary, Integer segmentNumber)
      throws IOException {
    if (!segmentRegistration.equals(SEGMENT_BOUNDARY_ASC)
      && !segmentRegistration.equals(SEGMENT_BOUNDARY_DESC)) {
      throw new IOException(
        "not available for segmentRegistration " + segmentRegistration);
    }
    if (segmentNumber == null) {
      return Long.valueOf(boundary);
    } else {
      return Math.floorDiv(Long.parseLong(boundary), segmentNumber);
    }
  }

  @Override
  public boolean validateSegmentBoundary(Object o) throws IOException {
    if (!(o instanceof Long)) {
      throw new IOException("incorrect type ");
    }
    return validateWithSegmentBoundary((Long) o);
  }
}
