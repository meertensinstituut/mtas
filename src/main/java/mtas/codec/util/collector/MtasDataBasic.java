package mtas.codec.util.collector;

import mtas.codec.util.CodecUtil;
import mtas.codec.util.DataCollector;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

abstract class MtasDataBasic<T1 extends Number & Comparable<T1>, T2 extends Number & Comparable<T2>>
    extends MtasDataCollector<T1, T2> implements Serializable {
  private static final long serialVersionUID = 1L;

  T1[] basicValueSumList = null;
  long[] basicValueNList = null;
  private transient T1[] newBasicValueSumList = null;
  private transient long[] newBasicValueNList = null;
  protected MtasDataOperations<T1, T2> operations;

  public MtasDataBasic(String collectorType, String dataType,
      SortedSet<String> statsItems, String sortType, String sortDirection,
      Integer start, Integer number, String[] subCollectorTypes,
      String[] subDataTypes, String[] subStatsTypes,
      SortedSet<String>[] subStatsItems, String[] subSortTypes,
      String[] subSortDirections, Integer[] subStart, Integer[] subNumber,
      MtasDataOperations<T1, T2> operations, String segmentRegistration,
      String boundary) throws IOException {
    super(collectorType, dataType, CodecUtil.STATS_BASIC, statsItems, sortType,
        sortDirection, start, number, subCollectorTypes, subDataTypes,
        subStatsTypes, subStatsItems, subSortTypes, subSortDirections, subStart,
        subNumber, segmentRegistration, boundary);
    this.operations = operations;
  }

  @Override
  public final void error(String error) throws IOException {
    add(false);
    setError(newCurrentPosition, error, newCurrentExisting);
  }

  @Override
  public final void error(String key, String error) throws IOException {
    if (key != null) {
      add(key, false);
      setError(newCurrentPosition, error, newCurrentExisting);
    }
  }

  protected void setError(int newPosition, String error,
      boolean currentExisting) {
    if (!currentExisting) {
      newBasicValueSumList[newPosition] = operations.getZero1();
      newBasicValueNList[newPosition] = 0;
    }
    newErrorNumber[newPosition]++;
    if (newErrorList[newPosition].containsKey(error)) {
      newErrorList[newPosition].put(error,
          newErrorList[newPosition].get(error) + 1);
    } else {
      newErrorList[newPosition].put(error, 1);
    }
  }

  protected void setValue(int newPosition, T1 valueSum, long valueN,
      boolean currentExisting) {
    if (valueN > 0) {
      if (currentExisting) {
        newBasicValueSumList[newPosition] = operations
            .add11(newBasicValueSumList[newPosition], valueSum);
        newBasicValueNList[newPosition] += valueN;
      } else {
        newBasicValueSumList[newPosition] = valueSum;
        newBasicValueNList[newPosition] = valueN;
      }
    }
  }

  protected void setValue(int newPosition, T1[] values, int number,
      boolean currentExisting) {
    if (number > 0) {
      T1 valueSum = null;
      for (int i = 0; i < number; i++) {
        valueSum = (i == 0) ? values[i] : operations.add11(valueSum, values[i]);
      }
      if (currentExisting) {
        newBasicValueSumList[newPosition] = operations
            .add11(newBasicValueSumList[newPosition], valueSum);
        newBasicValueNList[newPosition] += number;
      } else {
        newBasicValueSumList[newPosition] = valueSum;
        newBasicValueNList[newPosition] = number;
      }
    }
  }

  @Override
  protected final void increaseNewListSize() throws IOException {
    // register old situation
    int tmpOldSize = newKeyList.length;
    int tmpNewPosition = newPosition;
    // increase
    super.increaseNewListSize();
    // reconstruct
    T1[] tmpNewBasicValueList = newBasicValueSumList;
    long[] tmpNewBasicValueNList = newBasicValueNList;
    newBasicValueSumList = operations.createVector1(newSize);
    newBasicValueNList = new long[newSize];
    newPosition = tmpNewPosition;
    System.arraycopy(tmpNewBasicValueList, 0, newBasicValueSumList, 0,
        tmpOldSize);
    System.arraycopy(tmpNewBasicValueNList, 0, newBasicValueNList, 0,
        tmpOldSize);
  }

  @Override
  public void reduceToSegmentKeys() {
    if (segmentRegistration != null && size > 0) {
      int sizeCopy = size;
      String[] keyListCopy = keyList.clone();
      T1[] basicValueSumListCopy = basicValueSumList.clone();
      long[] basicValueNListCopy = basicValueNList.clone();
      size = 0;
      for (int i = 0; i < sizeCopy; i++) {
        if (segmentKeys.contains(keyListCopy[i])) {
          keyList[size] = keyListCopy[i];
          basicValueSumList[size] = basicValueSumListCopy[i];
          basicValueNList[size] = basicValueNListCopy[i];
          size++;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void reduceToKeys(Set<String> keys) {
    if (size > 0) {
      int sizeCopy = size;
      String[] keyListCopy = keyList.clone();
      int[] errorNumberCopy = errorNumber.clone();
      HashMap<String, Integer>[] errorListCopy = errorList.clone();
      int[] sourceNumberListCopy = sourceNumberList.clone();
      T1[] basicValueSumListCopy = basicValueSumList.clone();
      long[] basicValueNListCopy = basicValueNList.clone();
      keyList = new String[keys.size()];
      errorNumber = new int[keys.size()];
      errorList = new HashMap[keys.size()];
      sourceNumberList = new int[keys.size()];
      basicValueSumList = operations.createVector1(keys.size());
      basicValueNList = new long[keys.size()];
      size = 0;
      for (int i = 0; i < sizeCopy; i++) {
        if (keys.contains(keyListCopy[i])) {
          keyList[size] = keyListCopy[i];
          errorNumber[size] = errorNumberCopy[i];
          errorList[size] = errorListCopy[i];
          sourceNumberList[size] = sourceNumberListCopy[i];
          basicValueSumList[size] = basicValueSumListCopy[i];
          basicValueNList[size] = basicValueNListCopy[i];
          size++;
        }
      }
    }
  }

  @Override
  protected void copyToNew(int position, int newPosition) {
    newBasicValueSumList[newPosition] = basicValueSumList[position];
    newBasicValueNList[newPosition] = basicValueNList[position];
  }

  @Override
  protected void copyFromNew() {
    basicValueSumList = newBasicValueSumList;
    basicValueNList = newBasicValueNList;
  }

  @Override
  protected void remapData(int[][] mapping) throws IOException {
    super.remapData(mapping);
    T1[] originalBasicValueSumList = basicValueSumList.clone();
    long[] originalBasicValueNList = basicValueNList.clone();
    basicValueSumList = operations.createVector1(mapping.length);
    basicValueNList = new long[mapping.length];
    for (int i = 0; i < mapping.length; i++) {
      for (int j = 0; j < mapping[i].length; j++) {
        if (j == 0) {
          setValue(i, originalBasicValueSumList[mapping[i][j]],
              originalBasicValueNList[mapping[i][j]], false);
        } else {
          setValue(i, originalBasicValueSumList[mapping[i][j]],
              originalBasicValueNList[mapping[i][j]], true);
        }
      }
    }
    basicValueSumList = newBasicValueSumList;
    basicValueNList = newBasicValueNList;
  }

  @Override
  public void merge(MtasDataCollector<?, ?> newDataCollector,
      Map<MtasDataCollector<?, ?>, MtasDataCollector<?, ?>> map,
      boolean increaseSourceNumber) throws IOException {
    closeNewList();
    if (!collectorType.equals(newDataCollector.getCollectorType())
        || !dataType.equals(newDataCollector.getDataType())
        || !statsType.equals(newDataCollector.getStatsType())
        || !(newDataCollector instanceof MtasDataBasic)) {
      throw new IOException("cannot merge different dataCollectors");
    } else {
      segmentRegistration = null;
      @SuppressWarnings("unchecked")
      MtasDataBasic<T1, T2> newMtasDataBasic = (MtasDataBasic<T1, T2>) newDataCollector;
      newMtasDataBasic.closeNewList();
      initNewList(newMtasDataBasic.getSize());
      switch (collectorType) {
        case DataCollector.COLLECTOR_TYPE_LIST:
          map.put(newDataCollector, this);
          for (int i = 0; i < newMtasDataBasic.getSize(); i++) {
            MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector[1];
            subCollectors[0] = add(newMtasDataBasic.keyList[i],
              increaseSourceNumber);
            setError(newCurrentPosition, newMtasDataBasic.errorNumber[i],
              newMtasDataBasic.errorList[i], newCurrentExisting);
            setValue(newCurrentPosition, newMtasDataBasic.basicValueSumList[i],
              newMtasDataBasic.basicValueNList[i], newCurrentExisting);
            if (hasSub() && newMtasDataBasic.hasSub()) {
              // single key implies exactly one subCollector if hasSub
              subCollectors[0].merge(
                newMtasDataBasic.subCollectorListNextLevel[i], map,
                increaseSourceNumber);
            }
          }
          closeNewList();
          break;

        case DataCollector.COLLECTOR_TYPE_DATA:
          map.put(newDataCollector, this);
          if (newMtasDataBasic.getSize() > 0) {
            MtasDataCollector<?, ?> subCollector = add(increaseSourceNumber);
            setError(newCurrentPosition, newMtasDataBasic.errorNumber[0],
              newMtasDataBasic.errorList[0], newCurrentExisting);
            setValue(newCurrentPosition, newMtasDataBasic.basicValueSumList[0],
              newMtasDataBasic.basicValueNList[0], newCurrentExisting);
            if (hasSub() && newMtasDataBasic.hasSub()) {
              subCollector.merge(newMtasDataBasic.subCollectorNextLevel, map,
                increaseSourceNumber);
            }
          }
          closeNewList();
          break;

        default:
          throw new IOException("cannot merge " + collectorType);
      }
    }
  }

  @Override
  public final void initNewList(int maxNumberOfTerms) throws IOException {
    super.initNewList(maxNumberOfTerms);
    initNewListBasic();
  }

  @Override
  public final void initNewList(int maxNumberOfTerms, String segmentName,
      int segmentNumber, String boundary) throws IOException {
    super.initNewList(maxNumberOfTerms, segmentName, segmentNumber, boundary);
    initNewListBasic();
  }

  private void initNewListBasic() {
    newBasicValueSumList = operations.createVector1(newSize);
    newBasicValueNList = new long[newSize];
  }
}
