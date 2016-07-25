package mtas.codec.util.collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.TreeSet;
import mtas.codec.util.CodecUtil;
import mtas.codec.util.DataCollector;
import mtas.codec.util.DataCollector.MtasDataCollector;

/**
 * The Class MtasDataBasic.
 *
 * @param <T1> the generic type
 * @param <T2> the generic type
 * @param <T3> the generic type
 */
abstract class MtasDataBasic<T1 extends Number, T2 extends Number, T3 extends MtasDataItem<T1>>
    extends MtasDataCollector<T1, T3> implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The new basic value sum list. */
  protected T1[] basicValueSumList = null, newBasicValueSumList = null;

  /** The new basic value n list. */
  protected long[] basicValueNList = null, newBasicValueNList = null;

  /** The operations. */
  protected MtasDataOperations<T1, T2> operations;

  /**
   * Instantiates a new mtas data basic.
   *
   * @param collectorType the collector type
   * @param dataType the data type
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
   * @param operations the operations
   * @param segmentRegistration the segment registration
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasDataBasic(String collectorType, String dataType,
      TreeSet<String> statsItems, String sortType, String sortDirection,
      Integer start, Integer number, String[] subCollectorTypes,
      String[] subDataTypes, String[] subStatsTypes,
      TreeSet<String>[] subStatsItems, String[] subSortTypes,
      String[] subSortDirections, Integer[] subStart, Integer[] subNumber,
      MtasDataOperations<T1, T2> operations, boolean segmentRegistration)
      throws IOException {
    super(collectorType, dataType, CodecUtil.STATS_BASIC, statsItems, sortType,
        sortDirection, start, number, subCollectorTypes, subDataTypes,
        subStatsTypes, subStatsItems, subSortTypes, subSortDirections, subStart,
        subNumber, segmentRegistration);
    this.operations = operations;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.codec.util.DataCollector.MtasDataCollector#error(java.lang.String)
   */
  @Override
  public final void error(String error) throws IOException {
    add();
    setError(newCurrentPosition, error, newCurrentExisting);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.codec.util.DataCollector.MtasDataCollector#error(java.lang.String[],
   * java.lang.String)
   */
  @Override
  public final void error(String[] keys, String error) throws IOException {
    if (keys != null && keys.length > 0) {
      for (int i = 0; i < keys.length; i++) {
        add(keys[i]);
        setError(newCurrentPosition, error, newCurrentExisting);
      }
    }
  }

  /**
   * Sets the error.
   *
   * @param newPosition the new position
   * @param error the error
   * @param currentExisting the current existing
   */
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

  /**
   * Sets the value.
   *
   * @param newPosition the new position
   * @param valueSum the value sum
   * @param valueN the value n
   * @param currentExisting the current existing
   */
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

  /**
   * Sets the value.
   *
   * @param newPosition the new position
   * @param values the values
   * @param number the number
   * @param currentExisting the current existing
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#increaseNewListSize()
   */
  @Override
  protected final void increaseNewListSize() {
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

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#copyToNew(int, int)
   */
  @Override
  protected void copyToNew(int position, int newPosition) {
    newBasicValueSumList[newPosition] = basicValueSumList[position];
    newBasicValueNList[newPosition] = basicValueNList[position];
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#copyFromNew()
   */
  @Override
  protected void copyFromNew() {
    basicValueSumList = newBasicValueSumList;
    basicValueNList = newBasicValueNList;
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#remapData(int[][])
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#merge(mtas.codec.util.
   * DataCollector.MtasDataCollector)
   */
  @Override
  public void merge(MtasDataCollector<?, ?> newDataCollector)
      throws IOException {
    closeNewList();
    if (!collectorType.equals(newDataCollector.getCollectorType())
        || !dataType.equals(newDataCollector.getDataType())
        || !statsType.equals(newDataCollector.getStatsType())
        || !(newDataCollector instanceof MtasDataBasic)) {
      throw new IOException("cannot merge different dataCollectors");
    } else {
      MtasDataBasic<T1, T2, T3> newMtasDataBasic = (MtasDataBasic<T1, T2, T3>) newDataCollector;
      newMtasDataBasic.closeNewList();
      initNewList(newMtasDataBasic.getSize());
      if (collectorType.equals(DataCollector.COLLECTOR_TYPE_LIST)) {
        String[] keys = new String[1];
        for (int i = 0; i < newMtasDataBasic.getSize(); i++) {
          MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector[1];
          subCollectors[0] = add(newMtasDataBasic.keyList[i]);
          setError(newCurrentPosition, newMtasDataBasic.errorNumber[i],
              newMtasDataBasic.errorList[i], newCurrentExisting);
          setValue(newCurrentPosition, newMtasDataBasic.basicValueSumList[i],
              newMtasDataBasic.basicValueNList[i], newCurrentExisting);
          if (hasSub() && newMtasDataBasic.hasSub()) {
            // single key implies exactly one subCollector if hasSub
            subCollectors[0]
                .merge(newMtasDataBasic.subCollectorListNextLevel[i]);
          }
        }
        closeNewList();
      } else if (collectorType.equals(DataCollector.COLLECTOR_TYPE_DATA)) {
        if (newMtasDataBasic.getSize() > 0) {
          MtasDataCollector<?, ?> subCollector = add();
          setError(newCurrentPosition, newMtasDataBasic.errorNumber[0],
              newMtasDataBasic.errorList[0], newCurrentExisting);
          setValue(newCurrentPosition, newMtasDataBasic.basicValueSumList[0],
              newMtasDataBasic.basicValueNList[0], newCurrentExisting);
          if (hasSub() && newMtasDataBasic.hasSub()) {
            subCollector.merge(newMtasDataBasic.subCollectorNextLevel);
          }
        }
        closeNewList();
      } else {
        throw new IOException("cannot merge " + collectorType);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#initNewList(int)
   */
  @Override
  public final void initNewList(int maxNumberOfTerms) throws IOException {
    super.initNewList(maxNumberOfTerms);
    initNewListBasic(maxNumberOfTerms);
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.util.DataCollector.MtasDataCollector#initNewList(int,
   * java.lang.String)
   */
  @Override
  public final void initNewList(int maxNumberOfTerms, String segmentName,
      int segmentNumber) throws IOException {
    super.initNewList(maxNumberOfTerms, segmentName, segmentNumber);
    initNewListBasic(maxNumberOfTerms);
  }

  /**
   * Inits the new list basic.
   *
   * @param maxNumberOfTerms the max number of terms
   */
  private void initNewListBasic(int maxNumberOfTerms) {
    newBasicValueSumList = operations.createVector1(newSize);
    newBasicValueNList = new long[newSize];
  }

}
