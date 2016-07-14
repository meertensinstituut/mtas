package mtas.codec.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * The Class DataCollector.
 */
public class DataCollector {

  /** The collector type list. */
  public static String COLLECTOR_TYPE_LIST = "list";
  
  /** The collector type data. */
  public static String COLLECTOR_TYPE_DATA = "data";

 /**
  * Gets the collector.
  *
  * @param collectorType the collector type
  * @param dataType the data type
  * @param statsType the stats type
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
  * @return the collector
  * @throws IOException Signals that an I/O exception has occurred.
  */
 public static MtasDataCollector<?, ?> getCollector(String collectorType,
      String dataType, String statsType, TreeSet<String> statsItems,
      String sortType, String sortDirection, Integer start, Integer number,
      String[] subCollectorTypes, String[] subDataTypes, String[] subStatsTypes,
      TreeSet<String>[] subStatsItems, String[] subSortTypes,
      String[] subSortDirections, Integer[] subStart, Integer[] subNumber)
      throws IOException {
    if (dataType != null && dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
      if (statsType.equals(CodecUtil.STATS_BASIC)) {
        return new MtasDataLongBasic(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber);
      } else if (statsType.equals(CodecUtil.STATS_ADVANCED)) {
        return new MtasDataLongAdvanced(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber);
      } else if (statsType.equals(CodecUtil.STATS_FULL)) {
        return new MtasDataLongFull(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber);
      } else {
        throw new IOException("unknown statsType " + statsType);
      }
    } else if (dataType != null
        && dataType.equals(CodecUtil.DATA_TYPE_DOUBLE)) {
      if (statsType.equals(CodecUtil.STATS_BASIC)) {
        return new MtasDataDoubleBasic(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber);
      } else if (statsType.equals(CodecUtil.STATS_ADVANCED)) {
        return new MtasDataDoubleAdvanced(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber);
      } else if (statsType.equals(CodecUtil.STATS_FULL)) {
        return new MtasDataDoubleFull(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber);
      } else {
        throw new IOException("unknown statsType " + statsType);
      }
    } else {
      throw new IOException("unknown dataType " + dataType);
    }
  }

  /**
   * The Class MtasDataCollector.
   *
   * @param <T1> the generic type
   * @param <T2> the generic type
   */
  public abstract static class MtasDataCollector<T1 extends Number, T2 extends MtasDataItem<T1>>
      implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The size. */
    // size and position current level
    private int size;

    /** The position. */
    protected int position;

    /** The collector type. */
    // properties collector
    protected String collectorType;
    
    /** The stats type. */
    protected String statsType;
    
    /** The data type. */
    protected String dataType;
    
    /** The stats items. */
    protected TreeSet<String> statsItems;
    
    /** The sort type. */
    protected String sortType;
    
    /** The sort direction. */
    protected String sortDirection;
    
    /** The start. */
    protected Integer start;
    
    /** The number. */
    protected Integer number;

    /** The error number. */
    // error
    protected int[] errorNumber;
    
    /** The error list. */
    protected HashMap<String, Integer>[] errorList;

    /** The key list. */
    // administration keys
    protected String[] keyList;

    /** The has sub. */
    // subcollectors properties
    private boolean hasSub;
    
    /** The sub collector types. */
    private String[] subCollectorTypes;
    
    /** The sub data types. */
    private String[] subDataTypes;
    
    /** The sub stats types. */
    private String[] subStatsTypes;
    
    /** The sub stats items. */
    private TreeSet<String>[] subStatsItems;
    
    /** The sub sort types. */
    private String[] subSortTypes;
    
    /** The sub sort directions. */
    private String[] subSortDirections;
    
    /** The sub start. */
    private Integer[] subStart;
    
    /** The sub number. */
    private Integer[] subNumber;

    /** The sub collector list next level. */
    // subcollectors next level
    protected MtasDataCollector<?, ?>[] subCollectorListNextLevel = null;
    
    /** The sub collector next level. */
    protected MtasDataCollector<?, ?> subCollectorNextLevel = null;

    /** The new current position. */
    // administration for adding
    protected int newSize, newPosition, newCurrentPosition;
    
    /** The new current existing. */
    protected boolean newCurrentExisting;
    
    /** The new key list. */
    protected String[] newKeyList = null;
    
    /** The new error number. */
    protected int[] newErrorNumber;
    
    /** The new error list. */
    protected HashMap<String, Integer>[] newErrorList;

    /** The new sub collector types. */
    // subcollectors properties for adding
    private String[] newSubCollectorTypes;
    
    /** The new sub data types. */
    private String[] newSubDataTypes;
    
    /** The new sub stats types. */
    private String[] newSubStatsTypes;
    
    /** The new sub stats items. */
    private TreeSet<String>[] newSubStatsItems;
    
    /** The new sub sort types. */
    private String[] newSubSortTypes;
    
    /** The new sub sort directions. */
    private String[] newSubSortDirections;
    
    /** The new sub start. */
    private Integer[] newSubStart;
    
    /** The new sub number. */
    private Integer[] newSubNumber;

    /** The new sub collector list next level. */
    // subcollectors next level for adding
    protected MtasDataCollector<?, ?>[] newSubCollectorListNextLevel = null;
    
    /** The new sub collector next level. */
    protected MtasDataCollector<?, ?> newSubCollectorNextLevel = null;

    /**
     * Instantiates a new mtas data collector.
     *
     * @param collectorType the collector type
     * @param dataType the data type
     * @param statsType the stats type
     * @param statsItems the stats items
     * @param sortType the sort type
     * @param sortDirection the sort direction
     * @param start the start
     * @param number the number
     */
    protected MtasDataCollector(String collectorType, String dataType,
        String statsType, TreeSet<String> statsItems, String sortType,
        String sortDirection, Integer start, Integer number) {
      // set properties
      this.collectorType = collectorType; // data or list
      this.dataType = dataType; // long or double
      this.statsType = statsType; // basic, advanced or full
      this.statsItems = statsItems; // sum, n, all, ...
      this.sortType = sortType;
      this.sortDirection = sortDirection;
      this.start = start;
      this.number = number;
      // initialize administration
      keyList = new String[0];
      errorNumber = new int[0];
      errorList = (HashMap<String, Integer>[]) new HashMap<?, ?>[0];
      size = 0;
      position = 0;
      // subCollectors properties
      hasSub = false;
      subCollectorTypes = null;
      subDataTypes = null;
      subStatsTypes = null;
      subStatsItems = null;
      subSortTypes = null;
      subSortDirections = null;
      subStart = null;
      subNumber = null;
      subCollectorListNextLevel = null;
      subCollectorNextLevel = null;
    }

    /**
     * Instantiates a new mtas data collector.
     *
     * @param collectorType the collector type
     * @param dataType the data type
     * @param statsType the stats type
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
     */
    protected MtasDataCollector(String collectorType, String dataType,
        String statsType, TreeSet<String> statsItems, String sortType,
        String sortDirection, Integer start, Integer number,
        String[] subCollectorTypes, String[] subDataTypes,
        String[] subStatsTypes, TreeSet<String>[] subStatsItems,
        String subSortTypes[], String[] subSortDirections, Integer[] subStart,
        Integer[] subNumber) {
      // initialize
      this(collectorType, dataType, statsType, statsItems, sortType,
          sortDirection, start, number);
      // initialize subCollectors
      if (subCollectorTypes != null) {
        hasSub = true;
        this.subCollectorTypes = subCollectorTypes;
        this.subDataTypes = subDataTypes;
        this.subStatsTypes = subStatsTypes;
        this.subStatsItems = subStatsItems;
        this.subSortTypes = subSortTypes;
        this.subSortDirections = subSortDirections;
        this.subStart = subStart;
        this.subNumber = subNumber;
        if (subCollectorTypes.length > 1) {
          newSubCollectorTypes = Arrays.copyOfRange(subCollectorTypes, 1,
              subCollectorTypes.length);
          newSubDataTypes = Arrays.copyOfRange(subDataTypes, 1,
              subStatsTypes.length);
          newSubStatsTypes = Arrays.copyOfRange(subStatsTypes, 1,
              subStatsTypes.length);
          newSubStatsItems = Arrays.copyOfRange(subStatsItems, 1,
              subStatsItems.length);
          newSubSortTypes = Arrays.copyOfRange(subSortTypes, 1,
              subSortTypes.length);
          newSubSortDirections = Arrays.copyOfRange(subSortDirections, 1,
              subSortDirections.length);
          newSubStart = Arrays.copyOfRange(subStart, 1, subStart.length);
          newSubNumber = Arrays.copyOfRange(subNumber, 1, subNumber.length);
        }
        newSubCollectorListNextLevel = new MtasDataCollector[0];
      }
    }

    /**
     * Merge.
     *
     * @param newDataCollector the new data collector
     * @throws IOException Signals that an I/O exception has occurred.
     */
    abstract public void merge(MtasDataCollector<?, ?> newDataCollector)
        throws IOException;

    /**
     * Inits the new list.
     *
     * @param maxNumberOfTerms the max number of terms
     */
    protected void initNewList(int maxNumberOfTerms) {
      position = 0;
      newPosition = 0;
      newCurrentPosition = 0;
      newSize = maxNumberOfTerms + size;
      newKeyList = new String[newSize];
      newErrorNumber = new int[newSize];
      newErrorList = (HashMap<String, Integer>[]) new HashMap<?, ?>[newSize];
      if (hasSub) {
        newSubCollectorListNextLevel = new MtasDataCollector[newSize];
      }
    }

    /**
     * Increase new list size.
     */
    protected void increaseNewListSize() {
      String[] tmpNewKeyList = newKeyList;
      int[] tmpNewErrorNumber = newErrorNumber;
      HashMap<String, Integer>[] tmpNewErrorList = newErrorList;
      int tmpNewSize = newSize;
      newSize = 2 * newSize;
      newKeyList = new String[newSize];
      newErrorNumber = new int[newSize];
      newErrorList = (HashMap<String, Integer>[]) new HashMap<?, ?>[newSize];
      System.arraycopy(tmpNewKeyList, 0, newKeyList, 0, tmpNewSize);
      System.arraycopy(tmpNewErrorNumber, 0, newErrorNumber, 0, tmpNewSize);
      System.arraycopy(tmpNewErrorList, 0, newErrorList, 0, tmpNewSize);
      if (hasSub) {
        MtasDataCollector<?, ?>[] tmpNewSubCollectorListNextLevel = newSubCollectorListNextLevel;
        newSubCollectorListNextLevel = new MtasDataCollector[newSize];
        System.arraycopy(tmpNewSubCollectorListNextLevel, 0,
            newSubCollectorListNextLevel, 0, tmpNewSize);
      }
    }

    /**
     * Adds the.
     *
     * @return the mtas data collector
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected final MtasDataCollector<?, ?> add() throws IOException {
      if (!collectorType.equals(COLLECTOR_TYPE_DATA)) {
        throw new IOException("collector should be " + COLLECTOR_TYPE_DATA);
      } else {
        if (newPosition > 0) {
          newCurrentExisting = true;
        } else if (position < getSize()) {
          // copy
          newKeyList[0] = keyList[0];
          newErrorNumber[0] = errorNumber[0];
          newErrorList[0] = errorList[0];
          if (hasSub) {
            newSubCollectorNextLevel = subCollectorNextLevel;
          }
          copyToNew(0, 0);
          newPosition = 1;
          position = 1;
          newCurrentExisting = true;
        } else {
          // add key
          newKeyList[0] = COLLECTOR_TYPE_DATA;
          newErrorNumber[0] = 0;
          newErrorList[0] = new HashMap<String, Integer>();
          newPosition = 1;
          newCurrentPosition = newPosition - 1;
          newCurrentExisting = false;
          // ready, only handle sub
          if (hasSub) {
            newSubCollectorNextLevel = getCollector(subCollectorTypes[0],
                subDataTypes[0], subStatsTypes[0], subStatsItems[0],
                subSortTypes[0], subSortDirections[0], subStart[0],
                subNumber[0], newSubCollectorTypes, newSubDataTypes,
                newSubStatsTypes, newSubStatsItems, newSubSortTypes,
                newSubSortDirections, newSubStart, newSubNumber);
          } else {
            newSubCollectorNextLevel = null;
          }
        }
        return newSubCollectorNextLevel;
      }
    }

    /**
     * Adds the.
     *
     * @param key the key
     * @return the mtas data collector
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected final MtasDataCollector<?, ?> add(String key) throws IOException {
      if (collectorType.equals(COLLECTOR_TYPE_DATA)) {
        throw new IOException("collector should be " + COLLECTOR_TYPE_LIST);
      } else if (key == null) {
        throw new IOException("key shouldn't be null");
      } else {
        // check previous added
        if ((newPosition > 0)
            && newKeyList[(newPosition - 1)].compareTo(key) >= 0) {
          int i = newPosition;
          do {
            i--;
            if (newKeyList[i].equals(key)) {
              newCurrentPosition = i;
              newCurrentExisting = true;
              if (subDataTypes != null) {
                return newSubCollectorListNextLevel[newCurrentPosition];
              } else {
                return null;
              }
            }
          } while ((i > 0) && (newKeyList[i].compareTo(key) > 0));
        }
        // move position in old list
        if (position < getSize()) {
          // just add smaller or equal items
          while (keyList[position].compareTo(key) <= 0) {
            if (newPosition == newSize) {
              increaseNewListSize();
            }
            // copy
            newKeyList[newPosition] = keyList[position];
            newErrorNumber[newPosition] = errorNumber[position];
            newErrorList[newPosition] = errorList[position];
            if (hasSub) {
              newSubCollectorListNextLevel[newPosition] = subCollectorListNextLevel[position];
            }
            copyToNew(position, newPosition);
            newPosition++;
            position++;
            // check if added key from list is right key
            if (newKeyList[(newPosition - 1)].equals(key)) {
              newCurrentPosition = newPosition - 1;
              newCurrentExisting = true;
              // ready
              if (hasSub) {
                return newSubCollectorListNextLevel[newCurrentPosition];
              } else {
                return null;
              }
              // stop if position exceeds size
            } else if (position == getSize()) {
              break;
            }
          }
        }
        // check size
        if (newPosition == newSize) {
          increaseNewListSize();
        }
        // add key
        newKeyList[newPosition] = key;
        newErrorNumber[newPosition] = 0;
        newErrorList[newPosition] = new HashMap<String, Integer>();
        newPosition++;
        newCurrentPosition = newPosition - 1;
        newCurrentExisting = false;
        // ready, only handle sub
        if (hasSub) {
          newSubCollectorListNextLevel[newCurrentPosition] = getCollector(
              subCollectorTypes[0], subDataTypes[0], subStatsTypes[0],
              subStatsItems[0], subSortTypes[0], subSortDirections[0],
              subStart[0], subNumber[0], newSubCollectorTypes, newSubDataTypes,
              newSubStatsTypes, newSubStatsItems, newSubSortTypes,
              newSubSortDirections, newSubStart, newSubNumber);
          return newSubCollectorListNextLevel[newCurrentPosition];
        } else {
          return null;
        }
      }
    }

    /**
     * Copy to new.
     *
     * @param position the position
     * @param newPosition the new position
     */
    protected abstract void copyToNew(int position, int newPosition);

    /**
     * Copy from new.
     */
    protected abstract void copyFromNew();

    /**
     * Sets the error.
     *
     * @param newPosition the new position
     * @param errorNumberItem the error number item
     * @param errorListItem the error list item
     * @param currentExisting the current existing
     */
    protected final void setError(int newPosition, int errorNumberItem,
        HashMap<String, Integer> errorListItem, boolean currentExisting) {
      if (currentExisting) {
        newErrorNumber[newPosition] += errorNumberItem;
        HashMap<String, Integer> item = newErrorList[newPosition];
        for (Entry<String, Integer> entry : errorListItem.entrySet()) {
          if (item.containsKey(entry.getKey())) {
            item.put(entry.getKey(),
                item.get(entry.getKey()) + entry.getValue());
          } else {
            item.put(entry.getKey(), entry.getValue());
          }
        }
      } else {
        newErrorNumber[newPosition] = errorNumberItem;
        newErrorList[newPosition] = errorListItem;
      }
    }

    /**
     * Sorted and unique.
     *
     * @param keyList the key list
     * @param size the size
     * @return true, if successful
     */
    private boolean sortedAndUnique(String[] keyList, int size) {
      for (int i = 1; i < size; i++) {
        if (keyList[(i - 1)].compareTo(keyList[i]) >= 0) {
          return false;
        }
      }
      return true;
    }

    /**
     * Compute sort and unique mapping.
     *
     * @param keyList the key list
     * @param size the size
     * @return the int[][]
     */
    private int[][] computeSortAndUniqueMapping(String[] keyList, int size) {
      if (size > 0) {
        SortedMap<String, int[]> sortedMap = new TreeMap<String, int[]>();
        for (int i = 0; i < size; i++) {
          if (sortedMap.containsKey(keyList[i])) {
            int[] previousList = sortedMap.get(keyList[i]);
            int[] newList = new int[previousList.length + 1];
            System.arraycopy(previousList, 0, newList, 0, previousList.length);
            newList[previousList.length] = i;
            sortedMap.put(keyList[i], newList);
          } else {
            sortedMap.put(keyList[i], new int[] { i });
          }
        }
        Collection<int[]> values = sortedMap.values();
        int[][] result = new int[sortedMap.size()][];
        return values.toArray(result);
      } else {
        return null;
      }
    }

    /**
     * Remap data.
     *
     * @param mapping the mapping
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected void remapData(int[][] mapping) throws IOException {
      // remap and merge keys
      String[] newKeyList = new String[mapping.length];
      int[] newErrorNumber = new int[mapping.length];
      HashMap<String, Integer>[] newErrorList = (HashMap<String, Integer>[]) new HashMap<?, ?>[mapping.length];
      for (int i = 0; i < mapping.length; i++) {
        newKeyList[i] = keyList[mapping[i][0]];
        for (int j = 0; j < mapping[i].length; j++) {
          if (j == 0) {
            newErrorNumber[i] = errorNumber[mapping[i][j]];
            newErrorList[i] = errorList[mapping[i][j]];
          } else {
            newErrorNumber[i] += errorNumber[mapping[i][j]];
            for (Entry<String, Integer> entry : errorList[mapping[i][j]]
                .entrySet()) {
              if (newErrorList[i].containsKey(entry.getKey())) {
                newErrorList[i].put(entry.getKey(),
                    newErrorList[i].get(entry.getKey()) + entry.getValue());
              } else {
                newErrorList[i].put(entry.getKey(), entry.getValue());
              }
            }
          }
        }
      }
      if (hasSub) {
        newSubCollectorListNextLevel = new MtasDataCollector<?, ?>[mapping.length];
        for (int i = 0; i < mapping.length; i++) {
          for (int j = 0; j < mapping[i].length; j++) {
            if (j == 0 || newSubCollectorListNextLevel[i] == null) {
              newSubCollectorListNextLevel[i] = subCollectorListNextLevel[mapping[i][j]];
            } else {
              newSubCollectorListNextLevel[i]
                  .merge(subCollectorListNextLevel[mapping[i][j]]);
            }
          }
        }
        subCollectorListNextLevel = newSubCollectorListNextLevel;
      }
      keyList = newKeyList;
      errorNumber = newErrorNumber;
      errorList = newErrorList;
      size = keyList.length;
      position = 0;
    }

    /**
     * Close new list.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void closeNewList() throws IOException {
      if (newSize > 0) {
        // add remaining old
        while (position < getSize()) {
          if (newPosition == newSize) {
            increaseNewListSize();
          }
          newKeyList[newPosition] = keyList[position];
          newErrorNumber[newPosition] = errorNumber[position];
          newErrorList[newPosition] = errorList[position];
          if (hasSub) {
            newSubCollectorListNextLevel[newPosition] = subCollectorListNextLevel[position];
          }
          copyToNew(position, newPosition);
          position++;
          newPosition++;
        }
        // copy
        keyList = newKeyList;
        errorNumber = newErrorNumber;
        errorList = newErrorList;
        subCollectorListNextLevel = newSubCollectorListNextLevel;
        copyFromNew();
        size = newPosition;
        // sort and merge
        if (!sortedAndUnique(keyList, getSize())) {
          remapData(computeSortAndUniqueMapping(keyList, getSize()));
        }
      }
      position = 0;
      newSize = 0;
      newPosition = 0;
      newCurrentPosition = 0;
    }

    /**
     * Gets the item.
     *
     * @param i the i
     * @return the item
     */
    abstract protected T2 getItem(int i);

    /**
     * Checks for sub.
     *
     * @return true, if successful
     */
    protected boolean hasSub() {
      return hasSub;
    }

    /**
     * Error.
     *
     * @param error the error
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void error(String error) throws IOException;

    /**
     * Error.
     *
     * @param keys the keys
     * @param error the error
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void error(String keys[], String error) throws IOException;

    /**
     * Adds the.
     *
     * @param valueSum the value sum
     * @param valueN the value n
     * @return the mtas data collector
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract MtasDataCollector<?, ?> add(long valueSum, long valueN)
        throws IOException;

    /**
     * Adds the.
     *
     * @param values the values
     * @param number the number
     * @return the mtas data collector
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract MtasDataCollector<?, ?> add(long[] values, int number)
        throws IOException;

    /**
     * Adds the.
     *
     * @param valueSum the value sum
     * @param valueN the value n
     * @return the mtas data collector
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract MtasDataCollector<?, ?> add(double valueSum, long valueN)
        throws IOException;

    /**
     * Adds the.
     *
     * @param values the values
     * @param number the number
     * @return the mtas data collector
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract MtasDataCollector<?, ?> add(double[] values, int number)
        throws IOException;

    /**
     * Adds the.
     *
     * @param keys the keys
     * @param valueSum the value sum
     * @param valueN the value n
     * @return the mtas data collector[]
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract MtasDataCollector<?, ?>[] add(String[] keys, long valueSum,
        long valueN) throws IOException;

    /**
     * Adds the.
     *
     * @param keys the keys
     * @param values the values
     * @param number the number
     * @return the mtas data collector[]
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract MtasDataCollector<?, ?>[] add(String[] keys, long[] values,
        int number) throws IOException;

    /**
     * Adds the.
     *
     * @param keys the keys
     * @param valueSum the value sum
     * @param valueN the value n
     * @return the mtas data collector[]
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract MtasDataCollector<?, ?>[] add(String[] keys,
        double valueSum, long valueN) throws IOException;

    /**
     * Adds the.
     *
     * @param keys the keys
     * @param values the values
     * @param number the number
     * @return the mtas data collector[]
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract MtasDataCollector<?, ?>[] add(String[] keys,
        double[] values, int number) throws IOException;

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return this.getClass().getCanonicalName() + ": " + collectorType + " - "
          + statsType + " " + statsItems + " " + hasSub;
    }

    /**
     * Gets the list.
     *
     * @return the list
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public final SortedMap<String, T2> getList() throws IOException {
      final TreeMap<String, T2> basicList = getBasicList();
      SortedMap<String, T2> list = null;
      if (sortType.equals(CodecUtil.SORT_TERM)) {
        if (sortDirection.equals(CodecUtil.SORT_ASC)) {
          list = basicList;
        } else if (sortDirection.equals(CodecUtil.SORT_DESC)) {
          list = basicList.descendingMap();
        } else {
          throw new IOException("unknown sort direction " + sortDirection);
        }
      } else if (CodecUtil.STATS_TYPES.contains(sortType)) {
        // comperator
        Comparator<String> valueComparator = new Comparator<String>() {
          @Override
          public int compare(String k1, String k2) {
            int compare = basicList.get(k1).compareTo(basicList.get(k2));
            return compare == 0 ? k1.compareTo(k2) : compare;
          }
        };
        SortedMap<String, T2> sortedByValues = new TreeMap<String, T2>(
            valueComparator);
        sortedByValues.putAll(basicList);
        list = sortedByValues;
      } else {
        throw new IOException("unknown sort type " + sortType);
      }
      int start = this.start == null ? 0 : this.start;
      if (number == null || (start == 0 && number >= list.size())) {
        // ful list
        return list;
      } else if (start < list.size() && number > 0) {
        // subset
        String startKey = null, endKey = null;
        int counter = 0;
        for (String key : list.keySet()) {
          if (start == counter) {
            startKey = key;
          } else if (start + number == counter) {
            endKey = key;
            break;
          } else {
            endKey = key;
          }
          counter++;
        }
        return list.subMap(startKey, endKey);
      } else {
        // empty set
        return new TreeMap<String, T2>();
      }
    }

    /**
     * Gets the basic list.
     *
     * @return the basic list
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private TreeMap<String, T2> getBasicList() throws IOException {
      closeNewList();
      TreeMap<String, T2> list = new TreeMap<String, T2>();
      if (collectorType.equals(COLLECTOR_TYPE_LIST)) {
        for (int i = 0; i < getSize(); i++) {
          T2 newItem = getItem(i);
          if (list.containsKey(keyList[i])) {
            newItem.add(list.get(keyList[i]));
          }
          list.put(keyList[i], newItem);
        }
        return list;
      } else {
        throw new IOException("type " + collectorType + " not supported");
      }
    }

    /**
     * Gets the data.
     *
     * @return the data
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public final T2 getData() throws IOException {
      closeNewList();
      if (collectorType.equals(COLLECTOR_TYPE_DATA)) {
        if (getSize() > 0) {
          return getItem(0);
        } else {
          return null;
        }
      } else {
        throw new IOException("type " + collectorType + " not supported");
      }
    }

    /**
     * Gets the collector type.
     *
     * @return the collector type
     */
    public String getCollectorType() {
      return collectorType;
    }

    /**
     * Gets the stats type.
     *
     * @return the stats type
     */
    public String getStatsType() {
      return statsType;
    }

    /**
     * Gets the data type.
     *
     * @return the data type
     */
    public String getDataType() {
      return dataType;
    }

    /**
     * Gets the size.
     *
     * @return the size
     */
    public int getSize() {
      return size;
    }

  }

  /**
   * The Interface MtasDataOperations.
   *
   * @param <T1> the generic type
   * @param <T2> the generic type
   */
  private interface MtasDataOperations<T1 extends Number, T2 extends Number> {
    
    /**
     * Product11.
     *
     * @param arg1 the arg1
     * @param arg2 the arg2
     * @return the t1
     */
    public T1 product11(T1 arg1, T1 arg2);

    /**
     * Add11.
     *
     * @param arg1 the arg1
     * @param arg2 the arg2
     * @return the t1
     */
    public T1 add11(T1 arg1, T1 arg2);

    /**
     * Add22.
     *
     * @param arg1 the arg1
     * @param arg2 the arg2
     * @return the t2
     */
    public T2 add22(T2 arg1, T2 arg2);

    /**
     * Subtract12.
     *
     * @param arg1 the arg1
     * @param arg2 the arg2
     * @return the t2
     */
    public T2 subtract12(T1 arg1, T2 arg2);

    /**
     * Divide1.
     *
     * @param arg1 the arg1
     * @param arg2 the arg2
     * @return the t2
     */
    public T2 divide1(T1 arg1, long arg2);

    /**
     * Divide2.
     *
     * @param arg1 the arg1
     * @param arg2 the arg2
     * @return the t2
     */
    public T2 divide2(T2 arg1, long arg2);

    /**
     * Exp2.
     *
     * @param arg1 the arg1
     * @return the t2
     */
    public T2 exp2(T2 arg1);

    /**
     * Sqrt2.
     *
     * @param arg1 the arg1
     * @return the t2
     */
    public T2 sqrt2(T2 arg1);

    /**
     * Log1.
     *
     * @param arg1 the arg1
     * @return the t2
     */
    public T2 log1(T1 arg1);

    /**
     * Min11.
     *
     * @param arg1 the arg1
     * @param arg2 the arg2
     * @return the t1
     */
    public T1 min11(T1 arg1, T1 arg2);

    /**
     * Max11.
     *
     * @param arg1 the arg1
     * @param arg2 the arg2
     * @return the t1
     */
    public T1 max11(T1 arg1, T1 arg2);

    /**
     * Creates the vector1.
     *
     * @param length the length
     * @return the t1[]
     */
    public T1[] createVector1(int length);

    /**
     * Creates the vector2.
     *
     * @param length the length
     * @return the t2[]
     */
    public T2[] createVector2(int length);

    /**
     * Creates the matrix1.
     *
     * @param length the length
     * @return the t1[][]
     */
    public T1[][] createMatrix1(int length);

    /**
     * Gets the zero1.
     *
     * @return the zero1
     */
    public T1 getZero1();

    /**
     * Gets the zero2.
     *
     * @return the zero2
     */
    public T2 getZero2();

  }

  /**
   * The Class MtasDataLongOperations.
   */
  private static class MtasDataLongOperations
      implements MtasDataOperations<Long, Double>, Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#product11(java.lang.Number, java.lang.Number)
     */
    @Override
    public Long product11(Long arg1, Long arg2) {
      return arg1 * arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#add11(java.lang.Number, java.lang.Number)
     */
    @Override
    public Long add11(Long arg1, Long arg2) {
      return arg1 + arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#add22(java.lang.Number, java.lang.Number)
     */
    @Override
    public Double add22(Double arg1, Double arg2) {
      return arg1 + arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#subtract12(java.lang.Number, java.lang.Number)
     */
    @Override
    public Double subtract12(Long arg1, Double arg2) {
      return arg1.doubleValue() - arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#divide1(java.lang.Number, long)
     */
    @Override
    public Double divide1(Long arg1, long arg2) {
      return arg1 / (double) arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#divide2(java.lang.Number, long)
     */
    @Override
    public Double divide2(Double arg1, long arg2) {
      return arg1 / arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#min11(java.lang.Number, java.lang.Number)
     */
    @Override
    public Long min11(Long arg1, Long arg2) {
      return Math.min(arg1, arg2);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#max11(java.lang.Number, java.lang.Number)
     */
    @Override
    public Long max11(Long arg1, Long arg2) {
      return Math.max(arg1, arg2);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#exp2(java.lang.Number)
     */
    @Override
    public Double exp2(Double arg1) {
      return Math.exp(arg1);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#sqrt2(java.lang.Number)
     */
    @Override
    public Double sqrt2(Double arg1) {
      return Math.sqrt(arg1);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#log1(java.lang.Number)
     */
    @Override
    public Double log1(Long arg1) {
      return Math.log(arg1);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#createVector1(int)
     */
    @Override
    public Long[] createVector1(int length) {
      return new Long[length];
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#createVector2(int)
     */
    @Override
    public Double[] createVector2(int length) {
      return new Double[length];
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#createMatrix1(int)
     */
    @Override
    public Long[][] createMatrix1(int length) {
      return new Long[length][];
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#getZero1()
     */
    @Override
    public Long getZero1() {
      return Long.valueOf(0);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#getZero2()
     */
    @Override
    public Double getZero2() {
      return Double.valueOf(0);
    }

  }

  /**
   * The Class MtasDataDoubleOperations.
   */
  private static class MtasDataDoubleOperations
      implements MtasDataOperations<Double, Double>, Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#product11(java.lang.Number, java.lang.Number)
     */
    @Override
    public Double product11(Double arg1, Double arg2) {
      return arg1 * arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#add11(java.lang.Number, java.lang.Number)
     */
    @Override
    public Double add11(Double arg1, Double arg2) {
      return arg1 + arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#add22(java.lang.Number, java.lang.Number)
     */
    @Override
    public Double add22(Double arg1, Double arg2) {
      return arg1 + arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#subtract12(java.lang.Number, java.lang.Number)
     */
    @Override
    public Double subtract12(Double arg1, Double arg2) {
      return arg1 - arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#divide1(java.lang.Number, long)
     */
    @Override
    public Double divide1(Double arg1, long arg2) {
      return arg1 / arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#divide2(java.lang.Number, long)
     */
    @Override
    public Double divide2(Double arg1, long arg2) {
      return arg1 / arg2;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#min11(java.lang.Number, java.lang.Number)
     */
    @Override
    public Double min11(Double arg1, Double arg2) {
      return Math.min(arg1, arg2);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#max11(java.lang.Number, java.lang.Number)
     */
    @Override
    public Double max11(Double arg1, Double arg2) {
      return Math.max(arg1, arg2);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#exp2(java.lang.Number)
     */
    @Override
    public Double exp2(Double arg1) {
      return Math.exp(arg1);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#sqrt2(java.lang.Number)
     */
    @Override
    public Double sqrt2(Double arg1) {
      return Math.sqrt(arg1);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#log1(java.lang.Number)
     */
    @Override
    public Double log1(Double arg1) {
      return Math.log(arg1);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#createVector1(int)
     */
    @Override
    public Double[] createVector1(int length) {
      return new Double[length];
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#createVector2(int)
     */
    @Override
    public Double[] createVector2(int length) {
      return new Double[length];
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#createMatrix1(int)
     */
    @Override
    public Double[][] createMatrix1(int length) {
      return new Double[length][];
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#getZero1()
     */
    @Override
    public Double getZero1() {
      return Double.valueOf(0);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataOperations#getZero2()
     */
    @Override
    public Double getZero2() {
      return Double.valueOf(0);
    }

  }

  /**
   * The Class MtasDataItem.
   *
   * @param <T> the generic type
   */
  public abstract static class MtasDataItem<T extends Number>
      implements Serializable, Comparable<MtasDataItem<T>> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The sub. */
    protected MtasDataCollector<?, ?> sub;
    
    /** The stats items. */
    protected TreeSet<String> statsItems;
    
    /** The sort direction. */
    protected String sortType, sortDirection;
    
    /** The error number. */
    protected int errorNumber;
    
    /** The error list. */
    protected HashMap<String, Integer> errorList;

    /**
     * Instantiates a new mtas data item.
     *
     * @param sub the sub
     * @param statsItems the stats items
     * @param sortType the sort type
     * @param sortDirection the sort direction
     * @param errorNumber the error number
     * @param errorList the error list
     */
    public MtasDataItem(MtasDataCollector<?, ?> sub, TreeSet<String> statsItems,
        String sortType, String sortDirection, int errorNumber,
        HashMap<String, Integer> errorList) {
      this.sub = sub;
      this.statsItems = statsItems;
      this.sortType = sortType;
      this.sortDirection = sortDirection;
      this.errorNumber = errorNumber;
      this.errorList = errorList;
    }

    /**
     * Adds the.
     *
     * @param newItem the new item
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void add(MtasDataItem<T> newItem) throws IOException;

    /**
     * Rewrite.
     *
     * @return the map
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract Map<String, Object> rewrite() throws IOException;

    /**
     * Gets the sub.
     *
     * @return the sub
     */
    public MtasDataCollector<?, ?> getSub() {
      return sub;
    }

  }

  /**
   * The Class MtasDataItemBasic.
   *
   * @param <T1> the generic type
   * @param <T2> the generic type
   */
  private abstract static class MtasDataItemBasic<T1 extends Number, T2 extends Number>
      extends MtasDataItem<T1> implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The value sum. */
    protected T1 valueSum;
    
    /** The value n. */
    protected Long valueN;
    
    /** The operations. */
    protected MtasDataOperations<T1, T2> operations;

    /**
     * Instantiates a new mtas data item basic.
     *
     * @param valueSum the value sum
     * @param valueN the value n
     * @param sub the sub
     * @param statsItems the stats items
     * @param sortType the sort type
     * @param sortDirection the sort direction
     * @param errorNumber the error number
     * @param errorList the error list
     * @param operations the operations
     */
    public MtasDataItemBasic(T1 valueSum, long valueN,
        MtasDataCollector<?, ?> sub, TreeSet<String> statsItems,
        String sortType, String sortDirection, int errorNumber,
        HashMap<String, Integer> errorList,
        MtasDataOperations<T1, T2> operations) {
      super(sub, statsItems, sortType, sortDirection, errorNumber, errorList);
      this.valueSum = valueSum;
      this.valueN = valueN;
      this.operations = operations;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataItem#add(mtas.codec.util.DataCollector.MtasDataItem)
     */
    @Override
    public void add(MtasDataItem<T1> newItem) throws IOException {
      if (newItem instanceof MtasDataItemBasic) {
        MtasDataItemBasic<T1, T2> newTypedItem = (MtasDataItemBasic<T1, T2>) newItem;
        this.valueSum = operations.add11(this.valueSum, newTypedItem.valueSum);
        this.valueN += newTypedItem.valueN;
      } else {
        throw new IOException("can only add MtasDataItemBasic");
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataItem#rewrite()
     */
    @Override
    public Map<String, Object> rewrite() throws IOException {
      Map<String, Object> response = new HashMap<String, Object>();
      for (String statsItem : statsItems) {
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
        for (Entry<String, Integer> entry : errorList.entrySet()) {
          errorResponse.put(entry.getKey(), entry.getValue());
        }
        response.put("errorNumber", errorNumber);
        response.put("errorList", errorResponse);
      }
      // response.put("stats", "basic");
      return response;
    }

    /**
     * Gets the value.
     *
     * @param statsType the stats type
     * @return the value
     */
    protected T2 getValue(String statsType) {
      if (statsType.equals(CodecUtil.STATS_TYPE_MEAN)) {
        return operations.divide1(valueSum, valueN);
      } else {
        return null;
      }
    }

  }

  /**
   * The Class MtasDataItemAdvanced.
   *
   * @param <T1> the generic type
   * @param <T2> the generic type
   */
  private abstract static class MtasDataItemAdvanced<T1 extends Number, T2 extends Number>
      extends MtasDataItem<T1> implements Serializable {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The value sum. */
    protected T1 valueSum;
    
    /** The value sum of logs. */
    protected T2 valueSumOfLogs;
    
    /** The value sum of squares. */
    protected T1 valueSumOfSquares;
    
    /** The value min. */
    protected T1 valueMin;
    
    /** The value max. */
    protected T1 valueMax;
    
    /** The value n. */
    protected Long valueN;
    
    /** The operations. */
    protected MtasDataOperations<T1, T2> operations;

    /**
     * Instantiates a new mtas data item advanced.
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
     * @param operations the operations
     */
    public MtasDataItemAdvanced(T1 valueSum, T2 valueSumOfLogs,
        T1 valueSumOfSquares, T1 valueMin, T1 valueMax, long valueN,
        MtasDataCollector<?, ?> sub, TreeSet<String> statsItems,
        String sortType, String sortDirection, int errorNumber,
        HashMap<String, Integer> errorList,
        MtasDataOperations<T1, T2> operations) {
      super(sub, statsItems, sortType, sortDirection, errorNumber, errorList);
      this.valueSum = valueSum;
      this.valueSumOfLogs = valueSumOfLogs;
      this.valueSumOfSquares = valueSumOfSquares;
      this.valueMin = valueMin;
      this.valueMax = valueMax;
      this.valueN = valueN;
      this.operations = operations;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataItem#add(mtas.codec.util.DataCollector.MtasDataItem)
     */
    @Override
    public void add(MtasDataItem<T1> newItem) throws IOException {
      if (newItem instanceof MtasDataItemAdvanced) {
        MtasDataItemAdvanced<T1, T2> newTypedItem = (MtasDataItemAdvanced<T1, T2>) newItem;
        valueSum = operations.add11(valueSum, newTypedItem.valueSum);
        valueSumOfLogs = operations.add22(valueSumOfLogs,
            newTypedItem.valueSumOfLogs);
        valueSumOfSquares = operations.add11(valueSumOfSquares,
            newTypedItem.valueSumOfSquares);
        valueMin = operations.min11(valueMin, newTypedItem.valueMin);
        valueMax = operations.max11(valueMax, newTypedItem.valueMax);
        valueN += newTypedItem.valueN;
      } else {
        throw new IOException("can only add MtasDataItemAdvanced");
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataItem#rewrite()
     */
    @Override
    public Map<String, Object> rewrite() throws IOException {
      Map<String, Object> response = new HashMap<String, Object>();
      for (String statsItem : statsItems) {
        if (statsItem.equals(CodecUtil.STATS_TYPE_SUM)) {
          response.put(statsItem, valueSum);
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_N)) {
          response.put(statsItem, valueN);
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_MAX)) {
          response.put(statsItem, valueMax);
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_MIN)) {
          response.put(statsItem, valueMin);
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_SUMSQ)) {
          response.put(statsItem, valueSumOfSquares);
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_SUMOFLOGS)) {
          response.put(statsItem, valueSumOfLogs);
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_MEAN)) {
          response.put(statsItem, getValue(statsItem));
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_GEOMETRICMEAN)) {
          response.put(statsItem, getValue(statsItem));
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_STANDARDDEVIATION)) {
          response.put(statsItem, getValue(statsItem));
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_VARIANCE)) {
          response.put(statsItem, getValue(statsItem));
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_POPULATIONVARIANCE)) {
          response.put(statsItem, getValue(statsItem));
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_QUADRATICMEAN)) {
          response.put(statsItem, getValue(statsItem));
        } else {
          response.put(statsItem, null);
        }
      }
      if (errorNumber > 0) {
        Map<String, Object> errorResponse = new HashMap<String, Object>();
        for (Entry<String, Integer> entry : errorList.entrySet()) {
          errorResponse.put(entry.getKey(), entry.getValue());
        }
        response.put("errorNumber", errorNumber);
        response.put("errorList", errorResponse);
      }
      // response.put("stats", "advanced");
      return response;
    }

    /**
     * Gets the value.
     *
     * @param statsType the stats type
     * @return the value
     */
    protected T2 getValue(String statsType) {
      if (statsType.equals(CodecUtil.STATS_TYPE_MEAN)) {
        return operations.divide1(valueSum, valueN);
      } else if (statsType.equals(CodecUtil.STATS_TYPE_GEOMETRICMEAN)) {
        return operations.exp2(operations.divide2(valueSumOfLogs, valueN));
      } else if (statsType.equals(CodecUtil.STATS_TYPE_STANDARDDEVIATION)) {
        return operations
            .sqrt2(
                operations.divide2(
                    operations.subtract12(valueSumOfSquares,
                        operations.divide1(
                            operations.product11(valueSum, valueSum), valueN)),
                    (valueN - 1)));
      } else if (statsType.equals(CodecUtil.STATS_TYPE_VARIANCE)) {
        return operations
            .divide2(
                operations
                    .subtract12(valueSumOfSquares,
                        operations.divide1(
                            operations.product11(valueSum, valueSum), valueN)),
                (valueN - 1));
      } else if (statsType.equals(CodecUtil.STATS_TYPE_POPULATIONVARIANCE)) {
        return operations
            .divide2(
                operations
                    .subtract12(valueSumOfSquares,
                        operations.divide1(
                            operations.product11(valueSum, valueSum), valueN)),
                valueN);
      } else if (statsType.equals(CodecUtil.STATS_TYPE_QUADRATICMEAN)) {
        return operations.sqrt2(operations.divide1(valueSumOfSquares, valueN));
      } else {
        return null;
      }
    }

  }

  /**
   * The Class MtasDataItemFull.
   *
   * @param <T1> the generic type
   * @param <T2> the generic type
   */
  private abstract static class MtasDataItemFull<T1 extends Number, T2 extends Number>
      extends MtasDataItem<T1> implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The full values. */
    public T1[] fullValues;
    
    /** The operations. */
    protected MtasDataOperations<T1, T2> operations;
    
    /** The stats. */
    protected DescriptiveStatistics stats = null;
    
    /** The fp stats function items. */
    private Pattern fpStatsFunctionItems = Pattern
        .compile("(([^\\(,]+)(\\(([^\\)]*)\\))?)");

    /**
     * Instantiates a new mtas data item full.
     *
     * @param value the value
     * @param sub the sub
     * @param statsItems the stats items
     * @param sortType the sort type
     * @param sortDirection the sort direction
     * @param errorNumber the error number
     * @param errorList the error list
     * @param operations the operations
     */
    public MtasDataItemFull(T1[] value, MtasDataCollector<?, ?> sub,
        TreeSet<String> statsItems, String sortType, String sortDirection,
        int errorNumber, HashMap<String, Integer> errorList,
        MtasDataOperations<T1, T2> operations) {
      super(sub, statsItems, sortType, sortDirection, errorNumber, errorList);
      this.fullValues = value;
      this.operations = operations;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataItem#add(mtas.codec.util.DataCollector.MtasDataItem)
     */
    @Override
    public void add(MtasDataItem<T1> newItem) throws IOException {
      if (newItem instanceof MtasDataItemFull) {
        MtasDataItemFull<T1, T2> newTypedItem = (MtasDataItemFull<T1, T2>) newItem;
        T1[] tmpValue = operations
            .createVector1(fullValues.length + newTypedItem.fullValues.length);
        System.arraycopy(fullValues, 0, tmpValue, 0, fullValues.length);
        System.arraycopy(newTypedItem.fullValues, 0, tmpValue,
            fullValues.length, newTypedItem.fullValues.length);
        fullValues = tmpValue;
      } else {
        throw new IOException("can only add MtasDataItemFull");
      }
    }

    /**
     * Creates the stats.
     */
    protected void createStats() {
      if (stats == null) {
        stats = new DescriptiveStatistics();
        for (T1 value : fullValues) {
          stats.addValue(value.doubleValue());
        }
      }
    }

    /**
     * Gets the distribution.
     *
     * @param arguments the arguments
     * @return the distribution
     */
    abstract protected HashMap<String, Object> getDistribution(
        String arguments);

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataItem#rewrite()
     */
    @Override
    public Map<String, Object> rewrite() throws IOException {
      createStats();
      Map<String, Object> response = new HashMap<String, Object>();
      for (String statsItem : statsItems) {
        if (statsItem.equals(CodecUtil.STATS_TYPE_SUM)) {
          response.put(statsItem, stats.getSum());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_N)) {
          response.put(statsItem, stats.getN());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_MAX)) {
          response.put(statsItem, stats.getMax());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_MIN)) {
          response.put(statsItem, stats.getMin());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_SUMSQ)) {
          response.put(statsItem, stats.getSumsq());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_SUMOFLOGS)) {
          response.put(statsItem,
              stats.getN() * Math.log(stats.getGeometricMean()));
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_MEAN)) {
          response.put(statsItem, stats.getMean());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_GEOMETRICMEAN)) {
          response.put(statsItem, stats.getGeometricMean());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_STANDARDDEVIATION)) {
          response.put(statsItem, stats.getStandardDeviation());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_VARIANCE)) {
          response.put(statsItem, stats.getVariance());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_POPULATIONVARIANCE)) {
          response.put(statsItem, stats.getPopulationVariance());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_QUADRATICMEAN)) {
          response.put(statsItem, stats.getQuadraticMean());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_KURTOSIS)) {
          response.put(statsItem, stats.getKurtosis());
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_MEDIAN)) {
          response.put(statsItem, stats.getPercentile(50));
        } else if (statsItem.equals(CodecUtil.STATS_TYPE_SKEWNESS)) {
          response.put(statsItem, stats.getSkewness());
        } else {
          Matcher m = fpStatsFunctionItems.matcher(statsItem);
          if (m.find()) {
            String function = m.group(2).trim();
            if (function.equals(CodecUtil.STATS_FUNCTION_DISTRIBUTION)) {
              response.put(statsItem, getDistribution(m.group(4)));
            } else {
              response.put(statsItem, "test");
            }
          } else {
            response.put(statsItem, "niet");
          }
        }
      }
      if (errorNumber > 0) {
        Map<String, Object> errorResponse = new HashMap<String, Object>();
        for (Entry<String, Integer> entry : errorList.entrySet()) {
          errorResponse.put(entry.getKey(), entry.getValue());
        }
        response.put("errorNumber", errorNumber);
        response.put("errorList", errorResponse);
      }
      //response.put("stats", "full");
      return response;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(MtasDataItem<T1> o) {
      int compare = 0;
      if (o instanceof MtasDataItemLongFull) {
        MtasDataItemLongFull to = (MtasDataItemLongFull) o;
        createStats();
        to.createStats();
        if (sortType.equals(CodecUtil.STATS_TYPE_N)) {
          compare = Long.valueOf(stats.getN()).compareTo(to.stats.getN());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_SUM)) {
          compare = Double.valueOf(stats.getSum()).compareTo(to.stats.getSum());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_MAX)) {
          compare = Double.valueOf(stats.getMax()).compareTo(to.stats.getMax());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_MIN)) {
          compare = Double.valueOf(stats.getMin()).compareTo(to.stats.getMin());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_SUMSQ)) {
          compare = Double.valueOf(stats.getSumsq())
              .compareTo(to.stats.getSumsq());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_SUMOFLOGS)) {
          compare = Double
              .valueOf(stats.getN() * Math.log(stats.getGeometricMean()))
              .compareTo(
                  to.stats.getN() * Math.log(to.stats.getGeometricMean()));
        } else if (sortType.equals(CodecUtil.STATS_TYPE_MEAN)) {
          compare = Double.valueOf(stats.getMean())
              .compareTo(to.stats.getMean());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_GEOMETRICMEAN)) {
          compare = Double.valueOf(stats.getGeometricMean())
              .compareTo(to.stats.getGeometricMean());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_STANDARDDEVIATION)) {
          compare = Double.valueOf(stats.getStandardDeviation())
              .compareTo(to.stats.getStandardDeviation());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_VARIANCE)) {
          compare = Double.valueOf(stats.getVariance())
              .compareTo(to.stats.getVariance());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_POPULATIONVARIANCE)) {
          compare = Double.valueOf(stats.getPopulationVariance())
              .compareTo(to.stats.getPopulationVariance());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_QUADRATICMEAN)) {
          compare = Double.valueOf(stats.getQuadraticMean())
              .compareTo(to.stats.getQuadraticMean());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_KURTOSIS)) {
          compare = Double.valueOf(stats.getKurtosis())
              .compareTo(to.stats.getKurtosis());
        } else if (sortType.equals(CodecUtil.STATS_TYPE_MEDIAN)) {
          compare = Double.valueOf(stats.getPercentile(50))
              .compareTo(to.stats.getPercentile(50));
        } else if (sortType.equals(CodecUtil.STATS_TYPE_SKEWNESS)) {
          compare = Double.valueOf(stats.getSkewness())
              .compareTo(to.stats.getSkewness());
        }
      }
      return sortDirection.equals(CodecUtil.SORT_DESC) ? -1 * compare : compare;
    }

  }

  /**
   * The Class MtasDataItemLongBasic.
   */
  private static class MtasDataItemLongBasic
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
        HashMap<String, Integer> errorList) {
      super(valueSum, valueN, sub, statsItems, sortType, sortDirection,
          errorNumber, errorList, new MtasDataLongOperations());
    }

    /* (non-Javadoc)
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

  /**
   * The Class MtasDataItemDoubleBasic.
   */
  private static class MtasDataItemDoubleBasic
      extends MtasDataItemBasic<Double, Double> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new mtas data item double basic.
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
    public MtasDataItemDoubleBasic(double valueSum, long valueN,
        MtasDataCollector<?, ?> sub, TreeSet<String> statsItems,
        String sortType, String sortDirection, int errorNumber,
        HashMap<String, Integer> errorList) {
      super(valueSum, valueN, sub, statsItems, sortType, sortDirection,
          errorNumber, errorList, new MtasDataDoubleOperations());
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(MtasDataItem<Double> o) {
      int compare = 0;
      if (o instanceof MtasDataItemDoubleBasic) {
        MtasDataItemDoubleBasic to = (MtasDataItemDoubleBasic) o;
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

  /**
   * The Class MtasDataItemLongAdvanced.
   */
  private static class MtasDataItemLongAdvanced
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
        HashMap<String, Integer> errorList) {
      super(valueSum, valueSumOfLogs, valueSumOfSquares, valueMin, valueMax,
          valueN, sub, statsItems, sortType, sortDirection, errorNumber,
          errorList, new MtasDataLongOperations());
    }

    /* (non-Javadoc)
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

  /**
   * The Class MtasDataItemDoubleAdvanced.
   */
  private static class MtasDataItemDoubleAdvanced
      extends MtasDataItemAdvanced<Double, Double> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new mtas data item double advanced.
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
    public MtasDataItemDoubleAdvanced(double valueSum, double valueSumOfLogs,
        double valueSumOfSquares, double valueMin, double valueMax, long valueN,
        MtasDataCollector<?, ?> sub, TreeSet<String> statsItems,
        String sortType, String sortDirection, int errorNumber,
        HashMap<String, Integer> errorList) {
      super(valueSum, valueSumOfLogs, valueSumOfSquares, valueMin, valueMax,
          valueN, sub, statsItems, sortType, sortDirection, errorNumber,
          errorList, new MtasDataDoubleOperations());
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(MtasDataItem<Double> o) {
      int compare = 0;
      if (o instanceof MtasDataItemDoubleAdvanced) {
        MtasDataItemDoubleAdvanced to = (MtasDataItemDoubleAdvanced) o;
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

  /**
   * The Class MtasDataItemLongFull.
   */
  private static class MtasDataItemLongFull
      extends MtasDataItemFull<Long, Double> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The fp argument. */
    private static Pattern fpArgument = Pattern.compile("([^=,]+)=([^,]*)");

    /**
     * Instantiates a new mtas data item long full.
     *
     * @param value the value
     * @param sub the sub
     * @param statsItems the stats items
     * @param sortType the sort type
     * @param sortDirection the sort direction
     * @param errorNumber the error number
     * @param errorList the error list
     */
    public MtasDataItemLongFull(long[] value, MtasDataCollector<?, ?> sub,
        TreeSet<String> statsItems, String sortType, String sortDirection,
        int errorNumber, HashMap<String, Integer> errorList) {
      super(ArrayUtils.toObject(value), sub, statsItems, sortType,
          sortDirection, errorNumber, errorList, new MtasDataLongOperations());
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataItemFull#getDistribution(java.lang.String)
     */
    @Override
    protected HashMap<String, Object> getDistribution(String argument) {
      HashMap<String, Object> result = new LinkedHashMap<String, Object>();
      Long start = null, end = null, step = null;
      Integer number = null;
      if (argument != null) {
        Matcher m = fpArgument.matcher(argument);
        // get settings
        while (m.find()) {
          if (m.group(1).trim().equals("start")) {
            start = Long.parseLong(m.group(2));
          } else if (m.group(1).trim().equals("end")) {
            end = Long.parseLong(m.group(2));
          } else if (m.group(1).trim().equals("step")) {
            step = Long.parseLong(m.group(2));
          } else if (m.group(1).trim().equals("number")) {
            number = Integer.parseInt(m.group(2));
          }
        }
      }
      // always exactly one of (positive) number and (positive) step, other null
      if ((number == null || number < 1) && (step == null || step < 1)) {
        number = 10;
        step = null;
      } else if (step != null && step < 1) {
        step = null;
      } else if (number != null && number < 1) {
        number = null;
      } else if (step != null) {
        number = null;
      }
      // sanity checks start/end
      createStats();
      long tmpStart = Double.valueOf(Math.floor(stats.getMin())).longValue();
      long tmpEnd = Double.valueOf(Math.ceil(stats.getMax())).longValue();
      if (start != null && end != null && start > end) {
        return null;
      } else if (start != null && start > tmpEnd) {
        return null;
      } else if (end != null && end < tmpStart) {
        return null;
      }
      // check start and end
      if (start == null && end == null) {
        if (step == null) {
          step = -Math.floorDiv((tmpStart - tmpEnd - 1), number);
        }
        number = Long.valueOf(-Math.floorDiv((tmpStart - tmpEnd - 1), step))
            .intValue();
        start = tmpStart;
        end = start + (number * step);
      } else if (start == null) {
        if (step == null) {
          step = -Math.floorDiv((tmpStart - end - 1), number);
        }
        number = Long.valueOf(-Math.floorDiv((tmpStart - end - 1), step))
            .intValue();
        start = end - (number * step);
      } else if (end == null) {
        if (step == null) {
          step = -Math.floorDiv((start - tmpEnd - 1), number);
        }
        number = Long.valueOf(-Math.floorDiv((start - tmpEnd - 1), step))
            .intValue();
        end = start + (number * step);
      } else {
        if (step == null) {
          step = -Math.floorDiv((start - end - 1), number);
        }
        number = Long.valueOf(-Math.floorDiv((start - end - 1), step))
            .intValue();
      }
      long[] list = new long[number];
      for (Long v : fullValues) {
        if (v >= start && v <= end) {
          int i = Long.valueOf(Math.floorDiv((v - start), step)).intValue();
          list[i]++;
        }
      }
      for (int i = 0; i < number; i++) {
        Long l = start + i * step;
        Long r = Math.min(end, l + step - 1);
        String key;
        if (step > 1 && r > l) {
          key = "[" + String.valueOf(l) + "," + String.valueOf(r) + "]";
        } else {
          key = "[" + String.valueOf(l) + "]";
        }
        result.put(key, list[i]);
      }
      return result;
    }

  }

  /**
   * The Class MtasDataItemDoubleFull.
   */
  private static class MtasDataItemDoubleFull
      extends MtasDataItemFull<Double, Double> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The fp argument. */
    private static Pattern fpArgument = Pattern.compile("([^=,]+)=([^,]*)");

    /**
     * Instantiates a new mtas data item double full.
     *
     * @param value the value
     * @param sub the sub
     * @param statsItems the stats items
     * @param sortType the sort type
     * @param sortDirection the sort direction
     * @param errorNumber the error number
     * @param errorList the error list
     */
    public MtasDataItemDoubleFull(double[] value, MtasDataCollector<?, ?> sub,
        TreeSet<String> statsItems, String sortType, String sortDirection,
        int errorNumber, HashMap<String, Integer> errorList) {
      super(ArrayUtils.toObject(value), sub, statsItems, sortType,
          sortDirection, errorNumber, errorList,
          new MtasDataDoubleOperations());
    }

    /**
     * Gets the number of decimals.
     *
     * @param ds the ds
     * @return the number of decimals
     */
    private int getNumberOfDecimals(String ds) {
      if (!ds.contains(".")) {
        return 0;
      } else {
        return (ds.length() - ds.indexOf(".") - 1);
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataItemFull#getDistribution(java.lang.String)
     */
    @Override
    protected HashMap<String, Object> getDistribution(String argument) {
      HashMap<String, Object> result = new LinkedHashMap<String, Object>();
      Double start = null, end = null, step = null;
      Integer d = null, number = null;
      if (argument != null) {
        Matcher m = fpArgument.matcher(argument);
        // get settings
        while (m.find()) {
          if (m.group(1).trim().equals("start")) {
            start = Double.parseDouble(m.group(2));
            d = (d == null) ? getNumberOfDecimals(m.group(2))
                : Math.max(d, getNumberOfDecimals(m.group(2)));
          } else if (m.group(1).trim().equals("end")) {
            end = Double.parseDouble(m.group(2));
            d = (d == null) ? getNumberOfDecimals(m.group(2))
                : Math.max(d, getNumberOfDecimals(m.group(2)));
          } else if (m.group(1).trim().equals("step")) {
            step = Double.parseDouble(m.group(2));
            d = (d == null) ? getNumberOfDecimals(m.group(2))
                : Math.max(d, getNumberOfDecimals(m.group(2)));
          } else if (m.group(1).trim().equals("number")) {
            number = Integer.parseInt(m.group(2));
          }
        }
      }
      // always exactly one of (positive) number and (positive) step, other null
      if ((number == null || number < 1) && (step == null || step <= 0)) {
        number = 10;
        step = null;
      } else if (step != null && step <= 0) {
        step = null;
      } else if (number != null && number < 1) {
        number = null;
      } else if (step != null) {
        number = null;
      }
      // sanity checks start/end
      createStats();
      double tmpStart = stats.getMin();
      double tmpEnd = stats.getMax();
      if (start != null && end != null && start > end) {
        return null;
      } else if (start != null && start > tmpEnd) {
        return null;
      } else if (end != null && end < tmpStart) {
        return null;
      }
      // check start and end
      if (start == null && end == null) {
        if (step == null) {
          step = (tmpEnd - tmpStart) / number;
        }
        number = Double.valueOf(Math.ceil((tmpEnd - tmpStart) / step))
            .intValue();
        start = tmpStart;
        end = start + (number * step);
      } else if (start == null) {
        if (step == null) {
          step = (end - tmpStart) / number;
        }
        number = Double.valueOf(Math.ceil((end - tmpStart) / step)).intValue();
        start = end - (number * step);
      } else if (end == null) {
        if (step == null) {
          step = (tmpEnd - start) / number;
        }
        number = Double.valueOf(Math.ceil((tmpEnd - start) / step)).intValue();
        end = start + (number * step);
      } else {
        if (step == null) {
          step = (end - start) / number;
        }
        number = Double.valueOf(Math.ceil((end - start) / step)).intValue();
      }
      // round step to agreeable format and recompute number
      int tmpD = Double
          .valueOf(Math.max(0, 1 + Math.ceil(-1 * Math.log10(step))))
          .intValue();
      d = (d == null) ? tmpD : Math.max(d, tmpD);
      double tmp = Math.pow(10.0, d);
      step = Math.round(step * tmp) / tmp;
      number = Double.valueOf(Math.ceil((end - start) / step)).intValue();

      // compute distribution
      long[] list = new long[number];
      for (Double v : fullValues) {
        if (v >= start && v <= end) {
          int i = Math.min(
              Double.valueOf(Math.floor((v - start) / step)).intValue(),
              (number - 1));
          list[i]++;
        }
      }
      Double l, r;
      String ls, rs;
      for (int i = 0; i < number; i++) {
        l = start + i * step;
        r = Math.min(end, l + step);
        ls = String.format("%." + d + "f", l);
        rs = String.format("%." + d + "f", r);
        String key = "[" + ls + "," + rs
            + ((i == (number - 1) && r >= tmpEnd && l <= tmpEnd) ? "]" : ")");
        result.put(key, list[i]);
      }
      return result;
    }

  }

  /**
   * The Class MtasDataBasic.
   *
   * @param <T1> the generic type
   * @param <T2> the generic type
   * @param <T3> the generic type
   */
  private abstract static class MtasDataBasic<T1 extends Number, T2 extends Number, T3 extends MtasDataItem<T1>>
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
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDataBasic(String collectorType, String dataType,
        TreeSet<String> statsItems, String sortType, String sortDirection,
        Integer start, Integer number, String[] subCollectorTypes,
        String[] subDataTypes, String[] subStatsTypes,
        TreeSet<String>[] subStatsItems, String[] subSortTypes,
        String[] subSortDirections, Integer[] subStart, Integer[] subNumber,
        MtasDataOperations<T1, T2> operations) throws IOException {
      super(collectorType, dataType, CodecUtil.STATS_BASIC, statsItems,
          sortType, sortDirection, start, number, subCollectorTypes,
          subDataTypes, subStatsTypes, subStatsItems, subSortTypes,
          subSortDirections, subStart, subNumber);
      this.operations = operations;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#error(java.lang.String)
     */
    @Override
    public final void error(String error) throws IOException {
      add();
      setError(newCurrentPosition, error, newCurrentExisting);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#error(java.lang.String[], java.lang.String)
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
          valueSum = (i == 0) ? values[i]
              : operations.add11(valueSum, values[i]);
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

    /* (non-Javadoc)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#copyToNew(int, int)
     */
    @Override
    protected void copyToNew(int position, int newPosition) {
      newBasicValueSumList[newPosition] = basicValueSumList[position];
      newBasicValueNList[newPosition] = basicValueNList[position];
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#copyFromNew()
     */
    @Override
    protected void copyFromNew() {
      basicValueSumList = newBasicValueSumList;
      basicValueNList = newBasicValueNList;
    }

    /* (non-Javadoc)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#merge(mtas.codec.util.DataCollector.MtasDataCollector)
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
        if (collectorType.equals(COLLECTOR_TYPE_LIST)) {
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
        } else if (collectorType.equals(COLLECTOR_TYPE_DATA)) {
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#initNewList(int)
     */
    @Override
    public final void initNewList(int maxNumberOfTerms) {
      super.initNewList(maxNumberOfTerms);
      newBasicValueSumList = operations.createVector1(newSize);
      newBasicValueNList = new long[newSize];
    }

  }

  /**
   * The Class MtasDataAdvanced.
   *
   * @param <T1> the generic type
   * @param <T2> the generic type
   * @param <T3> the generic type
   */
  private abstract static class MtasDataAdvanced<T1 extends Number, T2 extends Number, T3 extends MtasDataItem<T1>>
      extends MtasDataCollector<T1, T3> implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The new advanced value sum list. */
    protected T1[] advancedValueSumList = null, newAdvancedValueSumList = null;
    
    /** The new advanced value max list. */
    protected T1[] advancedValueMaxList = null, newAdvancedValueMaxList = null;
    
    /** The new advanced value min list. */
    protected T1[] advancedValueMinList = null, newAdvancedValueMinList = null;
    
    /** The new advanced value sum of squares list. */
    protected T1[] advancedValueSumOfSquaresList = null,
        newAdvancedValueSumOfSquaresList = null;
    
    /** The new advanced value sum of logs list. */
    protected T2[] advancedValueSumOfLogsList = null,
        newAdvancedValueSumOfLogsList = null;
    
    /** The new advanced value n list. */
    protected long[] advancedValueNList = null, newAdvancedValueNList = null;
    
    /** The operations. */
    protected MtasDataOperations<T1, T2> operations;

    /**
     * Instantiates a new mtas data advanced.
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
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDataAdvanced(String collectorType, String dataType,
        TreeSet<String> statsItems, String sortType, String sortDirection,
        Integer start, Integer number, String[] subCollectorTypes,
        String[] subDataTypes, String[] subStatsTypes,
        TreeSet<String>[] subStatsItems, String[] subSortTypes,
        String[] subSortDirections, Integer[] subStart, Integer[] subNumber,
        MtasDataOperations<T1, T2> operations) throws IOException {
      super(collectorType, dataType, CodecUtil.STATS_ADVANCED, statsItems,
          sortType, sortDirection, start, number, subCollectorTypes,
          subDataTypes, subStatsTypes, subStatsItems, subSortTypes,
          subSortDirections, subStart, subNumber);
      this.operations = operations;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#error(java.lang.String)
     */
    @Override
    public final void error(String error) throws IOException {
      add();
      setError(newCurrentPosition, error, newCurrentExisting);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#error(java.lang.String[], java.lang.String)
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
        newAdvancedValueSumList[newPosition] = operations.getZero1();
        newAdvancedValueSumOfLogsList[newPosition] = operations.getZero2();
        newAdvancedValueSumOfSquaresList[newPosition] = operations.getZero1();
        newAdvancedValueMinList[newPosition] = operations.getZero1();
        newAdvancedValueMaxList[newPosition] = operations.getZero1();
        newAdvancedValueNList[newPosition] = 0;
      }
      newErrorNumber[newPosition]++;
      if (newErrorList[newPosition].containsKey(error)) {
        newErrorList[newPosition].put(error,
            newErrorList[newPosition].get(error) + 1);
      } else {
        newErrorList[newPosition].put(error, 1);
      }
    }

    /* (non-Javadoc)
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
      T1[] tmpNewAdvancedValueSumList = newAdvancedValueSumList;
      T2[] tmpNewAdvancedValueSumOfLogsList = newAdvancedValueSumOfLogsList;
      T1[] tmpNewAdvancedValueSumOfSquaresList = newAdvancedValueSumOfSquaresList;
      T1[] tmpNewAdvancedValueMinList = newAdvancedValueMinList;
      T1[] tmpNewAdvancedValueMaxList = newAdvancedValueMaxList;
      long[] tmpNewAdvancedValueNList = newAdvancedValueNList;
      newAdvancedValueSumList = operations.createVector1(newSize);
      newAdvancedValueSumOfLogsList = operations.createVector2(newSize);
      newAdvancedValueSumOfSquaresList = operations.createVector1(newSize);
      newAdvancedValueMinList = operations.createVector1(newSize);
      newAdvancedValueMaxList = operations.createVector1(newSize);
      newAdvancedValueNList = new long[newSize];
      newPosition = tmpNewPosition;
      System.arraycopy(tmpNewAdvancedValueSumList, 0, newAdvancedValueSumList,
          0, tmpOldSize);
      System.arraycopy(tmpNewAdvancedValueSumOfLogsList, 0,
          newAdvancedValueSumOfLogsList, 0, tmpOldSize);
      System.arraycopy(tmpNewAdvancedValueSumOfSquaresList, 0,
          newAdvancedValueSumOfSquaresList, 0, tmpOldSize);
      System.arraycopy(tmpNewAdvancedValueMinList, 0, newAdvancedValueMinList,
          0, tmpOldSize);
      System.arraycopy(tmpNewAdvancedValueMaxList, 0, newAdvancedValueMaxList,
          0, tmpOldSize);
      System.arraycopy(tmpNewAdvancedValueNList, 0, newAdvancedValueNList, 0,
          tmpOldSize);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#copyToNew(int, int)
     */
    @Override
    protected void copyToNew(int position, int newPosition) {
      newAdvancedValueSumList[newPosition] = advancedValueSumList[position];
      newAdvancedValueSumOfLogsList[newPosition] = advancedValueSumOfLogsList[position];
      newAdvancedValueSumOfSquaresList[newPosition] = advancedValueSumOfSquaresList[position];
      newAdvancedValueMinList[newPosition] = advancedValueMinList[position];
      newAdvancedValueMaxList[newPosition] = advancedValueMaxList[position];
      newAdvancedValueNList[newPosition] = advancedValueNList[position];
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#copyFromNew()
     */
    @Override
    protected void copyFromNew() {
      advancedValueSumList = newAdvancedValueSumList;
      advancedValueSumOfLogsList = newAdvancedValueSumOfLogsList;
      advancedValueSumOfSquaresList = newAdvancedValueSumOfSquaresList;
      advancedValueMinList = newAdvancedValueMinList;
      advancedValueMaxList = newAdvancedValueMaxList;
      advancedValueNList = newAdvancedValueNList;
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
        T2 valueSumOfLogs = null;
        T1 valueSumOfSquares = null;
        T1 valueMin = null;
        T1 valueMax = null;
        for (int i = 0; i < number; i++) {
          valueSum = (i == 0) ? values[i]
              : operations.add11(valueSum, values[i]);
          valueSumOfLogs = (i == 0) ? operations.log1(values[i])
              : operations.add22(valueSumOfLogs, operations.log1(values[i]));
          valueSumOfSquares = (i == 0)
              ? operations.product11(values[i], values[i])
              : operations.add11(valueSumOfSquares,
                  operations.product11(values[i], values[i]));
          valueMin = (i == 0) ? values[i]
              : operations.min11(valueMin, values[i]);
          valueMax = (i == 0) ? values[i]
              : operations.max11(valueMax, values[i]);
        }
        setValue(newPosition, valueSum, valueSumOfLogs, valueSumOfSquares,
            valueMin, valueMax, number, currentExisting);
      }
    }

    /**
     * Sets the value.
     *
     * @param newPosition the new position
     * @param valueSum the value sum
     * @param valueSumOfLogs the value sum of logs
     * @param valueSumOfSquares the value sum of squares
     * @param valueMin the value min
     * @param valueMax the value max
     * @param valueN the value n
     * @param currentExisting the current existing
     */
    private void setValue(int newPosition, T1 valueSum, T2 valueSumOfLogs,
        T1 valueSumOfSquares, T1 valueMin, T1 valueMax, long valueN,
        boolean currentExisting) {
      if (valueN > 0) {
        if (currentExisting) {
          newAdvancedValueSumList[newPosition] = operations
              .add11(newAdvancedValueSumList[newPosition], valueSum);
          newAdvancedValueSumOfLogsList[newPosition] = operations.add22(
              newAdvancedValueSumOfLogsList[newPosition], valueSumOfLogs);
          newAdvancedValueSumOfSquaresList[newPosition] = operations.add11(
              newAdvancedValueSumOfSquaresList[newPosition], valueSumOfSquares);
          newAdvancedValueMinList[newPosition] = operations
              .min11(newAdvancedValueMinList[newPosition], valueMin);
          newAdvancedValueMaxList[newPosition] = operations
              .max11(newAdvancedValueMaxList[newPosition], valueMax);
          newAdvancedValueNList[newPosition] += valueN;
        } else {
          newAdvancedValueSumList[newPosition] = valueSum;
          newAdvancedValueSumOfLogsList[newPosition] = valueSumOfLogs;
          newAdvancedValueSumOfSquaresList[newPosition] = valueSumOfSquares;
          newAdvancedValueMinList[newPosition] = valueMin;
          newAdvancedValueMaxList[newPosition] = valueMax;
          newAdvancedValueNList[newPosition] = valueN;
        }
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#remapData(int[][])
     */
    @Override
    protected void remapData(int[][] mapping) throws IOException {
      super.remapData(mapping);
      T1[] originalAdvancedValueSumList = advancedValueSumList.clone();
      T2[] originalAdvancedValueSumOfLogsList = advancedValueSumOfLogsList
          .clone();
      T1[] originalAdvancedValueSumOfSquaresList = advancedValueSumOfSquaresList
          .clone();
      T1[] originalAdvancedValueMinList = advancedValueMinList.clone();
      T1[] originalAdvancedValueMaxList = advancedValueMaxList.clone();
      long[] originalAdvancedValueNList = advancedValueNList.clone();
      advancedValueSumList = operations.createVector1(mapping.length);
      advancedValueSumOfLogsList = operations.createVector2(mapping.length);
      advancedValueSumOfSquaresList = operations.createVector1(mapping.length);
      advancedValueMinList = operations.createVector1(mapping.length);
      advancedValueMaxList = operations.createVector1(mapping.length);
      advancedValueNList = new long[mapping.length];
      for (int i = 0; i < mapping.length; i++) {
        for (int j = 0; j < mapping[i].length; j++) {
          if (j == 0) {
            setValue(i, originalAdvancedValueSumList[mapping[i][j]],
                originalAdvancedValueSumOfLogsList[mapping[i][j]],
                originalAdvancedValueSumOfSquaresList[mapping[i][j]],
                originalAdvancedValueMinList[mapping[i][j]],
                originalAdvancedValueMaxList[mapping[i][j]],
                originalAdvancedValueNList[mapping[i][j]], false);
          } else {
            setValue(i, originalAdvancedValueSumList[mapping[i][j]],
                originalAdvancedValueSumOfLogsList[mapping[i][j]],
                originalAdvancedValueSumOfSquaresList[mapping[i][j]],
                originalAdvancedValueMinList[mapping[i][j]],
                originalAdvancedValueMaxList[mapping[i][j]],
                originalAdvancedValueNList[mapping[i][j]], true);
          }
        }
      }
      advancedValueSumList = newAdvancedValueSumList;
      advancedValueSumOfLogsList = newAdvancedValueSumOfLogsList;
      advancedValueSumOfSquaresList = newAdvancedValueSumOfSquaresList;
      advancedValueMinList = newAdvancedValueMinList;
      advancedValueMaxList = newAdvancedValueMaxList;
      advancedValueNList = newAdvancedValueNList;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#merge(mtas.codec.util.DataCollector.MtasDataCollector)
     */
    @Override
    public void merge(MtasDataCollector<?, ?> newDataCollector)
        throws IOException {
      closeNewList();
      if (!collectorType.equals(newDataCollector.getCollectorType())
          || !dataType.equals(newDataCollector.getDataType())
          || !statsType.equals(newDataCollector.getStatsType())
          || !(newDataCollector instanceof MtasDataAdvanced)) {
        throw new IOException("cannot merge different dataCollectors");
      } else {
        MtasDataAdvanced<T1, T2, T3> newMtasDataAdvanced = (MtasDataAdvanced<T1, T2, T3>) newDataCollector;
        newMtasDataAdvanced.closeNewList();
        initNewList(newMtasDataAdvanced.getSize());
        if (collectorType.equals(COLLECTOR_TYPE_LIST)) {
          for (int i = 0; i < newMtasDataAdvanced.getSize(); i++) {
            MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector[1];
            subCollectors[0] = add(newMtasDataAdvanced.keyList[i]);
            setError(newCurrentPosition, newMtasDataAdvanced.errorNumber[i],
                newMtasDataAdvanced.errorList[i], newCurrentExisting);
            setValue(newCurrentPosition,
                newMtasDataAdvanced.advancedValueSumList[i],
                newMtasDataAdvanced.advancedValueSumOfLogsList[i],
                newMtasDataAdvanced.advancedValueSumOfSquaresList[i],
                newMtasDataAdvanced.advancedValueMinList[i],
                newMtasDataAdvanced.advancedValueMaxList[i],
                newMtasDataAdvanced.advancedValueNList[i], newCurrentExisting);
            if (hasSub() && newMtasDataAdvanced.hasSub()) {
              subCollectors[0]
                  .merge(newMtasDataAdvanced.subCollectorListNextLevel[i]);
            }
          }
          closeNewList();
        } else if (collectorType.equals(COLLECTOR_TYPE_DATA)) {
          if (newMtasDataAdvanced.getSize() > 0) {
            MtasDataCollector subCollector = add();
            setError(newCurrentPosition, newMtasDataAdvanced.errorNumber[0],
                newMtasDataAdvanced.errorList[0], newCurrentExisting);
            setValue(newCurrentPosition,
                newMtasDataAdvanced.advancedValueSumList[0],
                newMtasDataAdvanced.advancedValueSumOfLogsList[0],
                newMtasDataAdvanced.advancedValueSumOfSquaresList[0],
                newMtasDataAdvanced.advancedValueMinList[0],
                newMtasDataAdvanced.advancedValueMaxList[0],
                newMtasDataAdvanced.advancedValueNList[0], newCurrentExisting);
            if (hasSub() && newMtasDataAdvanced.hasSub()) {
              subCollector.merge(newMtasDataAdvanced.subCollectorNextLevel);
            }
          }
          closeNewList();
        } else {
          throw new IOException("cannot merge " + collectorType);
        }
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#initNewList(int)
     */
    @Override
    public final void initNewList(int maxNumberOfTerms) {
      super.initNewList(maxNumberOfTerms);
      newAdvancedValueSumList = operations.createVector1(newSize);
      newAdvancedValueSumOfLogsList = operations.createVector2(newSize);
      newAdvancedValueSumOfSquaresList = operations.createVector1(newSize);
      newAdvancedValueMinList = operations.createVector1(newSize);
      newAdvancedValueMaxList = operations.createVector1(newSize);
      newAdvancedValueNList = new long[newSize];
    }

  }

  /**
   * The Class MtasDataFull.
   *
   * @param <T1> the generic type
   * @param <T2> the generic type
   * @param <T3> the generic type
   */
  private abstract static class MtasDataFull<T1 extends Number, T2 extends Number, T3 extends MtasDataItem<T1>>
      extends MtasDataCollector<T1, T3> implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The new full value list. */
    protected T1[][] fullValueList = null, newFullValueList = null;
    
    /** The operations. */
    protected MtasDataOperations<T1, T2> operations;

    /**
     * Instantiates a new mtas data full.
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
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDataFull(String collectorType, String dataType,
        TreeSet<String> statsItems, String sortType, String sortDirection,
        Integer start, Integer number, String[] subCollectorTypes,
        String[] subDataTypes, String[] subStatsTypes,
        TreeSet<String>[] subStatsItems, String[] subSortTypes,
        String[] subSortDirections, Integer[] subStart, Integer[] subNumber,
        MtasDataOperations<T1, T2> operations) throws IOException {
      super(collectorType, dataType, CodecUtil.STATS_FULL, statsItems, sortType,
          sortDirection, start, number, subCollectorTypes, subDataTypes,
          subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
          subStart, subNumber);
      this.operations = operations;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#error(java.lang.String)
     */
    @Override
    public final void error(String error) throws IOException {
      add();
      setError(newCurrentPosition, error, newCurrentExisting);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#error(java.lang.String[], java.lang.String)
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
        newFullValueList[newPosition] = operations.createVector1(0);
      }
      newErrorNumber[newPosition]++;
      if (newErrorList[newPosition].containsKey(error)) {
        newErrorList[newPosition].put(error,
            newErrorList[newPosition].get(error) + 1);
      } else {
        newErrorList[newPosition].put(error, 1);
      }
    }

    /* (non-Javadoc)
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
      T1[][] tmpNewFullValueList = newFullValueList;
      newFullValueList = operations.createMatrix1(newSize);
      newPosition = tmpNewPosition;
      System.arraycopy(tmpNewFullValueList, 0, newFullValueList, 0, tmpOldSize);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#copyToNew(int, int)
     */
    @Override
    protected void copyToNew(int position, int newPosition) {
      newFullValueList[newPosition] = fullValueList[position];
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#copyFromNew()
     */
    @Override
    protected void copyFromNew() {
      fullValueList = newFullValueList;
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
        if (currentExisting) {
          T1[] tmpList = operations
              .createVector1(newFullValueList[newPosition].length + number);
          System.arraycopy(newFullValueList[newPosition], 0, tmpList, 0,
              newFullValueList[newPosition].length);
          System.arraycopy(values, 0, tmpList,
              newFullValueList[newPosition].length, number);
          newFullValueList[newPosition] = tmpList;
        } else {
          if (number < values.length) {
            T1[] tmpList = operations.createVector1(number);
            System.arraycopy(values, 0, tmpList, 0, number);
            newFullValueList[newPosition] = tmpList;
          } else {
            newFullValueList[newPosition] = values;
          }
        }
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#remapData(int[][])
     */
    @Override
    protected void remapData(int[][] mapping) throws IOException {
      super.remapData(mapping);
      T1[][] originalFullValueList = fullValueList.clone();
      fullValueList = operations.createMatrix1(mapping.length);
      for (int i = 0; i < mapping.length; i++) {
        for (int j = 0; j < mapping[i].length; j++) {
          if (j == 0) {
            setValue(i, originalFullValueList[mapping[i][j]],
                originalFullValueList[mapping[i][j]].length, false);
          } else {
            setValue(i, originalFullValueList[mapping[i][j]],
                originalFullValueList[mapping[i][j]].length, true);
          }
        }
      }
      fullValueList = newFullValueList;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#merge(mtas.codec.util.DataCollector.MtasDataCollector)
     */
    @Override
    public void merge(MtasDataCollector<?, ?> newDataCollector)
        throws IOException {
      closeNewList();
      if (!collectorType.equals(newDataCollector.getCollectorType())
          || !dataType.equals(newDataCollector.getDataType())
          || !statsType.equals(newDataCollector.getStatsType())
          || !(newDataCollector instanceof MtasDataFull)) {
        throw new IOException("cannot merge different dataCollectors");
      } else {
        MtasDataFull<T1, T2, T3> newMtasDataFull = (MtasDataFull<T1, T2, T3>) newDataCollector;
        closeNewList();
        initNewList(newMtasDataFull.getSize());
        if (collectorType.equals(COLLECTOR_TYPE_LIST)) {
          String[] keys = new String[1];
          for (int i = 0; i < newMtasDataFull.getSize(); i++) {
            if (newMtasDataFull.fullValueList[i].length > 0) {
              MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector<?, ?>[1];
              subCollectors[0] = add(newMtasDataFull.keyList[i]);
              setError(newCurrentPosition, newMtasDataFull.errorNumber[i],
                  newMtasDataFull.errorList[i], newCurrentExisting);
              setValue(newCurrentPosition, newMtasDataFull.fullValueList[i],
                  newMtasDataFull.fullValueList[i].length, newCurrentExisting);
              if (hasSub() && newMtasDataFull.hasSub()) {
                // single key implies exactly one subCollector if hasSub
                subCollectors[0]
                    .merge(newMtasDataFull.subCollectorListNextLevel[i]);
              }
            }
          }
        } else if (collectorType.equals(COLLECTOR_TYPE_DATA)) {
          if (newMtasDataFull.getSize() > 0) {
            MtasDataCollector<?, ?> subCollector = add();
            setError(newCurrentPosition, newMtasDataFull.errorNumber[0],
                newMtasDataFull.errorList[0], newCurrentExisting);
            setValue(newCurrentPosition, newMtasDataFull.fullValueList[0],
                newMtasDataFull.fullValueList[0].length, newCurrentExisting);
            if (hasSub() && newMtasDataFull.hasSub()) {
              subCollector.merge(newMtasDataFull.subCollectorNextLevel);
            }
          }
        } else {
          throw new IOException("cannot merge " + collectorType);
        }
        closeNewList();
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#initNewList(int)
     */
    @Override
    public final void initNewList(int maxNumberOfTerms) {
      super.initNewList(maxNumberOfTerms);
      newFullValueList = operations.createMatrix1(newSize);
    }

  }

  /**
   * The Class MtasDataLongBasic.
   */
  private static class MtasDataLongBasic
      extends MtasDataBasic<Long, Double, MtasDataItemLongBasic> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new mtas data long basic.
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
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDataLongBasic(String collectorType, TreeSet<String> statsItems,
        String sortType, String sortDirection, Integer start, Integer number,
        String[] subCollectorTypes, String[] subDataTypes,
        String[] subStatsTypes, TreeSet<String>[] subStatsItems,
        String[] subSortTypes, String[] subSortDirections, Integer[] subStart,
        Integer[] subNumber) throws IOException {
      super(collectorType, CodecUtil.DATA_TYPE_LONG, statsItems, sortType,
          sortDirection, start, number, subCollectorTypes, subDataTypes,
          subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
          subStart, subNumber, new MtasDataLongOperations());
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#getItem(int)
     */
    @Override
    protected MtasDataItemLongBasic getItem(int i) {
      return new MtasDataItemLongBasic(basicValueSumList[i], basicValueNList[i],
          hasSub() ? subCollectorListNextLevel[i] : null, statsItems, sortType,
          sortDirection, errorNumber[i], errorList[i]);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(long valueSum, long valueN)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      setValue(newCurrentPosition, valueSum, valueN, newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(double valueSum, long valueN)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      setValue(newCurrentPosition, Double.valueOf(valueSum).longValue(), valueN,
          newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, long valueSum,
        long valueN) throws IOException {
      if (keys != null && keys.length > 0) {
        MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector<?, ?>[keys.length];
        for (int i = 0; i < keys.length; i++) {
          subCollectors[i] = add(keys[i]);
          setValue(newCurrentPosition, valueSum, valueN, newCurrentExisting);
        }
        return subCollectors;
      } else {
        return null;
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long[], int)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, double valueSum,
        long valueN) throws IOException {
      if (keys != null && keys.length > 0) {
        MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector<?, ?>[keys.length];
        for (int i = 0; i < keys.length; i++) {
          subCollectors[i] = add(keys[i]);
          setValue(newCurrentPosition, Double.valueOf(valueSum).longValue(),
              valueN, newCurrentExisting);
        }
        return subCollectors;
      } else {
        return null;
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double[], int)
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

  }

  /**
   * The Class MtasDataDoubleBasic.
   */
  private static class MtasDataDoubleBasic
      extends MtasDataBasic<Double, Double, MtasDataItemDoubleBasic> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new mtas data double basic.
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
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDataDoubleBasic(String collectorType, TreeSet<String> statsItems,
        String sortType, String sortDirection, Integer start, Integer number,
        String[] subCollectorTypes, String[] subDataTypes,
        String[] subStatsTypes, TreeSet<String>[] subStatsItems,
        String[] subSortTypes, String[] subSortDirections, Integer[] subStart,
        Integer[] subNumber) throws IOException {
      super(collectorType, CodecUtil.DATA_TYPE_DOUBLE, statsItems, sortType,
          sortDirection, start, number, subCollectorTypes, subDataTypes,
          subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
          subStart, subNumber, new MtasDataDoubleOperations());
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#getItem(int)
     */
    @Override
    protected MtasDataItemDoubleBasic getItem(int i) {
      return new MtasDataItemDoubleBasic(basicValueSumList[i],
          basicValueNList[i], hasSub() ? subCollectorListNextLevel[i] : null,
          statsItems, sortType, sortDirection, errorNumber[i], errorList[i]);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(long valueSum, long valueN)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      setValue(newCurrentPosition, Double.valueOf(valueSum), valueN,
          newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long[], int)
     */
    @Override
    public MtasDataCollector<?, ?> add(long[] values, int number)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      Double[] newValues = new Double[number];
      for (int i = 0; i < values.length; i++)
        newValues[i] = Long.valueOf(values[i]).doubleValue();
      setValue(newCurrentPosition, newValues, number, newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(double valueSum, long valueN)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      setValue(newCurrentPosition, valueSum, valueN, newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double[], int)
     */
    @Override
    public MtasDataCollector<?, ?> add(double[] values, int number)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      setValue(newCurrentPosition, ArrayUtils.toObject(values), number,
          newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, long valueSum,
        long valueN) throws IOException {
      if (keys != null && keys.length > 0) {
        MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector<?, ?>[keys.length];
        for (int i = 0; i < keys.length; i++) {
          subCollectors[i] = add(keys[i]);
          setValue(newCurrentPosition, Double.valueOf(valueSum), valueN,
              newCurrentExisting);
        }
        return subCollectors;
      } else {
        return null;
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long[], int)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, long[] values,
        int number) throws IOException {
      if (keys != null && keys.length > 0) {
        Double[] newValues = new Double[number];
        for (int i = 0; i < values.length; i++)
          newValues[i] = Long.valueOf(values[i]).doubleValue();
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
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, double valueSum,
        long valueN) throws IOException {
      if (keys != null && keys.length > 0) {
        MtasDataCollector<?, ?>[] subCollectors = new MtasDataCollector<?, ?>[keys.length];
        for (int i = 0; i < keys.length; i++) {
          subCollectors[i] = add(keys[i]);
          setValue(newCurrentPosition, valueSum, valueN, newCurrentExisting);
        }
        return subCollectors;
      } else {
        return null;
      }
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double[], int)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, double[] values,
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

  }

  /**
   * The Class MtasDataLongAdvanced.
   */
  private static class MtasDataLongAdvanced
      extends MtasDataAdvanced<Long, Double, MtasDataItemLongAdvanced> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new mtas data long advanced.
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
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDataLongAdvanced(String collectorType,
        TreeSet<String> statsItems, String sortType, String sortDirection,
        Integer start, Integer number, String[] subCollectorTypes,
        String[] subDataTypes, String[] subStatsTypes,
        TreeSet<String>[] subStatsItems, String[] subSortTypes,
        String[] subSortDirections, Integer[] subStart, Integer[] subNumber)
        throws IOException {
      super(collectorType, CodecUtil.DATA_TYPE_LONG, statsItems, sortType,
          sortDirection, start, number, subCollectorTypes, subDataTypes,
          subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
          subStart, subNumber, new MtasDataLongOperations());
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#getItem(int)
     */
    @Override
    protected final MtasDataItemLongAdvanced getItem(int i) {
      return new MtasDataItemLongAdvanced(advancedValueSumList[i],
          advancedValueSumOfLogsList[i], advancedValueSumOfSquaresList[i],
          advancedValueMinList[i], advancedValueMaxList[i],
          advancedValueNList[i], hasSub() ? subCollectorListNextLevel[i] : null,
          statsItems, sortType, sortDirection, errorNumber[i], errorList[i]);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(long valueSum, long valueN)
        throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(double valueSum, long valueN)
        throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, long valueSum,
        long valueN) throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long[], int)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, double valueSum,
        long valueN) throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double[], int)
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

  }

  /**
   * The Class MtasDataDoubleAdvanced.
   */
  private static class MtasDataDoubleAdvanced
      extends MtasDataAdvanced<Double, Double, MtasDataItemDoubleAdvanced> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new mtas data double advanced.
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
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDataDoubleAdvanced(String collectorType,
        TreeSet<String> statsItems, String sortType, String sortDirection,
        Integer start, Integer number, String[] subCollectorTypes,
        String[] subDataTypes, String[] subStatsTypes,
        TreeSet<String>[] subStatsItems, String[] subSortTypes,
        String[] subSortDirections, Integer[] subStart, Integer[] subNumber)
        throws IOException {
      super(collectorType, CodecUtil.DATA_TYPE_DOUBLE, statsItems, sortType,
          sortDirection, start, number, subCollectorTypes, subDataTypes,
          subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
          subStart, subNumber, new MtasDataDoubleOperations());
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#getItem(int)
     */
    @Override
    protected final MtasDataItemDoubleAdvanced getItem(int i) {
      return new MtasDataItemDoubleAdvanced(advancedValueSumList[i],
          advancedValueSumOfLogsList[i], advancedValueSumOfSquaresList[i],
          advancedValueMinList[i], advancedValueMaxList[i],
          advancedValueNList[i], hasSub() ? subCollectorListNextLevel[i] : null,
          statsItems, sortType, sortDirection, errorNumber[i], errorList[i]);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(long valueSum, long valueN)
        throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long[], int)
     */
    @Override
    public MtasDataCollector<?, ?> add(long[] values, int number)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      Double[] newValues = new Double[number];
      for (int i = 0; i < values.length; i++)
        newValues[i] = Long.valueOf(values[i]).doubleValue();
      setValue(newCurrentPosition, newValues, number, newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(double valueSum, long valueN)
        throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double[], int)
     */
    @Override
    public MtasDataCollector<?, ?> add(double[] values, int number)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      setValue(newCurrentPosition, ArrayUtils.toObject(values), number,
          newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, long valueSum,
        long valueN) throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long[], int)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, long[] values,
        int number) throws IOException {
      if (keys != null && keys.length > 0) {
        Double[] newValues = new Double[number];
        for (int i = 0; i < values.length; i++)
          newValues[i] = Long.valueOf(values[i]).doubleValue();
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
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, double valueSum,
        long valueN) throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double[], int)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, double[] values,
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

  }

  /**
   * The Class MtasDataLongFull.
   */
  private static class MtasDataLongFull
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
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDataLongFull(String collectorType, TreeSet<String> statsItems,
        String sortType, String sortDirection, Integer start, Integer number,
        String[] subCollectorTypes, String[] subDataTypes,
        String[] subStatsTypes, TreeSet<String>[] subStatsItems,
        String[] subSortTypes, String[] subSortDirections, Integer[] subStart,
        Integer[] subNumber) throws IOException {
      super(collectorType, CodecUtil.DATA_TYPE_LONG, statsItems, sortType,
          sortDirection, start, number, subCollectorTypes, subDataTypes,
          subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
          subStart, subNumber, new MtasDataLongOperations());
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#getItem(int)
     */
    @Override
    protected MtasDataItemLongFull getItem(int i) {
      return new MtasDataItemLongFull(ArrayUtils.toPrimitive(fullValueList[i]),
          hasSub() ? subCollectorListNextLevel[i] : null, statsItems, sortType,
          sortDirection, errorNumber[i], errorList[i]);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(long valueSum, long valueN)
        throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(double valueSum, long valueN)
        throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, long valueSum,
        long valueN) throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long[], int)
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

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, double valueSum,
        long valueN) throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double[], int)
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

  }

  /**
   * The Class MtasDataDoubleFull.
   */
  private static class MtasDataDoubleFull
      extends MtasDataFull<Double, Double, MtasDataItemDoubleFull> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new mtas data double full.
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
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDataDoubleFull(String collectorType, TreeSet<String> statsItems,
        String sortType, String sortDirection, Integer start, Integer number,
        String[] subCollectorTypes, String[] subDataTypes,
        String[] subStatsTypes, TreeSet<String>[] subStatsItems,
        String[] subSortTypes, String[] subSortDirections, Integer[] subStart,
        Integer[] subNumber) throws IOException {
      super(collectorType, CodecUtil.DATA_TYPE_DOUBLE, statsItems, sortType,
          sortDirection, start, number, subCollectorTypes, subDataTypes,
          subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
          subStart, subNumber, new MtasDataDoubleOperations());
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#getItem(int)
     */
    @Override
    protected MtasDataItemDoubleFull getItem(int i) {
      return new MtasDataItemDoubleFull(
          ArrayUtils.toPrimitive(fullValueList[i]),
          hasSub() ? subCollectorListNextLevel[i] : null, statsItems, sortType,
          sortDirection, errorNumber[i], errorList[i]);
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(long valueSum, long valueN)
        throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(long[], int)
     */
    @Override
    public MtasDataCollector<?, ?> add(long[] values, int number)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      Double[] newValues = new Double[number];
      for (int i = 0; i < values.length; i++)
        newValues[i] = Long.valueOf(values[i]).doubleValue();
      setValue(newCurrentPosition, newValues, number, newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double, long)
     */
    @Override
    public MtasDataCollector<?, ?> add(double valueSum, long valueN)
        throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(double[], int)
     */
    @Override
    public MtasDataCollector<?, ?> add(double[] values, int number)
        throws IOException {
      MtasDataCollector<?, ?> dataCollector = add();
      setValue(newCurrentPosition, ArrayUtils.toObject(values), number,
          newCurrentExisting);
      return dataCollector;
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, long valueSum,
        long valueN) throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], long[], int)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, long[] values,
        int number) throws IOException {
      if (keys != null && keys.length > 0) {
        Double[] newValues = new Double[number];
        for (int i = 0; i < values.length; i++)
          newValues[i] = Long.valueOf(values[i]).doubleValue();
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
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double, long)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, double valueSum,
        long valueN) throws IOException {
      throw new IOException("not supported");
    }

    /* (non-Javadoc)
     * @see mtas.codec.util.DataCollector.MtasDataCollector#add(java.lang.String[], double[], int)
     */
    @Override
    public MtasDataCollector<?, ?>[] add(String[] keys, double[] values,
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

  }

}
