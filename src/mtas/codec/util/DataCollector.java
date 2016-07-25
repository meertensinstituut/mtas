package mtas.codec.util;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import mtas.codec.util.collector.MtasDataDoubleAdvanced;
import mtas.codec.util.collector.MtasDataDoubleBasic;
import mtas.codec.util.collector.MtasDataDoubleFull;
import mtas.codec.util.collector.MtasDataItem;
import mtas.codec.util.collector.MtasDataLongAdvanced;
import mtas.codec.util.collector.MtasDataLongBasic;
import mtas.codec.util.collector.MtasDataLongFull;

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
   * @param segmentRegistration the segment registration
   * @return the collector
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static MtasDataCollector<?, ?> getCollector(String collectorType,
      String dataType, String statsType, TreeSet<String> statsItems,
      String sortType, String sortDirection, Integer start, Integer number,
      String[] subCollectorTypes, String[] subDataTypes, String[] subStatsTypes,
      TreeSet<String>[] subStatsItems, String[] subSortTypes,
      String[] subSortDirections, Integer[] subStart, Integer[] subNumber,
      boolean segmentRegistration) throws IOException {
    if (dataType != null && dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
      if (statsType.equals(CodecUtil.STATS_BASIC)) {
        return new MtasDataLongBasic(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration);
      } else if (statsType.equals(CodecUtil.STATS_ADVANCED)) {
        return new MtasDataLongAdvanced(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration);
      } else if (statsType.equals(CodecUtil.STATS_FULL)) {
        return new MtasDataLongFull(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration);
      } else {
        throw new IOException("unknown statsType " + statsType);
      }
    } else if (dataType != null
        && dataType.equals(CodecUtil.DATA_TYPE_DOUBLE)) {
      if (statsType.equals(CodecUtil.STATS_BASIC)) {
        return new MtasDataDoubleBasic(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration);
      } else if (statsType.equals(CodecUtil.STATS_ADVANCED)) {
        return new MtasDataDoubleAdvanced(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration);
      } else if (statsType.equals(CodecUtil.STATS_FULL)) {
        return new MtasDataDoubleFull(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration);
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

    /** The segment registration. */
    protected boolean segmentRegistration;

    /** The segment key value list. */
    protected LinkedHashMap<String, HashMap<String, T1>> segmentKeyValueList;

    /** The segment recompute key list. */
    protected LinkedHashMap<String, HashSet<String>> segmentRecomputeKeyList;

    /** The segment keys. */
    protected HashSet<String> segmentKeys;

    /** The segment value boundary. */
    protected LinkedHashMap<String, T1> segmentValueBoundary;
    
    /** The segment value max list min. */
    protected LinkedHashMap<String, T1> segmentValueMaxListMin;

    /** The segment value max list. */
    protected ArrayList<T1> segmentValueMaxList;

    /** The segment name. */
    protected String segmentName;

    /** The segment number. */
    protected int segmentNumber;

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
     * @param segmentRegistration the segment registration
     */
    protected MtasDataCollector(String collectorType, String dataType,
        String statsType, TreeSet<String> statsItems, String sortType,
        String sortDirection, Integer start, Integer number,
        boolean segmentRegistration) {
      // set properties
      this.collectorType = collectorType; // data or list
      this.dataType = dataType; // long or double
      this.statsType = statsType; // basic, advanced or full
      this.statsItems = statsItems; // sum, n, all, ...
      this.sortType = sortType;
      this.sortDirection = sortDirection;
      this.start = start;
      this.number = number;
      this.segmentRegistration = segmentRegistration;
      if (segmentRegistration) {
        segmentKeys = new HashSet<String>();
        segmentKeyValueList = new LinkedHashMap<String, HashMap<String, T1>>();
        segmentValueBoundary = new LinkedHashMap<String, T1>();
        segmentValueMaxListMin = new LinkedHashMap<String, T1>();
      }
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
     * @param segmentRegistration the segment registration
     */
    protected MtasDataCollector(String collectorType, String dataType,
        String statsType, TreeSet<String> statsItems, String sortType,
        String sortDirection, Integer start, Integer number,
        String[] subCollectorTypes, String[] subDataTypes,
        String[] subStatsTypes, TreeSet<String>[] subStatsItems,
        String subSortTypes[], String[] subSortDirections, Integer[] subStart,
        Integer[] subNumber, boolean segmentRegistration) {
      // initialize
      this(collectorType, dataType, statsType, statsItems, sortType,
          sortDirection, start, number, segmentRegistration);
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
     * @param segmentName the segment name
     * @param segmentNumber the segment number
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected void initNewList(int maxNumberOfTerms, String segmentName,
        int segmentNumber) throws IOException {
      initNewListBasic(maxNumberOfTerms);
      if (segmentRegistration) {        
        this.segmentName = segmentName;
        this.segmentNumber = segmentNumber;
        if (!segmentKeyValueList.containsKey(segmentName)) {
          segmentKeyValueList.put(segmentName, new HashMap<String, T1>());
          segmentValueBoundary.put(segmentName, null);
          segmentValueMaxListMin.put(segmentName, null);
        }
        this.segmentValueMaxList = new ArrayList<T1>();
      } 
    }

    /**
     * Inits the new list.
     *
     * @param maxNumberOfTerms the max number of terms
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected void initNewList(int maxNumberOfTerms) throws IOException {
      if (segmentRegistration) {
        throw new IOException("missing segment name");
      } else {
        initNewListBasic(maxNumberOfTerms);
      }
    }

    /**
     * Inits the new list basic.
     *
     * @param maxNumberOfTerms the max number of terms
     */
    private void initNewListBasic(int maxNumberOfTerms) {
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
                newSubSortDirections, newSubStart, newSubNumber,
                segmentRegistration);
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
              newSubSortDirections, newSubStart, newSubNumber,
              segmentRegistration);
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
     * Compare for computing segment.
     *
     * @param value the value
     * @param boundary the boundary
     * @return true, if successful
     */
    protected abstract boolean compareForComputingSegment(T1 value,
        T1 boundary);

    /**
     * Minimum for computing segment.
     *
     * @param value the value
     * @param boundary the boundary
     * @return the t1
     */
    protected abstract T1 minimumForComputingSegment(T1 value, T1 boundary);

    /**
     * Minimum for computing segment.
     *
     * @return the t1
     */
    protected abstract T1 minimumForComputingSegment();

    /**
     * Boundary for segment.
     *
     * @return the t1
     */
    protected abstract T1 boundaryForSegment();

    /**
     * Boundary for computing segment.
     *
     * @return the t1
     */
    protected abstract T1 boundaryForComputingSegment();

    /**
     * Close segment key value registration.
     */
    public void closeSegmentKeyValueRegistration() {
      if(segmentRegistration) {
        HashMap<String, T1> keyValueList = segmentKeyValueList.get(segmentName);
        T1 tmpSegmentValueBoundary = segmentValueBoundary.get(segmentName);
        for (String key : keyValueList.keySet()) {
          if (tmpSegmentValueBoundary == null || compareForComputingSegment(
              keyValueList.get(key), tmpSegmentValueBoundary)) {
            segmentKeys.add(key);
          }
        }
      }  
    }

    /**
     * Recompute segment keys.
     */
    public void recomputeSegmentKeys() {
      segmentKeys.clear();
      segmentRecomputeKeyList = new LinkedHashMap<String, HashSet<String>>();
      // recompute boundaries      
      for (String segmentName : segmentKeyValueList.keySet()) {
        this.segmentName = segmentName;
        T1 tmpSegmentValueBoundary = boundaryForSegment();
        segmentValueBoundary.put(segmentName, tmpSegmentValueBoundary);
      }
      // compute adjusted boundaries and compute keys
      for (String segmentName : segmentKeyValueList.keySet()) {
        this.segmentName = segmentName;
        HashMap<String, T1> keyValueList = segmentKeyValueList.get(segmentName);
        T1 tmpSegmentValueBoundaryForComputing = boundaryForComputingSegment();
        for (String key : keyValueList.keySet()) {
          if (tmpSegmentValueBoundaryForComputing == null
              || compareForComputingSegment(keyValueList.get(key),
                  tmpSegmentValueBoundaryForComputing)) {
            if (!segmentKeys.contains(key)) {
              segmentKeys.add(key);
            }
          }
        }
      }
      HashMap<String, T1> keyValueList;
      HashSet<String> recomputeKeyList;
      for (String key : segmentKeys) {        
        for (String segmentName : segmentKeyValueList.keySet()) {
          keyValueList = segmentKeyValueList.get(segmentName);
          if(!keyValueList.containsKey(key)) {
            if(!segmentRecomputeKeyList.containsKey(segmentName)) {
              recomputeKeyList = new HashSet<String>();
              segmentRecomputeKeyList.put(segmentName, recomputeKeyList);
            } else {
              recomputeKeyList = segmentRecomputeKeyList.get(segmentName);
            }
            recomputeKeyList.add(key);
          } else {
            break;
          }
        }
      }
      this.segmentName = null;
    }

    /**
     * Check existence necessary keys.
     *
     * @return true, if successful
     */
    public boolean checkExistenceNecessaryKeys() {
      return segmentRecomputeKeyList.size() == 0;
    }

    /**
     * Validate segment value.
     *
     * @param key the key
     * @param value the value
     * @param maximumNumber the maximum number
     * @param segmentNumber the segment number
     * @return true, if successful
     */
    public boolean validateSegmentValue(String key, T1 value, int maximumNumber,
        int segmentNumber) {  
      if(segmentRegistration) {
        if(maximumNumber>0) {
          segmentKeyValueList.get(segmentName).put(key, value);
          T1 tmpSegmentValueMaxListMin = segmentValueMaxListMin.get(segmentName);
          T1 tmpSegmentValueBoundary = segmentValueBoundary.get(segmentName);
          if (segmentValueMaxList.size() < maximumNumber) {
            segmentValueMaxList.add(value);
            segmentValueMaxListMin.put(segmentName,
                (tmpSegmentValueMaxListMin == null) ? value
                    : minimumForComputingSegment(tmpSegmentValueMaxListMin, value));
            if (segmentValueMaxList.size() == maximumNumber) {
              tmpSegmentValueMaxListMin = segmentValueMaxListMin.get(segmentName);
              segmentValueMaxListMin.put(segmentName, tmpSegmentValueMaxListMin);
              segmentValueBoundary.put(segmentName, boundaryForComputingSegment());
            }
            return true;
          } else if (compareForComputingSegment(value, tmpSegmentValueBoundary)) {
            if (compareForComputingSegment(value, tmpSegmentValueMaxListMin)) {
              segmentValueMaxList.remove(tmpSegmentValueMaxListMin);
              segmentValueMaxList.add(value);
              tmpSegmentValueMaxListMin = minimumForComputingSegment();
              segmentValueMaxListMin.put(segmentName, tmpSegmentValueMaxListMin);
              segmentValueBoundary.put(segmentName, boundaryForComputingSegment());
            }
            return true;
          } else if (segmentKeys.contains(key)) {
            return true;
          } else {
            return false;
          }
        } else {
          return false;
        }
      } else {
        return true;
      }
    }

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
      if (segmentRegistration) {
        this.segmentName = null;
      }
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

    /*
     * (non-Javadoc)
     * 
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

    /**
     * Write object.
     *
     * @param out the out
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
      segmentRegistration = false;
      out.defaultWriteObject();
    }

  }


 


 

}
