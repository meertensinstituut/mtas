package mtas.codec.util.collector;

import mtas.codec.util.DataCollector;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

public abstract class MtasDataCollector<T1 extends Number & Comparable<T1>, T2 extends Number & Comparable<T2>>
    implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SEGMENT_SORT_ASC = "segment_asc";
  public static final String SEGMENT_SORT_DESC = "segment_desc";
  public static final String SEGMENT_BOUNDARY_ASC = "segment_boundary_asc";
  public static final String SEGMENT_BOUNDARY_DESC = "segment_boundary_desc";
  public static final String SEGMENT_KEY = "key";
  public static final String SEGMENT_NEW = "new";
  public static final String SEGMENT_KEY_OR_NEW = "key_or_new";
  public static final String SEGMENT_POSSIBLE_KEY = "possible_key";

  protected int size;
  protected int position;
  protected String collectorType; // properties collector
  protected String statsType;
  protected String dataType;
  private SortedSet<String> statsItems;
  protected String sortType;
  protected String sortDirection;
  protected Integer start;
  protected Integer number;
  protected int[] errorNumber;
  protected HashMap<String, Integer>[] errorList;
  protected String[] keyList;
  protected int[] sourceNumberList;
  private boolean withTotal;

  public transient String segmentRegistration;
  protected transient LinkedHashMap<String, Map<String, T1>> segmentKeyValueList;
  public transient Map<String, Set<String>> segmentRecomputeKeyList;
  public transient Set<String> segmentKeys;
  protected transient Map<String, T1> segmentValuesBoundary;
  protected transient T1 segmentValueBoundary;
  protected transient Map<String, T1> segmentValueTopListLast;
  protected transient ArrayList<T1> segmentValueTopList;
  protected transient String segmentName;
  protected transient int segmentNumber;

  private boolean hasSub;
  private String[] subCollectorTypes;
  private String[] subDataTypes;
  private String[] subStatsTypes;
  private SortedSet<String>[] subStatsItems;
  private String[] subSortTypes;
  private String[] subSortDirections;
  private Integer[] subStart;
  private Integer[] subNumber;

  protected MtasDataCollector<?, ?>[] subCollectorListNextLevel = null;
  protected MtasDataCollector<?, ?> subCollectorNextLevel = null;
  protected transient int newSize;
  protected transient int newPosition;
  protected transient int newCurrentPosition;
  protected transient boolean newCurrentExisting;
  protected transient String[] newKeyList = null;
  protected transient int[] newSourceNumberList = null;
  protected transient int[] newErrorNumber;
  protected transient HashMap<String, Integer>[] newErrorList;

  public transient Set<String> newKnownKeyFoundInSegment;

  private transient String[] newSubCollectorTypes;
  private transient String[] newSubDataTypes;
  private transient String[] newSubStatsTypes;
  private transient SortedSet<String>[] newSubStatsItems;
  private transient String[] newSubSortTypes;
  private transient String[] newSubSortDirections;
  private transient Integer[] newSubStart;
  private transient Integer[] newSubNumber;

  protected transient MtasDataCollector<?, ?>[] newSubCollectorListNextLevel = null;
  protected transient MtasDataCollector<?, ?> newSubCollectorNextLevel = null;
  protected transient boolean closed = false;
  private transient MtasDataCollectorResult<T1, T2> result = null;

  @SuppressWarnings("unchecked")
  protected MtasDataCollector(String collectorType, String dataType,
      String statsType, SortedSet<String> statsItems, String sortType,
      String sortDirection, Integer start, Integer number,
      String segmentRegistration, String boundary) throws IOException {
    // set properties
    this.closed = false;
    this.collectorType = collectorType; // data or list
    this.dataType = dataType; // long or double
    this.statsType = statsType; // basic, advanced or full
    this.statsItems = statsItems; // sum, n, all, ...
    this.sortType = sortType;
    this.sortDirection = sortDirection;
    this.start = start;
    this.number = number;
    this.segmentRegistration = segmentRegistration;
    this.withTotal = false;
    if (segmentRegistration != null) {
      segmentKeys = new HashSet<>();
      segmentKeyValueList = new LinkedHashMap<>();
      segmentValuesBoundary = new LinkedHashMap<>();
      segmentValueTopListLast = new LinkedHashMap<>();
      if (segmentRegistration.equals(SEGMENT_BOUNDARY_ASC)
          || segmentRegistration.equals(SEGMENT_BOUNDARY_DESC)) {
        if (boundary != null) {
          segmentValueBoundary = stringToBoundary(boundary);
        } else {
          throw new IOException("did expect boundary with segmentRegistration "
              + segmentRegistration);
        }
      } else if (boundary != null) {
        throw new IOException("didn't expect boundary with segmentRegistration "
            + segmentRegistration);
      }
    }
    // initialize administration
    keyList = new String[0];
    sourceNumberList = new int[0];
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

  protected MtasDataCollector(String collectorType, String dataType,
      String statsType, SortedSet<String> statsItems, String sortType,
      String sortDirection, Integer start, Integer number,
      String[] subCollectorTypes, String[] subDataTypes, String[] subStatsTypes,
      SortedSet<String>[] subStatsItems, String[] subSortTypes,
      String[] subSortDirections, Integer[] subStart, Integer[] subNumber,
      String segmentRegistration, String boundary) throws IOException {
    // initialize
    this(collectorType, dataType, statsType, statsItems, sortType,
        sortDirection, start, number, segmentRegistration, boundary);
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

  abstract public void merge(MtasDataCollector<?, ?> newDataCollector,
      Map<MtasDataCollector<?, ?>, MtasDataCollector<?, ?>> map,
      boolean increaseSourceNumber) throws IOException;

  public void initNewList(int maxNumberOfTerms, String segmentName,
      int segmentNumber, String boundary) throws IOException {
    if (closed) {
      result = null;
      closed = false;
    }
    initNewListBasic(maxNumberOfTerms);
    if (segmentRegistration != null) {
      this.segmentName = segmentName;
      this.segmentNumber = segmentNumber;
      if (!segmentKeyValueList.containsKey(segmentName)) {
        segmentKeyValueList.put(segmentName, new HashMap<String, T1>());
        if (segmentRegistration.equals(SEGMENT_BOUNDARY_ASC)
            || segmentRegistration.equals(SEGMENT_BOUNDARY_DESC)) {
          if (boundary != null) {
            segmentValuesBoundary.put(segmentName,
                stringToBoundary(boundary, segmentNumber));
          } else {
            throw new IOException("expected boundary");
          }
        } else {
          segmentValuesBoundary.put(segmentName, null);
        }
        segmentValueTopListLast.put(segmentName, null);
      }
      this.segmentValueTopList = new ArrayList<>();
    }
  }

  public void initNewList(int maxNumberOfTerms) throws IOException {
    if (closed) {
      result = null;
      closed = false;
    }
    if (segmentRegistration != null) {
      throw new IOException("missing segment name");
    } else {
      initNewListBasic(maxNumberOfTerms);
    }
  }

  @SuppressWarnings("unchecked")
  private void initNewListBasic(int maxNumberOfTerms) throws IOException {
    if (!closed) {
      position = 0;
      newPosition = 0;
      newCurrentPosition = 0;
      newSize = maxNumberOfTerms + size;
      newKeyList = new String[newSize];
      newSourceNumberList = new int[newSize];
      newErrorNumber = new int[newSize];
      newErrorList = (HashMap<String, Integer>[]) new HashMap<?, ?>[newSize];
      newKnownKeyFoundInSegment = new HashSet<>();
      if (hasSub) {
        newSubCollectorListNextLevel = new MtasDataCollector[newSize];
      }
    } else {
      throw new IOException("already closed");
    }
  }

  @SuppressWarnings("unchecked")
  protected void increaseNewListSize() throws IOException {
    if (!closed) {
      String[] tmpNewKeyList = newKeyList;
      int[] tmpNewSourceNumberList = newSourceNumberList;
      int[] tmpNewErrorNumber = newErrorNumber;
      HashMap<String, Integer>[] tmpNewErrorList = newErrorList;
      int tmpNewSize = newSize;
      newSize = 2 * newSize;
      newKeyList = new String[newSize];
      newSourceNumberList = new int[newSize];
      newErrorNumber = new int[newSize];
      newErrorList = (HashMap<String, Integer>[]) new HashMap<?, ?>[newSize];
      System.arraycopy(tmpNewKeyList, 0, newKeyList, 0, tmpNewSize);
      System.arraycopy(tmpNewSourceNumberList, 0, newSourceNumberList, 0,
          tmpNewSize);
      System.arraycopy(tmpNewErrorNumber, 0, newErrorNumber, 0, tmpNewSize);
      System.arraycopy(tmpNewErrorList, 0, newErrorList, 0, tmpNewSize);
      if (hasSub) {
        MtasDataCollector<?, ?>[] tmpNewSubCollectorListNextLevel = newSubCollectorListNextLevel;
        newSubCollectorListNextLevel = new MtasDataCollector[newSize];
        System.arraycopy(tmpNewSubCollectorListNextLevel, 0,
            newSubCollectorListNextLevel, 0, tmpNewSize);
      }
    } else {
      throw new IOException("already closed");
    }
  }

  protected final MtasDataCollector add(boolean increaseSourceNumber)
      throws IOException {
    if (!closed) {
      if (!collectorType.equals(DataCollector.COLLECTOR_TYPE_DATA)) {
        throw new IOException(
            "collector should be " + DataCollector.COLLECTOR_TYPE_DATA);
      } else {
        if (newPosition > 0) {
          newCurrentExisting = true;
        } else if (position < getSize()) {
          // copy
          newKeyList[0] = keyList[0];
          newSourceNumberList[0] = sourceNumberList[0];
          if (increaseSourceNumber) {
            newSourceNumberList[0]++;
          }
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
          newKeyList[0] = DataCollector.COLLECTOR_TYPE_DATA;
          newSourceNumberList[0] = 1;
          newErrorNumber[0] = 0;
          newErrorList[0] = new HashMap<>();
          newPosition = 1;
          newCurrentPosition = newPosition - 1;
          newCurrentExisting = false;
          // ready, only handle sub
          if (hasSub) {
            newSubCollectorNextLevel = DataCollector.getCollector(
                subCollectorTypes[0], subDataTypes[0], subStatsTypes[0],
                subStatsItems[0], subSortTypes[0], subSortDirections[0],
                subStart[0], subNumber[0], newSubCollectorTypes,
                newSubDataTypes, newSubStatsTypes, newSubStatsItems,
                newSubSortTypes, newSubSortDirections, newSubStart,
                newSubNumber, segmentRegistration, null);
          } else {
            newSubCollectorNextLevel = null;
          }
        }
        return newSubCollectorNextLevel;
      }
    } else {
      throw new IOException("already closed");
    }
  }

  protected final MtasDataCollector add(String key,
      boolean increaseSourceNumber) throws IOException {
    if (!closed) {
      if (collectorType.equals(DataCollector.COLLECTOR_TYPE_DATA)) {
        throw new IOException(
            "collector should be " + DataCollector.COLLECTOR_TYPE_LIST);
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
            newSourceNumberList[newPosition] = sourceNumberList[position];
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
              if (increaseSourceNumber) {
                newSourceNumberList[(newPosition - 1)]++;
              }
              newCurrentPosition = newPosition - 1;
              newCurrentExisting = true;
              // register known key found again in segment
              newKnownKeyFoundInSegment.add(key);
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
        newSourceNumberList[newPosition] = 1;
        newErrorNumber[newPosition] = 0;
        newErrorList[newPosition] = new HashMap<>();
        newPosition++;
        newCurrentPosition = newPosition - 1;
        newCurrentExisting = false;
        // ready, only handle sub
        if (hasSub) {
          newSubCollectorListNextLevel[newCurrentPosition] = DataCollector
              .getCollector(subCollectorTypes[0], subDataTypes[0],
                  subStatsTypes[0], subStatsItems[0], subSortTypes[0],
                  subSortDirections[0], subStart[0], subNumber[0],
                  newSubCollectorTypes, newSubDataTypes, newSubStatsTypes,
                  newSubStatsItems, newSubSortTypes, newSubSortDirections,
                  newSubStart, newSubNumber, segmentRegistration, null);
          return newSubCollectorListNextLevel[newCurrentPosition];
        } else {
          return null;
        }
      }
    } else {
      throw new IOException("already closed");
    }
  }

  protected abstract void copyToNew(int position, int newPosition);

  protected abstract void copyFromNew();

  protected abstract boolean compareWithBoundary(T1 value, T1 boundary)
      throws IOException;

  protected abstract T1 lastForComputingSegment(T1 value, T1 boundary)
      throws IOException;

  protected abstract T1 lastForComputingSegment() throws IOException;

  protected abstract T1 boundaryForSegment(String segmentName)
      throws IOException;

  protected abstract T1 boundaryForSegmentComputing(String segmentName)
      throws IOException;

  protected abstract T1 stringToBoundary(String boundary, Integer segmentNumber)
      throws IOException;

  protected T1 stringToBoundary(String boundary) throws IOException {
    return stringToBoundary(boundary, null);
  }

  public void closeSegmentKeyValueRegistration() throws IOException {
    if (!closed) {
      if (segmentRegistration != null) {
        Map<String, T1> keyValueList = segmentKeyValueList.get(segmentName);
        T1 tmpSegmentValueBoundary = segmentValuesBoundary.get(segmentName);
        for (Entry<String, T1> entry : keyValueList.entrySet()) {
          if (tmpSegmentValueBoundary == null || compareWithBoundary(
              entry.getValue(), tmpSegmentValueBoundary)) {
            segmentKeys.add(entry.getKey());
          }
        }
      }
    } else {
      throw new IOException("already closed");
    }
  }

  public void recomputeSegmentKeys() throws IOException {
    if (!closed && segmentRegistration != null) {
      if (segmentRegistration.equals(SEGMENT_SORT_ASC)
          || segmentRegistration.equals(SEGMENT_SORT_DESC)
          || segmentRegistration.equals(SEGMENT_BOUNDARY_ASC)
          || segmentRegistration.equals(SEGMENT_BOUNDARY_DESC)) {

        if (segmentRegistration.equals(SEGMENT_SORT_ASC)
            || segmentRegistration.equals(SEGMENT_SORT_DESC)) {
          segmentKeys.clear();
          // recompute boundaries
          for (Entry<String, Map<String, T1>> entry : segmentKeyValueList
              .entrySet()) {
            T1 tmpSegmentValueBoundary = boundaryForSegment(entry.getKey());
            segmentValuesBoundary.put(entry.getKey(), tmpSegmentValueBoundary);
          }
          // compute adjusted boundaries and compute keys
          for (Entry<String, Map<String, T1>> entry : segmentKeyValueList
              .entrySet()) {
            this.segmentName = entry.getKey();
            Map<String, T1> keyValueList = entry.getValue();
            T1 tmpSegmentValueBoundaryForComputing = boundaryForSegmentComputing(
                entry.getKey());
            for (Entry<String, T1> subEntry : keyValueList.entrySet()) {
              if (tmpSegmentValueBoundaryForComputing == null
                  || compareWithBoundary(subEntry.getValue(),
                      tmpSegmentValueBoundaryForComputing)) {
                if (!segmentKeys.contains(subEntry.getKey())) {
                  segmentKeys.add(subEntry.getKey());
                }
              }
            }
          }
        }

        Map<String, T1> keyValueList;
        Set<String> recomputeKeyList;
        segmentRecomputeKeyList = new LinkedHashMap<>();
        for (String key : segmentKeys) {
          for (Entry<String, Map<String, T1>> entry : segmentKeyValueList
              .entrySet()) {
            keyValueList = entry.getValue();
            if (!keyValueList.containsKey(key)) {
              if (!segmentRecomputeKeyList.containsKey(entry.getKey())) {
                recomputeKeyList = new HashSet<>();
                segmentRecomputeKeyList.put(entry.getKey(), recomputeKeyList);
              } else {
                recomputeKeyList = segmentRecomputeKeyList.get(entry.getKey());
              }
              recomputeKeyList.add(key);
            }
          }
        }
        this.segmentName = null;
      } else {
        throw new IOException(
            "not for segmentRegistration " + segmentRegistration);
      }
    } else {
      throw new IOException("already closed or no segmentRegistration ("
          + segmentRegistration + ")");
    }
  }

  public abstract void reduceToKeys(Set<String> keys);

  public void reduceToSegmentKeys() {
    if (segmentRegistration != null) {
      reduceToKeys(segmentKeys);
    }
  }

  public boolean checkExistenceNecessaryKeys() throws IOException {
    if (!closed) {
      if (segmentRegistration != null) {
        return segmentRecomputeKeyList.size() == 0;
      } else {
        return true;
      }
    } else {
      throw new IOException("already closed");
    }
  }

  abstract public boolean validateSegmentBoundary(Object o) throws IOException;

  protected boolean validateWithSegmentBoundary(T1 value) throws IOException {
    if (!closed && segmentRegistration != null) {
      T1 tmpSegmentValueBoundary = segmentValuesBoundary.get(segmentName);
      if (tmpSegmentValueBoundary == null
          || compareWithBoundary(value, tmpSegmentValueBoundary)) {
        return true;
      }
    }
    return false;
  }

  public String validateSegmentValue(T1 value, int maximumNumber,
      int segmentNumber) throws IOException {
    if (!closed) {
      if (segmentRegistration != null) {
        if (maximumNumber > 0) {
          T1 tmpSegmentValueBoundary = segmentValuesBoundary.get(segmentName);
          if (segmentValueTopList.size() < maximumNumber
              || compareWithBoundary(value, tmpSegmentValueBoundary)) {
            return SEGMENT_KEY_OR_NEW;
          } else if (segmentKeys.size() > newKnownKeyFoundInSegment.size()) {
            return SEGMENT_POSSIBLE_KEY;
          } else {
            return null;
          }
        } else {
          return null;
        }
      } else {
        return null;
      }
    } else {
      throw new IOException("already closed");
    }
  }

  public String validateSegmentValue(String key, T1 value, int maximumNumber,
      int segmentNumber, boolean test) throws IOException {
    if (!closed) {
      if (segmentRegistration != null) {
        if (maximumNumber > 0) {
          T1 tmpSegmentValueMaxListMin = segmentValueTopListLast
              .get(segmentName);
          T1 tmpSegmentValueBoundary = segmentValuesBoundary.get(segmentName);
          if (segmentValueTopList.size() < maximumNumber) {
            if (!test) {
              segmentKeyValueList.get(segmentName).put(key, value);
              segmentValueTopList.add(value);
              segmentValueTopListLast.put(segmentName,
                  (tmpSegmentValueMaxListMin == null) ? value
                      : lastForComputingSegment(tmpSegmentValueMaxListMin,
                          value));
              if (segmentValueTopList.size() == maximumNumber) {
                tmpSegmentValueMaxListMin = segmentValueTopListLast
                    .get(segmentName);
                segmentValueTopListLast.put(segmentName,
                    tmpSegmentValueMaxListMin);
                segmentValuesBoundary.put(segmentName,
                    boundaryForSegmentComputing(segmentName));
              }
            }
            return segmentKeys.contains(key) ? SEGMENT_KEY : SEGMENT_NEW;
          } else if (compareWithBoundary(value, tmpSegmentValueBoundary)) {
            // System.out.println(key+" "+value+" "+tmpSegmentValueBoundary);
            if (!test) {
              segmentKeyValueList.get(segmentName).put(key, value);
              if (compareWithBoundary(value, tmpSegmentValueMaxListMin)) {
                segmentValueTopList.add(value);
                segmentValueTopList.remove(tmpSegmentValueMaxListMin);
                tmpSegmentValueMaxListMin = lastForComputingSegment();
                segmentValueTopListLast.put(segmentName,
                    tmpSegmentValueMaxListMin);
                segmentValuesBoundary.put(segmentName,
                    boundaryForSegmentComputing(segmentName));
              }
            }
            return segmentKeys.contains(key) ? SEGMENT_KEY : SEGMENT_NEW;
          } else if (segmentKeys.contains(key)) {
            if (!test) {
              segmentKeyValueList.get(segmentName).put(key, value);
            }
            return SEGMENT_KEY;
          } else {
            return null;
          }
        } else {
          return null;
        }
      } else {
        return null;
      }
    } else {
      throw new IOException("already closed");
    }
  }

  protected final void setError(int newPosition, int errorNumberItem,
      HashMap<String, Integer> errorListItem, boolean currentExisting)
      throws IOException {
    if (!closed) {
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
    } else {
      throw new IOException("already closed");
    }
  }

  private boolean sortedAndUnique(String[] keyList, int size)
      throws IOException {
    if (!closed) {
      for (int i = 1; i < size; i++) {
        if (keyList[(i - 1)].compareTo(keyList[i]) >= 0) {
          return false;
        }
      }
      return true;
    } else {
      throw new IOException("already closed");
    }
  }

  private int[][] computeSortAndUniqueMapping(String[] keyList, int size)
      throws IOException {
    if (!closed) {
      if (size > 0) {
        SortedMap<String, int[]> sortedMap = new TreeMap<>();
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
    } else {
      throw new IOException("already closed");
    }
  }

  protected void remapData(int[][] mapping) throws IOException {
    if (!closed) {
      // remap and merge keys
      String[] newKeyList = new String[mapping.length];
      // process mapping for functions?
      HashMap<MtasDataCollector<?, ?>, MtasDataCollector<?, ?>> map = new HashMap<>();
      int[] newSourceNumberList = new int[mapping.length];
      int[] newErrorNumber = new int[mapping.length];
      @SuppressWarnings("unchecked")
      HashMap<String, Integer>[] newErrorList = (HashMap<String, Integer>[]) new HashMap<?, ?>[mapping.length];
      for (int i = 0; i < mapping.length; i++) {
        newKeyList[i] = keyList[mapping[i][0]];
        newSourceNumberList[i] = sourceNumberList[mapping[i][0]];
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
                  .merge(subCollectorListNextLevel[mapping[i][j]], map, false);
            }
          }
        }
        subCollectorListNextLevel = newSubCollectorListNextLevel;
      }
      keyList = newKeyList;
      sourceNumberList = newSourceNumberList;
      errorNumber = newErrorNumber;
      errorList = newErrorList;
      size = keyList.length;
      position = 0;
    } else {
      throw new IOException("already closed");
    }
  }

  public void closeNewList() throws IOException {
    if (!closed) {
      if (segmentRegistration != null) {
        this.segmentName = null;
      }
      if (newSize > 0) {
        // add remaining old
        while (position < getSize()) {
          if (newPosition == newSize) {
            increaseNewListSize();
          }
          newKeyList[newPosition] = keyList[position];
          newSourceNumberList[newPosition] = sourceNumberList[position];
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
        sourceNumberList = newSourceNumberList;
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
  }

  abstract protected MtasDataItem<T1, T2> getItem(int i);

  protected boolean hasSub() {
    return hasSub;
  }

  public abstract void error(String error) throws IOException;

  public abstract void error(String key, String error) throws IOException;

  public abstract MtasDataCollector add(long valueSum, long valueN)
      throws IOException;

  public abstract MtasDataCollector add(long[] values, int number)
      throws IOException;

  public abstract MtasDataCollector add(double valueSum, long valueN)
      throws IOException;

  public abstract MtasDataCollector add(double[] values, int number)
      throws IOException;

  public abstract MtasDataCollector add(String key, long valueSum, long valueN)
      throws IOException;

  public abstract MtasDataCollector add(String key, long[] values, int number)
      throws IOException;

  public abstract MtasDataCollector add(String key, double valueSum,
      long valueN) throws IOException;

  public abstract MtasDataCollector add(String key, double[] values, int number)
      throws IOException;

  @Override
  public String toString() {
    StringBuilder text = new StringBuilder();
    text.append(this.getClass().getSimpleName() + "-" + this.hashCode() + "\n");
    text.append("\t=== " + collectorType + " - " + statsType + " " + statsItems
        + " " + hasSub + " ===\n");
    text.append("\tclosed: " + closed + "\n");
    text.append("\tkeylist: " + Arrays.asList(keyList) + "\n");
    text.append("\tsegmentKeys: "
        + (segmentKeys != null ? segmentKeys.contains("1") : "null") + "\n");
    return text.toString().trim();
  }

  public MtasDataCollectorResult<T1, T2> getResult() throws IOException {
    if (!closed) {
      close();
    }
    return result;
  }

  public Set<String> getKeyList() throws IOException {
    if (!closed) {
      close();
    }
    return new HashSet<>(Arrays.asList(keyList));
  }

  public SortedSet<String> getStatsItems() {
    return statsItems;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void close() throws IOException {
    if (!closed) {
      closeNewList();
      if (collectorType.equals(DataCollector.COLLECTOR_TYPE_LIST)) {
        // compute initial basic list
        TreeMap<String, MtasDataItem<T1, T2>> basicList = new TreeMap<>();
        for (int i = 0; i < getSize(); i++) {
          MtasDataItem<T1, T2> newItem = getItem(i);
          if (basicList.containsKey(keyList[i])) {
            newItem.add(basicList.get(keyList[i]));
          }
          basicList.put(keyList[i], newItem);
        }
        // create result based on basic list
        result = new MtasDataCollectorResult<>(collectorType, sortType,
            sortDirection, basicList, start, number);
        // reduce
        if (segmentRegistration != null) {
          if (segmentRegistration.equals(SEGMENT_SORT_ASC)
              || segmentRegistration.equals(SEGMENT_SORT_DESC)) {
            reduceToKeys(result.getComparatorList().keySet());
          } else if (segmentRegistration.equals(SEGMENT_BOUNDARY_ASC)
              || segmentRegistration.equals(SEGMENT_BOUNDARY_DESC)) {
            Map<String, MtasDataItemNumberComparator> comparatorList = result
                .getComparatorList();
            HashSet<String> filteredKeySet = new HashSet<>();
            if (segmentRegistration.equals(SEGMENT_BOUNDARY_ASC)) {
              for (Entry<String, MtasDataItemNumberComparator> entry : comparatorList
                  .entrySet()) {
                if (entry.getValue().compareTo(segmentValueBoundary) < 0) {
                  filteredKeySet.add(entry.getKey());
                }
              }
            } else {
              for (Entry<String, MtasDataItemNumberComparator> entry : comparatorList
                  .entrySet()) {
                if (entry.getValue().compareTo(segmentValueBoundary) > 0) {
                  filteredKeySet.add(entry.getKey());
                }
              }
            }
            reduceToKeys(filteredKeySet);
            basicList.keySet().retainAll(filteredKeySet);
            result = new MtasDataCollectorResult<>(collectorType, sortType,
                sortDirection, basicList, start, number);
          }
        }
      } else if (collectorType.equals(DataCollector.COLLECTOR_TYPE_DATA)) {
        if (getSize() > 0) {
          result = new MtasDataCollectorResult<>(collectorType, getItem(0));
        } else {
          result = new MtasDataCollectorResult<>(collectorType, sortType,
              sortDirection);
        }
      } else {
        throw new IOException("type " + collectorType + " not supported");
      }
      closed = true;
    }
  }

  public String getCollectorType() {
    return collectorType;
  }

  public String getStatsType() {
    return statsType;
  }

  public String getDataType() {
    return dataType;
  }

  public int getSize() {
    return size;
  }

  public boolean withTotal() {
    return withTotal;
  }

  public void setWithTotal() throws IOException {
    if (collectorType.equals(DataCollector.COLLECTOR_TYPE_LIST)) {
      if (segmentName != null) {
        throw new IOException("can't get total with segmentRegistration");
      } else {
        withTotal = true;
      }
    } else {
      throw new IOException(
          "can't get total for dataCollector of type " + collectorType);
    }
  }
}