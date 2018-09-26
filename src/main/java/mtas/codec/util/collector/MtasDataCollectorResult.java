package mtas.codec.util.collector;

import mtas.codec.util.CodecUtil;
import mtas.codec.util.DataCollector;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

public class MtasDataCollectorResult<T1 extends Number & Comparable<T1>, T2 extends Number & Comparable<T2>>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  private SortedMap<String, MtasDataItem<T1, T2>> list;
  private MtasDataItem<T1, T2> item;
  private String sortType;
  private String sortDirection;
  private String collectorType;
  private MtasDataItemNumberComparator lastSortValue;

  String startKey;
  String endKey;

  public MtasDataCollectorResult(String collectorType, String sortType,
      String sortDirection,
      NavigableMap<String, MtasDataItem<T1, T2>> basicList, Integer start,
      Integer number) throws IOException {
    this(collectorType, sortType, sortDirection);
    if (sortType == null || sortType.equals(CodecUtil.SORT_TERM)) {
      if (sortDirection == null || sortDirection.equals(CodecUtil.SORT_ASC)) {
        list = basicList;
      } else if (sortDirection.equals(CodecUtil.SORT_DESC)) {
        list = basicList.descendingMap();
      } else {
        throw new IOException("unknown sort direction " + sortDirection);
      }
    } else if (CodecUtil.isStatsType(sortType)) {
      // comperator
      Comparator<String> valueComparator = new Comparator<String>() {
        @Override
        public int compare(String k1, String k2) {
          int compare = basicList.get(k1).compareTo(basicList.get(k2));
          return compare == 0 ? k1.compareTo(k2) : compare;
        }
      };
      SortedMap<String, MtasDataItem<T1, T2>> sortedByValues = new TreeMap<>(
          valueComparator);
      sortedByValues.putAll(basicList);
      list = sortedByValues;
    } else {
      throw new IOException("unknown sort type " + sortType);
    }
    int listStart = start == null ? 0 : start;
    if (number == null || (start == 0 && number >= list.size())) {
      // do nothing, full list is ok      
    } else if (listStart < list.size() && number > 0) {
      // subset
      String boundaryEndKey = null;
      int counter = 0;
      MtasDataItem<T1, T2> previous = null;
      for (Entry<String, MtasDataItem<T1, T2>> entry : list.entrySet()) {
        if (listStart == counter) {
          startKey = entry.getKey();
        } else if (listStart + number <= counter) {
          if (sortType == null || sortType.equals(CodecUtil.SORT_TERM)) {
            endKey = entry.getKey();
            boundaryEndKey = entry.getKey();
            break;
          } else if (previous != null) {
            if (previous.compareTo(entry.getValue()) != 0) {
              //ready, previous not equal to this item
              break;
            } else {
              //register this as possible boundaryEndKey, but continue
              boundaryEndKey = entry.getKey();
            }
          } else {
            //possibly ready, but check next
            endKey = entry.getKey();
            boundaryEndKey = entry.getKey();
            previous = entry.getValue();
          }
        } 
        counter++;
      }            
      if(startKey!=null) {
        if(boundaryEndKey!=null) {
          list = list.subMap(startKey, boundaryEndKey);
        } else {
          list = list.tailMap(startKey);
        }
      } else {
        list = new TreeMap<>();
      }
    } else {
      list = new TreeMap<>();
    }
    if (list.size() > 0 && sortType != null) {
      lastSortValue = list.get(list.lastKey()).getComparableValue();
    }
  }

  public MtasDataCollectorResult(String collectorType,
      MtasDataItem<T1, T2> item) {
    this(collectorType, null, null);
    this.item = item;
  }

  public MtasDataCollectorResult(String collectorType, String sortType,
      String sortDirection) {
    list = null;
    item = null;
    lastSortValue = null;
    this.collectorType = collectorType;
    this.sortType = sortType;
    this.sortDirection = sortDirection;
  }

  public final SortedMap<String, MtasDataItem<T1, T2>> getList()
      throws IOException {
    return getList(true);
  }

  public final SortedMap<String, MtasDataItem<T1, T2>> getList(boolean reduce)
      throws IOException {
    if (collectorType.equals(DataCollector.COLLECTOR_TYPE_LIST)) {
      if (reduce && startKey != null && endKey != null) {
        return list.subMap(startKey, endKey);
      } else {
        return list;
      }
    } else {
      throw new IOException("type " + collectorType + " not supported");
    }
  }

  @SuppressWarnings("rawtypes")
  public final Map<String, MtasDataItemNumberComparator> getComparatorList()
      throws IOException {
    if (collectorType.equals(DataCollector.COLLECTOR_TYPE_LIST)) {
      LinkedHashMap<String, MtasDataItemNumberComparator> comparatorList = new LinkedHashMap<>();
      for (Entry<String, MtasDataItem<T1, T2>> entry : list.entrySet()) {
        comparatorList.put(entry.getKey(),
            entry.getValue().getComparableValue());
      }
      return comparatorList;
    } else {
      throw new IOException("type " + collectorType + " not supported");
    }
  }

  @SuppressWarnings("rawtypes")
  public final MtasDataItemNumberComparator getLastSortValue() {
    return lastSortValue;
  }

  public final MtasDataItem<T1, T2> getData() throws IOException {
    if (collectorType.equals(DataCollector.COLLECTOR_TYPE_DATA)) {
      return item;
    } else {
      throw new IOException("type " + collectorType + " not supported");
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "(");
    buffer.append(collectorType + "," + sortType + "," + sortDirection);
    buffer.append(")");
    return buffer.toString();
  }
}
