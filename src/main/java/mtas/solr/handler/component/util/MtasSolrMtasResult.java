package mtas.solr.handler.component.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import mtas.codec.util.DataCollector;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.codec.util.collector.MtasDataCollectorResult;
import mtas.codec.util.collector.MtasDataItem;

/**
 * The Class MtasSolrMtasResult.
 */
public class MtasSolrMtasResult implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The data type. */
  public String dataType;

  /** The stats type. */
  public String statsType;

  /** The stats items. */
  public SortedSet<String> statsItems;

  /** The sort type. */
  public String sortType;

  /** The sort direction. */
  public String sortDirection;

  /** The start. */
  public Integer start;

  /** The number. */
  public Integer number;

  /** The data collector. */
  public MtasDataCollector<?, ?> dataCollector = null;

  /** The function data. */
  public Map<MtasDataCollector<?, ?>, HashMap<String, MtasSolrMtasResult>> functionData;

  /** The sub data type. */
  private String[] subDataType;

  /** The sub stats type. */
  private String[] subStatsType;

  /** The sub stats items. */
  private SortedSet<String>[] subStatsItems;

  /** The sub sort type. */
  private String[] subSortType;

  /** The sub sort direction. */
  private String[] subSortDirection;

  /** The sub start. */
  private Integer[] subStart;

  /** The sub number. */
  private Integer[] subNumber;

  /**
   * Instantiates a new mtas solr mtas result.
   *
   * @param dataCollector the data collector
   * @param dataType the data type
   * @param statsType the stats type
   * @param statsItems the stats items
   * @param sortType the sort type
   * @param sortDirection the sort direction
   * @param start the start
   * @param number the number
   * @param functionData the function data
   */
  @SuppressWarnings("unchecked")
  public MtasSolrMtasResult(MtasDataCollector<?, ?> dataCollector,
      String[] dataType, String[] statsType, SortedSet<String>[] statsItems,
      String[] sortType, String[] sortDirection, Integer[] start,
      Integer[] number,
      Map<MtasDataCollector<?, ?>, HashMap<String, MtasSolrMtasResult>> functionData) {
    this.dataCollector = dataCollector;
    this.functionData = functionData;
    this.dataType = (dataType == null) ? null : dataType[0];
    this.statsType = (statsType == null) ? null : statsType[0];
    this.statsItems = (statsItems == null) ? null : statsItems[0];
    this.sortType = (sortType == null) ? null : sortType[0];
    this.sortDirection = (sortDirection == null) ? null : sortDirection[0];
    this.start = (start == null) ? null : start[0];
    this.number = (number == null) ? null : number[0];
    this.subStart = null;
    this.subNumber = null;
    if ((dataType != null) && (dataType.length > 1)) {
      subDataType = new String[dataType.length - 1];
      subStatsType = new String[dataType.length - 1];
      subStatsItems = new TreeSet[dataType.length - 1];
      subSortType = new String[dataType.length - 1];
      subSortDirection = new String[dataType.length - 1];
      System.arraycopy(dataType, 1, subDataType, 0, dataType.length - 1);
      System.arraycopy(statsType, 1, subStatsType, 0, dataType.length - 1);
      System.arraycopy(statsItems, 1, subStatsItems, 0, dataType.length - 1);
      System.arraycopy(sortType, 1, subSortType, 0, dataType.length - 1);
      System.arraycopy(sortDirection, 1, subSortDirection, 0,
          dataType.length - 1);
    } else {
      subDataType = null;
      subStatsType = null;
      subStatsItems = null;
      subSortType = null;
      subSortDirection = null;
    }
  }

  /**
   * Instantiates a new mtas solr mtas result.
   *
   * @param dataCollector the data collector
   * @param dataType the data type
   * @param statsType the stats type
   * @param statsItems the stats items
   * @param functionData the function data
   */
  @SuppressWarnings("unchecked")
  public MtasSolrMtasResult(MtasDataCollector<?, ?> dataCollector,
      String dataType, String statsType, SortedSet<String> statsItems,
      Map<MtasDataCollector<?, ?>, HashMap<String, MtasSolrMtasResult>> functionData) {
    this(dataCollector, new String[] { dataType }, new String[] { statsType },
        new SortedSet[] { statsItems }, new String[] { null },
        new String[] { null }, new Integer[] { 0 }, new Integer[] { 1 },
        functionData);
  }

  /**
   * Merge.
   *
   * @param newItem the new item
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void merge(MtasSolrMtasResult newItem) throws IOException {
    HashMap<MtasDataCollector<?, ?>, MtasDataCollector<?, ?>> map = new HashMap<>();
    if (newItem.dataCollector.withTotal()) {
      dataCollector.setWithTotal();
    }
    dataCollector.merge(newItem.dataCollector, map, true);
    if (newItem.functionData != null) {
      if (functionData == null) {
        functionData = new HashMap<>();
      }
      for (MtasDataCollector<?, ?> keyCollector : newItem.functionData
          .keySet()) {
        if (map.containsKey(keyCollector)) {
          // compute mapped key
          MtasDataCollector<?, ?> newKeyCollector = keyCollector;
          while (map.containsKey(newKeyCollector)) {
            newKeyCollector = map.get(keyCollector);
          }
          if (functionData.containsKey(newKeyCollector)) {
            HashMap<String, MtasSolrMtasResult> tmpList = functionData
                .get(newKeyCollector);
            for (String functionKey : newItem.functionData.get(keyCollector)
                .keySet()) {
              if (tmpList.containsKey(functionKey)) {
                tmpList.get(functionKey).merge(
                    newItem.functionData.get(keyCollector).get(functionKey));
              } else {
                tmpList.put(functionKey,
                    newItem.functionData.get(keyCollector).get(functionKey));
              }
            }
          } else {
            functionData.put(newKeyCollector,
                newItem.functionData.get(keyCollector));
          }
        }
      }
    }
  }

  /**
   * Gets the data.
   *
   * @param showDebugInfo the show debug info
   * @return the data
   * @throws IOException Signals that an I/O exception has occurred.
   */
  NamedList<Object> getData(boolean showDebugInfo) throws IOException {
    if (dataCollector.getCollectorType()
        .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
      NamedList<Object> mtasResponse = new SimpleOrderedMap<>();
      // functions
      Map<String, NamedList<Object>> functionList = new HashMap<>();
      if (functionData != null && functionData.containsKey(dataCollector)) {
        HashMap<String, MtasSolrMtasResult> functionDataItem = functionData
            .get(dataCollector);
        for (Entry<String, MtasSolrMtasResult> entry : functionDataItem
            .entrySet()) {
          MtasSolrMtasResult functionResult = entry.getValue();
          if (functionResult.dataCollector.getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
            NamedList<Object> functionData = functionResult
                .getData(showDebugInfo);
            functionList.put(entry.getKey(), functionData);
          } else {
            throw new IOException("unexpected function collectorType "
                + functionResult.dataCollector.getCollectorType());
          }
        }
      }
      // main result
      MtasDataItem<?, ?> dataItem = dataCollector.getResult().getData();
      if (dataItem != null) {
        mtasResponse.addAll(dataItem.rewrite(showDebugInfo));
        if (functionList.size() > 0) {
          mtasResponse.add("functions", functionList);
        }
        if ((subDataType != null) && (dataItem.getSub() != null)) {
          MtasSolrMtasResult css = new MtasSolrMtasResult(dataItem.getSub(),
              subDataType, subStatsType, subStatsItems, subSortType,
              subSortDirection, subStart, subNumber, functionData);
          if (dataItem.getSub().getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
            mtasResponse.add(dataItem.getSub().getCollectorType(),
                css.getNamedList(showDebugInfo));
          } else if (dataItem.getSub().getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
            mtasResponse.add(dataItem.getSub().getCollectorType(),
                css.getData(showDebugInfo));
          }
        }
      }
      return mtasResponse;
    } else {
      throw new IOException(
          "only allowed for " + DataCollector.COLLECTOR_TYPE_DATA);
    }
  }

  /**
   * Gets the key list.
   *
   * @return the key list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public Set<String> getKeyList() throws IOException {
    if (dataCollector.getCollectorType()
        .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
      return dataCollector.getResult().getComparatorList().keySet();
    } else {
      throw new IOException(
          "only allowed for " + DataCollector.COLLECTOR_TYPE_LIST);
    }
  }

  /**
   * Gets the full key list.
   *
   * @return the full key list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public Set<String> getFullKeyList() throws IOException {
    if (dataCollector.getCollectorType()
        .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
      return dataCollector.getKeyList();
    } else {
      throw new IOException(
          "only allowed for " + DataCollector.COLLECTOR_TYPE_LIST);
    }
  }

  /**
   * Gets the named list.
   *
   * @param showDebugInfo the show debug info
   * @return the named list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  NamedList<Object> getNamedList(boolean showDebugInfo) throws IOException {
    if (dataCollector.getCollectorType()
        .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
      SimpleOrderedMap<Object> mtasResponseList = new SimpleOrderedMap<>();
      // functions
      Map<String, SimpleOrderedMap<Object>> functionList = new HashMap<>();
      if (functionData != null && functionData.containsKey(dataCollector)) {
        HashMap<String, MtasSolrMtasResult> functionDataItem = functionData
            .get(dataCollector);
        for (Entry<String, MtasSolrMtasResult> entry : functionDataItem
            .entrySet()) {
          MtasSolrMtasResult functionResult = entry.getValue();
          if (functionResult.dataCollector.getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
            NamedList<Object> functionNamedList = functionResult
                .getNamedList(showDebugInfo);
            for (int i = 0; i < functionNamedList.size(); i++) {
              if (functionList.containsKey(functionNamedList.getName(i))) {
                SimpleOrderedMap<Object> tmpMap = functionList
                    .get(functionNamedList.getName(i));
                tmpMap.add(entry.getKey(), functionNamedList.getVal(i));
              } else {
                SimpleOrderedMap<Object> tmpMap = new SimpleOrderedMap<>();
                tmpMap.add(entry.getKey(), functionNamedList.getVal(i));
                functionList.put(functionNamedList.getName(i), tmpMap);
              }
            }
          } else {
            throw new IOException("unexpected function collectorType "
                + functionResult.dataCollector.getCollectorType());
          }
        }
      }
      // main result
      Map<String, ?> dataList = dataCollector.getResult().getList();
      for (Entry<String, ?> entry : dataList.entrySet()) {
        SimpleOrderedMap<Object> mtasResponseListItem = new SimpleOrderedMap<>();
        MtasDataItem<?, ?> dataItem = (MtasDataItem<?, ?>) entry.getValue();
        mtasResponseListItem.addAll(dataItem.rewrite(showDebugInfo));
        if (functionList.containsKey(entry.getKey())) {
          mtasResponseListItem.add("functions",
              functionList.get(entry.getKey()));
        }
        if ((subDataType != null) && (dataItem.getSub() != null)) {
          MtasSolrMtasResult css = new MtasSolrMtasResult(dataItem.getSub(),
              subDataType, subStatsType, subStatsItems, subSortType,
              subSortDirection, subStart, subNumber, functionData);
          if (dataItem.getSub().getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
            if (css.dataCollector.withTotal()) {
              mtasResponseListItem.add(
                  DataCollector.COLLECTOR_TYPE_LIST + "Total",
                  css.dataCollector.getSize());
            }
            mtasResponseListItem.add(DataCollector.COLLECTOR_TYPE_LIST,
                css.getNamedList(showDebugInfo));
          } else if (dataItem.getSub().getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
            mtasResponseListItem.add(DataCollector.COLLECTOR_TYPE_DATA,
                css.getData(showDebugInfo));
          }
        }
        mtasResponseList.add(entry.getKey(), mtasResponseListItem);
      }
      return mtasResponseList;
    } else {
      throw new IOException(
          "only allowed for " + DataCollector.COLLECTOR_TYPE_LIST);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    if (dataCollector.getCollectorType()
        .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
      return this.getClass().getSimpleName() + "(data-" + hashCode() + ")";
    }
    if (dataCollector.getCollectorType()
        .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
      return this.getClass().getSimpleName() + "(list("
          + dataCollector.getSize() + ")-" + hashCode() + ")";
    } else {
      return this.getClass().getSimpleName() + ": unknown";
    }
  }

  /**
   * Gets the result.
   *
   * @return the result
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasDataCollectorResult getResult() throws IOException {
    return dataCollector.getResult();
  }

}
