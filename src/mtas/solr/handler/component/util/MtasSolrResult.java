package mtas.solr.handler.component.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import mtas.codec.util.DataCollector;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.codec.util.collector.MtasDataCollectorResult;
import mtas.codec.util.collector.MtasDataItem;

/**
 * The Class MtasSolrResult.
 */
public class MtasSolrResult implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The stats type. */
  public String dataType, statsType;

  /** The stats items. */
  public TreeSet<String> statsItems;

  /** The sort direction. */
  public String sortType, sortDirection;

  /** The number. */
  public Integer start, number;

  /** The data collector. */
  public MtasDataCollector<?, ?> dataCollector = null;

  /** The function data. */
  public HashMap<MtasDataCollector<?, ?>, HashMap<String, MtasSolrResult>> functionData;

  /** The sub stats type. */
  private String[] subDataType, subStatsType;

  /** The sub stats items. */
  private TreeSet<String>[] subStatsItems;

  /** The sub sort direction. */
  private String[] subSortType, subSortDirection;

  /** The sub number. */
  private Integer[] subStart, subNumber;

  /**
   * Instantiates a new mtas solr result.
   *
   * @param dataCollector
   *          the data collector
   * @param dataType
   *          the data type
   * @param statsType
   *          the stats type
   * @param statsItems
   *          the stats items
   * @param sortType
   *          the sort type
   * @param sortDirection
   *          the sort direction
   * @param start
   *          the start
   * @param number
   *          the number
   * @param functionData
   *          the function data
   */
  @SuppressWarnings("unchecked")
  public MtasSolrResult(MtasDataCollector<?, ?> dataCollector,
      String[] dataType, String[] statsType, TreeSet<String>[] statsItems,
      String[] sortType, String[] sortDirection, Integer[] start,
      Integer[] number,
      HashMap<MtasDataCollector<?, ?>, HashMap<String, MtasSolrResult>> functionData) {
    this.dataCollector = dataCollector;
    this.functionData = functionData;
    this.dataType = (dataType == null) ? null : dataType[0];
    this.statsType = (statsType == null) ? null : statsType[0];
    this.statsItems = (statsItems == null) ? null : statsItems[0];
    this.sortType = (sortType == null) ? null : sortType[0];
    this.sortDirection = (sortDirection == null) ? null : sortDirection[0];
    this.start = (start == null) ? null : start[0];
    this.number = (number == null) ? null : number[0];
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
   * Instantiates a new mtas solr result.
   *
   * @param dataCollector
   *          the data collector
   * @param dataType
   *          the data type
   * @param statsType
   *          the stats type
   * @param statsItems
   *          the stats items
   * @param functionData
   *          the function data
   */
  @SuppressWarnings("unchecked")
  public MtasSolrResult(MtasDataCollector<?, ?> dataCollector, String dataType,
      String statsType, TreeSet<String> statsItems,
      HashMap<MtasDataCollector<?, ?>, HashMap<String, MtasSolrResult>> functionData) {
    this(dataCollector, new String[] { dataType }, new String[] { statsType },
        new TreeSet[] { statsItems }, new String[] { null },
        new String[] { null }, new Integer[] { 0 }, new Integer[] { 1 },
        functionData);
  }

  /**
   * Merge.
   *
   * @param newItem
   *          the new item
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  void merge(MtasSolrResult newItem) throws IOException {
    HashMap<MtasDataCollector<?, ?>, MtasDataCollector<?, ?>> map = new HashMap<MtasDataCollector<?, ?>, MtasDataCollector<?, ?>>();
    dataCollector.merge(newItem.dataCollector, map, true);
    if (newItem.functionData != null) {
      if (functionData == null) {
        functionData = new HashMap<MtasDataCollector<?, ?>, HashMap<String, MtasSolrResult>>();
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
            HashMap<String, MtasSolrResult> tmpList = functionData
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
   * @param showDebugInfo
   *          the show debug info
   * @return the data
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  NamedList<Object> getData(boolean showDebugInfo) throws IOException {
    if (dataCollector.getCollectorType()
        .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
      NamedList<Object> mtasResponse = new SimpleOrderedMap<>();
      // functions
      Map<String, NamedList<Object>> functionList = new HashMap<String, NamedList<Object>>();
      if (functionData != null && functionData.containsKey(dataCollector)) {
        HashMap<String, MtasSolrResult> functionDataItem = functionData
            .get(dataCollector);
        for (String functionKey : functionDataItem.keySet()) {
          MtasSolrResult functionResult = functionDataItem.get(functionKey);
          if (functionResult.dataCollector.getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
            NamedList<Object> functionData = functionResult
                .getData(showDebugInfo);
            functionList.put(functionKey, functionData);
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
      }
      if ((subDataType != null) && (dataItem.getSub() != null)) {
        MtasSolrResult css = new MtasSolrResult(dataItem.getSub(), subDataType,
            subStatsType, subStatsItems, subSortType, subSortDirection,
            subStart, subNumber, functionData);
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
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @param showDebugInfo
   *          the show debug info
   * @return the named list
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  NamedList<Object> getNamedList(boolean showDebugInfo) throws IOException {
    if (dataCollector.getCollectorType()
        .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
      SimpleOrderedMap<Object> mtasResponseList = new SimpleOrderedMap<>();
      // functions
      Map<String, SimpleOrderedMap<Object>> functionList = new HashMap<String, SimpleOrderedMap<Object>>();
      if (functionData != null && functionData.containsKey(dataCollector)) {
        HashMap<String, MtasSolrResult> functionDataItem = functionData
            .get(dataCollector);
        for (String functionKey : functionDataItem.keySet()) {
          MtasSolrResult functionResult = functionDataItem.get(functionKey);
          if (functionResult.dataCollector.getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
            NamedList<Object> functionNamedList = functionResult
                .getNamedList(showDebugInfo);
            for (int i = 0; i < functionNamedList.size(); i++) {
              if (functionList.containsKey(functionNamedList.getName(i))) {
                SimpleOrderedMap<Object> tmpMap = functionList
                    .get(functionNamedList.getName(i));
                tmpMap.add(functionKey, functionNamedList.getVal(i));
              } else {
                SimpleOrderedMap<Object> tmpMap = new SimpleOrderedMap<>();
                tmpMap.add(functionKey, functionNamedList.getVal(i));
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
          MtasSolrResult css = new MtasSolrResult(dataItem.getSub(),
              subDataType, subStatsType, subStatsItems, subSortType,
              subSortDirection, subStart, subNumber, functionData);
          if (dataItem.getSub().getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
            mtasResponseListItem.add(dataItem.getSub().getCollectorType(),
                css.getNamedList(showDebugInfo));
          } else if (dataItem.getSub().getCollectorType()
              .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
            mtasResponseListItem.add(dataItem.getSub().getCollectorType(),
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
      return "MtasSolrResult(data-" + hashCode() + ")";
    }
    if (dataCollector.getCollectorType()
        .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
      return "MtasSolrResult(list(" + dataCollector.getSize() + ")-"
          + hashCode() + ")";
    } else {
      return "MtasSolrResult: unknown";
    }
  }

  /**
   * Gets the result.
   *
   * @return the result
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public MtasDataCollectorResult<?, ?> getResult() throws IOException {
    return dataCollector.getResult();
  }

}
