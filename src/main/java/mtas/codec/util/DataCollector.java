package mtas.codec.util;

import mtas.codec.util.collector.MtasDataCollector;
import mtas.codec.util.collector.MtasDataDoubleAdvanced;
import mtas.codec.util.collector.MtasDataDoubleBasic;
import mtas.codec.util.collector.MtasDataDoubleFull;
import mtas.codec.util.collector.MtasDataLongAdvanced;
import mtas.codec.util.collector.MtasDataLongBasic;
import mtas.codec.util.collector.MtasDataLongFull;

import java.io.IOException;
import java.util.SortedSet;

public class DataCollector {
  public static final String COLLECTOR_TYPE_LIST = "list";
  public static final String COLLECTOR_TYPE_DATA = "data";

  private DataCollector() {
  }

  public static MtasDataCollector<?, ?> getCollector(String collectorType,
      String dataType, String statsType, SortedSet<String> statsItems,
      String sortType, String sortDirection, Integer start, Integer number,
      String segmentRegistration, String boundary) throws IOException {
    return getCollector(collectorType, dataType, statsType, statsItems,
        sortType, sortDirection, start, number, null, null, null, null, null,
        null, null, null, segmentRegistration, boundary);
  }

  public static MtasDataCollector<?, ?> getCollector(String collectorType,
      String dataType, String statsType, SortedSet<String> statsItems,
      String sortType, String sortDirection, Integer start, Integer number,
      String[] subCollectorTypes, String[] subDataTypes, String[] subStatsTypes,
      SortedSet<String>[] subStatsItems, String[] subSortTypes,
      String[] subSortDirections, Integer[] subStart, Integer[] subNumber,
      String segmentRegistration, String boundary) throws IOException {
    if (dataType != null && dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
      switch (statsType) {
        case CodecUtil.STATS_BASIC:
          return new MtasDataLongBasic(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration, boundary);
        case CodecUtil.STATS_ADVANCED:
          return new MtasDataLongAdvanced(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration, boundary);
        case CodecUtil.STATS_FULL:
          return new MtasDataLongFull(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration, boundary);
        default:
          throw new IOException("unknown statsType " + statsType);
      }
    } else if (dataType != null && dataType.equals(CodecUtil.DATA_TYPE_DOUBLE)) {
      switch (statsType) {
        case CodecUtil.STATS_BASIC:
          return new MtasDataDoubleBasic(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration, boundary);
        case CodecUtil.STATS_ADVANCED:
          return new MtasDataDoubleAdvanced(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration, boundary);
        case CodecUtil.STATS_FULL:
          return new MtasDataDoubleFull(collectorType, statsItems, sortType,
            sortDirection, start, number, subCollectorTypes, subDataTypes,
            subStatsTypes, subStatsItems, subSortTypes, subSortDirections,
            subStart, subNumber, segmentRegistration, boundary);
        default:
          throw new IOException("unknown statsType " + statsType);
      }
    } else {
      throw new IOException("unknown dataType " + dataType);
    }
  }

}
