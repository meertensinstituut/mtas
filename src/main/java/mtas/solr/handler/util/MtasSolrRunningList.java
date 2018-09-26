package mtas.solr.handler.util;

import java.util.ArrayList;
import java.util.List;

public class MtasSolrRunningList extends MtasSolrBaseList {
  private Integer timeout;
  private Integer garbageTimeout;
  private static final Integer GARBAGE_FACTOR = 1000;

  public MtasSolrRunningList() {
    super();
    timeout = null;
    garbageTimeout = null;
  }

  public MtasSolrRunningList(Integer timeout) {
    super();
    this.timeout = timeout;
    garbageTimeout = GARBAGE_FACTOR * timeout;
  }

  public final void garbageCollect() {
    if (timeout != null && !data.isEmpty()) {
      long boundaryTime = System.currentTimeMillis() - (garbageTimeout);
      data.removeIf((MtasSolrStatus solrStatus) -> solrStatus.finished() || solrStatus.getStartTime() < boundaryTime);
      index.clear();
      data.forEach((MtasSolrStatus solrStatus) -> index.put(solrStatus.key(), solrStatus));
    }
  }

  public final List<MtasSolrStatus> checkForExceptions() {
    List<MtasSolrStatus> statusWithException = null;
    for (MtasSolrStatus item : data) {
      if (item.checkResponseForException()) {
        if (statusWithException == null) {
          statusWithException = new ArrayList<>();
        }
        statusWithException.add(item);
      }
    }
    return statusWithException;
  }
}
