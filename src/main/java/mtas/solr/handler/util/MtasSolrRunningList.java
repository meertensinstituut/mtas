package mtas.solr.handler.util;

import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class MtasSolrList.
 */
public class MtasSolrRunningList extends MtasSolrBaseList {

  /** The timeout. */
  private Integer timeout;

  /** The garbage timeout. */
  private Integer garbageTimeout;

  /** The Constant GARBAGE_FACTOR. */
  private static final Integer GARBAGE_FACTOR = 1000;

  /**
   * Instantiates a new mtas solr list.
   */
  public MtasSolrRunningList() {
    super();
    timeout = null;
    garbageTimeout = null;
  }

  /**
   * Instantiates a new mtas solr list.
   *
   * @param timeout
   *          the timeout
   */
  public MtasSolrRunningList(Integer timeout) {
    super();
    this.timeout = timeout;
    garbageTimeout = GARBAGE_FACTOR * timeout;
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.solr.handler.util.MtasSolrBaseList#garbageCollect()
   */
  public final void garbageCollect() {
    if (timeout != null && !data.isEmpty()) {
      long boundaryTime = System.currentTimeMillis() - (garbageTimeout);
      data.removeIf((MtasSolrStatus solrStatus) -> solrStatus.finished() || solrStatus.getStartTime() < boundaryTime);
      index.clear();
      data.forEach((MtasSolrStatus solrStatus) -> index.put(solrStatus.key(), solrStatus));
    }
  }

  /**
   * Check for exceptions.
   *
   * @return the list
   */
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
