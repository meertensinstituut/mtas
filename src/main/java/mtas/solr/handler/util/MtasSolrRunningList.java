package mtas.solr.handler.util;

/**
 * The Class MtasSolrList.
 */
public class MtasSolrRunningList extends MtasSolrBaseList {

  /** The timeout. */
  private Integer timeout;
  
  /** The garbage timeout. */
  private Integer garbageTimeout;

  /** The Constant GARBAGE_FACTOR. */
  private final static Integer GARBAGE_FACTOR = 1000;

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
   * @param timeout the timeout
   */
  public MtasSolrRunningList(Integer timeout) {
    super();
    this.timeout = timeout;
    garbageTimeout = GARBAGE_FACTOR * timeout;
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.util.MtasSolrBaseList#garbageCollect()
   */
  public final void garbageCollect() {
    if (timeout != null && !list.isEmpty()) {
      long boundaryTime = System.currentTimeMillis() - (garbageTimeout);
      list.removeIf((MtasSolrStatus solrStatus) -> solrStatus.finished() || solrStatus
          .getStartTime() < boundaryTime);
      index.clear();
      list.forEach((MtasSolrStatus solrStatus) -> index.put(solrStatus.key(),
          solrStatus));
    }
  }

}
