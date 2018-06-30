package mtas.solr.handler.util;

import java.io.IOException;

import org.apache.solr.common.util.SimpleOrderedMap;

// TODO: Auto-generated Javadoc
/**
 * The Class MtasSolrHistoryList.
 */
public class MtasSolrHistoryList extends MtasSolrBaseList {

  /** The soft limit. */
  private int softLimit;
  
  /** The hard limit. */
  private int hardLimit;

  /** The Constant NAME_SOFTLIMIT. */
  private static final String NAME_SOFTLIMIT = "softLimit";
  
  /** The Constant NAME_HARDLIMIT. */
  private static final String NAME_HARDLIMIT = "hardLimit";

  /**
   * Instantiates a new mtas solr history list.
   */
  public MtasSolrHistoryList() {
    super();
    softLimit = 0;
    hardLimit = 0;
  }

  /**
   * Instantiates a new mtas solr history list.
   *
   * @param softLimit the soft limit
   * @param hardLimit the hard limit
   */
  public MtasSolrHistoryList(int softLimit, int hardLimit) {
    this();
    setLimits(softLimit, hardLimit);
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.util.MtasSolrBaseList#add(mtas.solr.handler.util.MtasSolrStatus)
   */
  @Override
  public void add(MtasSolrStatus status) throws IOException {
    if (softLimit > 0) {
      super.add(status);
    }
  }

  /**
   * Sets the limits.
   *
   * @param softLimit the soft limit
   * @param hardLimit the hard limit
   */
  public void setLimits(int softLimit, int hardLimit) {
    if ((softLimit > 0 && hardLimit > softLimit) || (softLimit == 0 && hardLimit == 0)) {
      this.softLimit = softLimit;
      this.hardLimit = hardLimit;
      garbageCollect();
    } else {
      throw new IllegalArgumentException();
    }
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.util.MtasSolrBaseList#garbageCollect()
   */
  public void garbageCollect() {
    if (softLimit == 0) {
      reset();
    } else if (data.size() > hardLimit) {
      long boundaryTime = data.get(softLimit).getStartTime();
      data.removeIf((MtasSolrStatus status) -> status.getStartTime() < boundaryTime);
      index.clear();
      data.forEach((MtasSolrStatus status) -> index.put(status.key(), status));
    }
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.util.MtasSolrBaseList#createListOutput(boolean, int)
   */
  @Override
  public SimpleOrderedMap<Object> createListOutput(boolean shardRequests, int number) {
    SimpleOrderedMap<Object> output = super.createListOutput(shardRequests, number);
    output.add(NAME_SOFTLIMIT, softLimit);
    output.add(NAME_HARDLIMIT, hardLimit);
    return output;
  }

}
