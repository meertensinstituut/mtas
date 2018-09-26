package mtas.solr.handler.util;

import org.apache.solr.common.util.SimpleOrderedMap;

import java.io.IOException;

public class MtasSolrHistoryList extends MtasSolrBaseList {
  private int softLimit;
  private int hardLimit;
  private static final String NAME_SOFTLIMIT = "softLimit";
  private static final String NAME_HARDLIMIT = "hardLimit";

  public MtasSolrHistoryList() {
    super();
    softLimit = 0;
    hardLimit = 0;
  }

  public MtasSolrHistoryList(int softLimit, int hardLimit) {
    this();
    setLimits(softLimit, hardLimit);
  }

  @Override
  public void add(MtasSolrStatus status) throws IOException {
    if (softLimit > 0) {
      super.add(status);
    }
  }

  public void setLimits(int softLimit, int hardLimit) {
    if ((softLimit > 0 && hardLimit > softLimit) || (softLimit == 0 && hardLimit == 0)) {
      this.softLimit = softLimit;
      this.hardLimit = hardLimit;
      garbageCollect();
    } else {
      throw new IllegalArgumentException();
    }
  }

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

  @Override
  public SimpleOrderedMap<Object> createListOutput(boolean shardRequests, int number) {
    SimpleOrderedMap<Object> output = super.createListOutput(shardRequests, number);
    output.add(NAME_SOFTLIMIT, softLimit);
    output.add(NAME_HARDLIMIT, hardLimit);
    return output;
  }
}
