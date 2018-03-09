package mtas.solr.handler.util;

import java.io.IOException;

import org.apache.solr.common.util.SimpleOrderedMap;

public class MtasSolrHistoryList extends MtasSolrBaseList {

  private int softLimit;
  private int hardLimit;
  
  private final static String NAME_SOFTLIMIT = "softLimit";
  private final static String NAME_HARDLIMIT = "hardLimit";
  
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
    if(softLimit>0) {
      super.add(status);
    }
  }
  
  public void setLimits(int softLimit, int hardLimit) {
    if ((softLimit > 0 && hardLimit > softLimit) || (softLimit==0 && hardLimit==0)) {
      this.softLimit = softLimit;
      this.hardLimit = hardLimit;
      garbageCollect();
    } else {
      throw new IllegalArgumentException();
    }  
  }

  public void garbageCollect() {
    if(softLimit==0) {
      reset();
    } else if (list.size() > hardLimit) {
      long boundaryTime = list.get(softLimit).getStartTime();
      list.removeIf(
          (MtasSolrStatus status) -> status.getStartTime() < boundaryTime);
      index.clear();
      list.forEach((MtasSolrStatus status) -> index.put(status.key(), status));
    }
  }
    
  @Override
  public SimpleOrderedMap<Object> createListOutput(boolean shardRequests) {
    SimpleOrderedMap<Object> output = super.createListOutput(shardRequests);
    output.add(NAME_SOFTLIMIT, softLimit);
    output.add(NAME_HARDLIMIT, hardLimit);
    return output;
  }

}
