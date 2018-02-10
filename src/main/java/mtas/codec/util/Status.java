package mtas.codec.util;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Status {
  
  public final static String TYPE_SEGMENT = "segment";

  public final static String KEY_NUMBER = "number";
  public final static String KEY_NUMBEROFDOCUMENTS = "numberOfDocuments";
  
  public volatile Integer numberSegmentsFinished = null;
  public volatile Integer numberSegmentsTotal = null;
  
  public volatile Integer subNumberSegmentsTotal = null;
  public volatile Integer subNumberSegmentsFinishedTotal = null;
  public volatile Map<String,Integer> subNumberSegmentsFinished = new ConcurrentHashMap<>();
  
  public volatile Long numberDocumentsFound = null;
  public volatile Long numberDocumentsFinished = null;
  public volatile Long numberDocumentsTotal = null;
  
  public volatile Long subNumberDocumentsTotal = null;
  public volatile Long subNumberDocumentsFinishedTotal = null;
  public volatile Map<String,Long> subNumberDocumentsFinished = new ConcurrentHashMap<>();
  
  public void init(long numberOfDocuments, int numberOfSegments) throws IOException {
    if(numberDocumentsTotal==null) {
      numberDocumentsTotal = numberOfDocuments;
    } else if(numberDocumentsTotal!=numberOfDocuments) {
      throw new IOException("conflict number of documents: "+numberDocumentsTotal+" / "+numberOfDocuments);
    }
    if(numberSegmentsTotal==null) {
      numberSegmentsTotal = numberOfSegments;
    } else if(numberSegmentsTotal!=numberOfSegments) {
      throw new IOException("conflict number of segments: "+numberSegmentsTotal+" / "+numberOfSegments);
    }
    numberDocumentsFinished = (numberDocumentsFinished==null) ? Long.valueOf(0) : numberDocumentsFinished;
    numberSegmentsFinished = (numberSegmentsFinished==null) ? 0 : numberSegmentsFinished;
    subNumberDocumentsTotal = numberDocumentsTotal * subNumberDocumentsFinished.size();
    subNumberDocumentsFinishedTotal = (subNumberDocumentsFinishedTotal==null) ? Long.valueOf(0) : subNumberDocumentsFinishedTotal;
    subNumberSegmentsTotal = numberOfSegments * subNumberSegmentsFinished.size();
    subNumberSegmentsFinishedTotal = (subNumberSegmentsFinishedTotal ==null) ? 0 : subNumberSegmentsFinishedTotal;    
  }
  
  public void addSubs(Set<String> subItems) {
    for(String subItem : subItems) {
      addSub(subItem);
    }
  }
  
  public void addSub(String subItem) {
    if(!subNumberSegmentsFinished.containsKey(subItem)) {
      subNumberSegmentsFinished.put(subItem, 0);
      if(numberSegmentsTotal!=null) {
        subNumberSegmentsTotal += numberSegmentsTotal;
      }
    }
    if(!subNumberDocumentsFinished.containsKey(subItem)) {
      subNumberDocumentsFinished.put(subItem, Long.valueOf(0));
      if(numberDocumentsTotal!=null) {
        subNumberDocumentsTotal += numberDocumentsTotal;
      }
    }
  }
  
}
