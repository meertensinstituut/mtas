package mtas.solr.update.processor;

import java.io.Serializable;

import org.apache.lucene.util.BytesRef;

public class MtasUpdateRequestProcessorResultItem implements Serializable {
  /** The token terms. */
  public String tokenTerm;
  
  /** The token offset starts. */
  public Integer tokenOffsetStart;
  
  /** The token offset ends. */
  public Integer tokenOffsetEnd;
  
  /** The token pos incrs. */
  public Integer tokenPosIncr;
  
  /** The token payloads. */
  public byte[] tokenPayload;
  
  /** The token flags. */
  public Integer tokenFlags;
  
  public MtasUpdateRequestProcessorResultItem(String term, Integer offsetStart, Integer offsetEnd, Integer posIncr, BytesRef payload, Integer flags) {
    tokenTerm = term;
    if(offsetStart!=null && offsetEnd!=null) {
      tokenOffsetStart = offsetStart;
      tokenOffsetEnd = offsetEnd;
    } else {
      tokenOffsetStart = null;
      tokenOffsetEnd = null;
    }
    if(posIncr!=null && posIncr!=1) {
      tokenPosIncr = posIncr;
    } else {
      tokenPosIncr = null;
    }
    if(payload!=null) {
      tokenPayload = payload.bytes;
    } else {
      tokenPayload = null;
    }
    tokenFlags = flags;
  }
  
}