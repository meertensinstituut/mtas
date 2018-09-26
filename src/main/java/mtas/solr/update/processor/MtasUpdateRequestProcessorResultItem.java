package mtas.solr.update.processor;

import org.apache.lucene.util.BytesRef;

import java.io.Serializable;

public class MtasUpdateRequestProcessorResultItem implements Serializable {
  private static final long serialVersionUID = 1L;

  public String tokenTerm;
  public Integer tokenOffsetStart;
  public Integer tokenOffsetEnd;
  public Integer tokenPosIncr;
  public byte[] tokenPayload;
  public Integer tokenFlags;

  public MtasUpdateRequestProcessorResultItem(String term, Integer offsetStart,
      Integer offsetEnd, Integer posIncr, BytesRef payload, Integer flags) {
    tokenTerm = term;
    if (offsetStart != null && offsetEnd != null) {
      tokenOffsetStart = offsetStart;
      tokenOffsetEnd = offsetEnd;
    } else {
      tokenOffsetStart = null;
      tokenOffsetEnd = null;
    }
    if (posIncr != null && posIncr != 1) {
      tokenPosIncr = posIncr;
    } else {
      tokenPosIncr = null;
    }
    if (payload != null) {
      tokenPayload = payload.bytes;
    } else {
      tokenPayload = null;
    }
    tokenFlags = flags;
  }
}