package mtas.solr.update.processor;

import java.io.Serializable;

import org.apache.lucene.util.BytesRef;

/**
 * The Class MtasUpdateRequestProcessorResultItem.
 */
public class MtasUpdateRequestProcessorResultItem implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The token term. */
  public String tokenTerm;

  /** The token offset start. */
  public Integer tokenOffsetStart;

  /** The token offset end. */
  public Integer tokenOffsetEnd;

  /** The token pos incr. */
  public Integer tokenPosIncr;

  /** The token payload. */
  public byte[] tokenPayload;

  /** The token flags. */
  public Integer tokenFlags;

  /**
   * Instantiates a new mtas update request processor result item.
   *
   * @param term the term
   * @param offsetStart the offset start
   * @param offsetEnd the offset end
   * @param posIncr the pos incr
   * @param payload the payload
   * @param flags the flags
   */
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