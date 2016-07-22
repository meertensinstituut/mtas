package mtas.solr.update.processor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.lucene.util.BytesRef;

/**
 * The Class MtasUpdateRequestProcessorResult.
 */
public class MtasUpdateRequestProcessorResult implements Serializable {
  
  private static final long serialVersionUID = 1L;

  /** The stored string value. */
  private String storedStringValue;
  
  /** The tokens. */
  private List<String> tokenTerms;
  private List<Integer> tokenOffsetStarts;
  private List<Integer> tokenOffsetEnds;
  private List<Integer> tokenPosIncrs;
  private List<byte[]> tokenPayloads;
  private List<Integer> tokenFlags;
  
  /**
   * Instantiates a new mtas update request processor result.
   *
   * @param value the value
   */
  public MtasUpdateRequestProcessorResult(String value) {
    storedStringValue = value;
    tokenTerms = new ArrayList<String>();
    tokenOffsetStarts = new ArrayList<Integer>();
    tokenOffsetEnds = new ArrayList<Integer>();
    tokenPosIncrs = new ArrayList<Integer>();
    tokenPayloads = new ArrayList<byte[]>();
    tokenFlags = new ArrayList<Integer>();
  }
  
  /**
   * Adds the item.
   *
   * @param term the term
   * @param offsetStart the offset start
   * @param offsetEnd the offset end
   * @param posIncr the pos incr
   * @param payload the payload
   * @param flags the flags
   */
  public void addItem(String term, Integer offsetStart, Integer offsetEnd, Integer posIncr, BytesRef payload, Integer flags) {
    tokenTerms.add(term);    
    if(offsetStart!=null && offsetEnd!=null) {
      tokenOffsetStarts.add(offsetStart.intValue());
      tokenOffsetEnds.add(offsetEnd.intValue());
    } else {
      tokenOffsetStarts.add(null);
      tokenOffsetEnds.add(null);
    }
    if(posIncr!=null && posIncr!=1) {
      tokenPosIncrs.add(posIncr.intValue());     
    } else {
      tokenPosIncrs.add(null); 
    }
    if(payload!=null) {
      tokenPayloads.add(payload.bytes);
    } else {
      tokenPayloads.add(null);
    }
    if(flags!=null) {
      tokenFlags.add(flags);
    } else {
      tokenFlags.add(null);
    }
  }
  
  /**
   * Gets the stored string value.
   *
   * @return the stored string value
   */
  public String getStoredStringValue() {
    return storedStringValue;
  }
  
  /**
   * Gets the stored bin value.
   *
   * @return the stored bin value
   */
  public byte[] getStoredBinValue() {
    return null;
  }
  
  /**
   * Gets the tokens.
   *
   * @return the tokens
   */
  public Integer getTokenNumber() {
    return tokenTerms.size();
  }
  
  public String getTokenTerm(int id) {
    return tokenTerms.get(id);
  }
  
  public Integer getTokenOffsetStart(int id) {
    return tokenOffsetStarts.get(id);
  }
  
  public Integer getTokenOffsetEnd(int id) {
    return tokenOffsetEnds.get(id);
  }
  
  public Integer getTokenPosIncr(int id) {
    return tokenPosIncrs.get(id);
  }
  
  public byte[] getTokenPayload(int id) {
    return tokenPayloads.get(id);
  }
  
  public Integer getTokenFlag(int id) {
    return tokenFlags.get(id);
  }
  
  /**
   * From string.
   *
   * @param s the s
   * @return the mtas update request processor result
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws ClassNotFoundException the class not found exception
   */
  public static MtasUpdateRequestProcessorResult fromString(String s)
      throws IOException, ClassNotFoundException {
    try {
      byte[] data = Base64.getDecoder().decode(s);
      ObjectInputStream ois = new ObjectInputStream(
          new ByteArrayInputStream(data));
      Object o = ois.readObject();
      ois.close();
      if(o instanceof MtasUpdateRequestProcessorResult) {
        return (MtasUpdateRequestProcessorResult) o;
      } else {
        throw new IOException("could not deserialize "+s);
      }
    } catch (IllegalArgumentException e) {      
      return null;
    }
  }

  /**
   * To string.
   *
   * @param o the o
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static String toString(MtasUpdateRequestProcessorResult o) throws IOException {
    System.out.println("Maak string "+o.tokenTerms.size());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(o);
    oos.close();     
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

}

