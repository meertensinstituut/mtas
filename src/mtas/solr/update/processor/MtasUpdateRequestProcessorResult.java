package mtas.solr.update.processor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;

/**
 * The Class MtasUpdateRequestProcessorResult.
 */
public class MtasUpdateRequestProcessorResult implements Serializable {
  
  /** The stored string value. */
  private String storedStringValue;
  
  /** The tokens. */
  private List<Map<String,Object>> tokens;
  
  /** The Constant TERM_KEY. */
  public static final String TERM_KEY = "t";
  
  /** The Constant OFFSET_START_KEY. */
  public static final String OFFSET_START_KEY = "s";
  
  /** The Constant OFFSET_END_KEY. */
  public static final String OFFSET_END_KEY = "e";
  
  /** The Constant POSINCR_KEY. */
  public static final String POSINCR_KEY = "i";
  
  /** The Constant PAYLOAD_KEY. */
  public static final String PAYLOAD_KEY = "p";
  
  /** The Constant TYPE_KEY. */
  public static final String TYPE_KEY = "y";
  
  /** The Constant FLAGS_KEY. */
  public static final String FLAGS_KEY = "f"; 
 
  /**
   * Instantiates a new mtas update request processor result.
   *
   * @param value the value
   */
  public MtasUpdateRequestProcessorResult(String value) {
    storedStringValue = value;
    tokens = new ArrayList<Map<String,Object>>();
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
    Map<String,Object> item = new HashMap<String,Object>();
    if(term!=null) {
      item.put(TERM_KEY, term);
    }
    if(offsetStart!=null && offsetEnd!=null) {
      item.put(OFFSET_START_KEY, offsetStart.intValue());
      item.put(OFFSET_END_KEY, offsetEnd.intValue());
    }
    if(posIncr!=null && posIncr!=1) {
      item.put(POSINCR_KEY, posIncr.intValue());
    }
    if(payload!=null) {
      item.put(PAYLOAD_KEY, payload.bytes);
    }
    if(flags!=null) {
      item.put(FLAGS_KEY, flags);
    }
    tokens.add(item);
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
  public List<Map<String,Object>> getTokens() {
    return tokens;
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
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(o);
    oos.close();
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

}

