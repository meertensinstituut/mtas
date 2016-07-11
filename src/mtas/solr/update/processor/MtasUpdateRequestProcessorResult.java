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

public class MtasUpdateRequestProcessorResult implements Serializable {
  private String storedStringValue;
  private List<Map<String,Object>> tokens;
  
  public static final String TERM_KEY = "t";
  public static final String OFFSET_START_KEY = "s";
  public static final String OFFSET_END_KEY = "e";
  public static final String POSINCR_KEY = "i";
  public static final String PAYLOAD_KEY = "p";
  public static final String TYPE_KEY = "y";
  public static final String FLAGS_KEY = "f"; 
 
  public MtasUpdateRequestProcessorResult(String value) {
    storedStringValue = value;
    tokens = new ArrayList<Map<String,Object>>();
  }
  
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
  
  public String getStoredStringValue() {
    return storedStringValue;
  }
  
  public byte[] getStoredBinValue() {
    return null;
  }
  
  public List<Map<String,Object>> getTokens() {
    return tokens;
  }
  
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

  public static String toString(MtasUpdateRequestProcessorResult o) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(o);
    oos.close();
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

}

