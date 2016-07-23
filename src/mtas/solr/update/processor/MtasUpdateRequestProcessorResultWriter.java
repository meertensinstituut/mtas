package mtas.solr.update.processor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.Base64;

/**
 * The Class MtasUpdateRequestProcessorResult.
 */
/**
 * @author matthijs
 *
 */
public class MtasUpdateRequestProcessorResultWriter {
  
  /** The stored string value. */
  private String storedStringValue;
  
  private List<MtasUpdateRequestProcessorResultItem> items;  
  
  private boolean closed;
  
  /**
   * Instantiates a new mtas update request processor result.
   *
   * @param value the value
   */
  public MtasUpdateRequestProcessorResultWriter(String value) {
    storedStringValue = value;
    items = new ArrayList<MtasUpdateRequestProcessorResultItem>();
    closed = false;
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
    if(!closed) {
      items.add(new MtasUpdateRequestProcessorResultItem(term, offsetStart, offsetEnd, posIncr, payload, flags));
    }  
  }
  
  
  public int getTokenNumber() {
    return items.size();
  }
  
  
  /**
   * Gets the token number.
   *
   * @return the token number
   */
   
  
  /**
   * From file.
   *
   * @param fileName the file name
   * @return the mtas update request processor result
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws ClassNotFoundException the class not found exception
   */
  
  public static MtasUpdateRequestProcessorResultWriter fromFileOld(String fileName)
      throws IOException, ClassNotFoundException {
    try {
      ObjectInputStream ois =
          new ObjectInputStream(new FileInputStream(fileName));      
      Object o = ois.readObject();
      ois.close();
      File file = new File(fileName);
      file.delete();
      if(o instanceof MtasUpdateRequestProcessorResultWriter) {
        return (MtasUpdateRequestProcessorResultWriter) o;
      } else {
        throw new IOException("could not deserialize "+fileName);
      }
    } catch (IllegalArgumentException e) {      
      return null;
    }
  }

  /**
   * To file.
   *
   * @param o the o
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  
  public static String toFileOld(MtasUpdateRequestProcessorResultWriter o) throws IOException {
    File temporaryFile = File.createTempFile("MtasUpdateRequestProcessorResult", ".data");
    FileOutputStream fos = new FileOutputStream(temporaryFile);
    ObjectOutputStream oos = new ObjectOutputStream(fos);
    oos.writeObject(o);
    oos.close();     
    return temporaryFile.getAbsolutePath();
  }
  
  
  
  public String createFile() throws IOException {
    File temporaryFile = File.createTempFile("MtasUpdateRequestProcessorResult", ".data");
    FileOutputStream fos = new FileOutputStream(temporaryFile);
    ObjectOutputStream oos = new ObjectOutputStream(fos);
    oos.writeObject(storedStringValue);
    oos.writeInt(items.size());
    for(MtasUpdateRequestProcessorResultItem item : items) {
      oos.writeObject(item);      
    }
    oos.close();  
    return temporaryFile.getAbsolutePath();    
  }

  
  

}

