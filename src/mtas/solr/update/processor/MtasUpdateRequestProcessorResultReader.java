package mtas.solr.update.processor;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

/**
 * The Class MtasUpdateRequestProcessorResultReader.
 */
public class MtasUpdateRequestProcessorResultReader implements Closeable {

  /** The stored string value. */
  private String storedStringValue;

  /** The token number. */
  private int tokenNumber;

  /** The ois. */
  private ObjectInputStream ois;

  /** The file name. */
  private String fileName;
  
  /** The iterator. */
  private Iterator<MtasUpdateRequestProcessorResultItem> iterator;

  /**
   * Instantiates a new mtas update request processor result reader.
   *
   * @param fileName the file name
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasUpdateRequestProcessorResultReader(String fileName)
      throws IOException {
    this.fileName = fileName;
    ois = new ObjectInputStream(
        new FileInputStream(fileName));
    try {
      storedStringValue = (String) ois.readObject();
      tokenNumber = ois.readInt();
      iterator = new Iterator<MtasUpdateRequestProcessorResultItem>() {
        int position = 0;
        @Override
        public boolean hasNext() {
          return position<tokenNumber;
        }
        @Override
        public MtasUpdateRequestProcessorResultItem next() {
          if(position<tokenNumber) {
            position++;
            try {
              return (MtasUpdateRequestProcessorResultItem) ois.readObject();
            } catch (ClassNotFoundException e1) {
              position = tokenNumber;
              forceClose();
              return null;
            } catch (IOException e3) {
              position = tokenNumber;
              forceClose();
              return null;
            }
          } else {
            forceClose();
            return null;
          }
        }
      };

    } catch (ClassNotFoundException e) {
      throw new IOException("invalid tokenStream");
    }

  }

  /**
   * Gets the token number.
   *
   * @return the token number
   */
  public int getTokenNumber() {
    return tokenNumber;
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
   * Gets the iterator.
   *
   * @return the iterator
   */
  public Iterator<MtasUpdateRequestProcessorResultItem> getIterator() {
    return iterator;
  }

  /* (non-Javadoc)
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    ois.close();
    File file = new File(fileName);
    file.delete();
  }
  
  /**
   * Force close.
   */
  private void forceClose() {
    try {
      ois.close();
    } catch (IOException e) {
      //do nothing
    }
    File file = new File(fileName);
    file.delete();
  }

}
