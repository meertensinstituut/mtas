package mtas.solr.update.processor;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

public class MtasUpdateRequestProcessorResultReader implements Closeable {

  /** The stored string value. */
  private String storedStringValue;

  private int tokenNumber;

  private ObjectInputStream ois;

  private String fileName;
  
  private Iterator<MtasUpdateRequestProcessorResultItem> iterator;

  public MtasUpdateRequestProcessorResultReader(String fileName)
      throws FileNotFoundException, IOException {
    this.fileName = fileName;
    ObjectInputStream ois = new ObjectInputStream(
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
            } catch (ClassNotFoundException e) {
              return null;
            } catch (IOException e) {
              return null;
            }
          } else {
            return null;
          }
        }
      };

    } catch (ClassNotFoundException e) {
      throw new IOException("invalid tokenStream");
    }

  }

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
  
  public Iterator<MtasUpdateRequestProcessorResultItem> getIterator() {
    return iterator;
  }

  @Override
  public void close() throws IOException {
    ois.close();
    File file = new File(fileName);
    file.delete();
  }

}
