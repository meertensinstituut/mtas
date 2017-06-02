package mtas.solr.update.processor;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Class MtasUpdateRequestProcessorResultReader.
 */
public class MtasUpdateRequestProcessorResultReader implements Closeable {

  /** The Constant log. */
  private static final Log log = LogFactory
      .getLog(MtasUpdateRequestProcessorResultReader.class);

  /** The stored string value. */
  private String storedStringValue;
 
  /** The file input stream. */
  private FileInputStream fileInputStream;

  /** The object input stream. */
  private ObjectInputStream objectInputStream;

  /** The file. */
  private File file;

  /** The iterator. */
  private Iterator<MtasUpdateRequestProcessorResultItem> iterator;

  /** The closed. */
  private boolean closed;

  /**
   * Instantiates a new mtas update request processor result reader.
   *
   * @param fileName the file name
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasUpdateRequestProcessorResultReader(String fileName)
      throws IOException {
    file = null;
    fileInputStream = null;
    objectInputStream = null;
    closed = false;
    iterator = null;
    if (fileName != null) {
      file = new File(fileName);
      fileInputStream = new FileInputStream(file);
      objectInputStream = new ObjectInputStream(fileInputStream);
      try {
        Object o = objectInputStream.readObject();
        if (o instanceof String) {
          storedStringValue = (String) o;
        } else {
          throw new IOException("invalid tokenStream");
        }
        iterator = new Iterator<MtasUpdateRequestProcessorResultItem>() {
          MtasUpdateRequestProcessorResultItem next = null;

          @Override
          public boolean hasNext() {
            if (!closed) {
              if (next != null) {
                return true;
              } else {
                next = getNext();                
                return next != null;
              }
            } else {
              return false;
            }
          }

          @Override
          public MtasUpdateRequestProcessorResultItem next() {
            if (!closed) {
              MtasUpdateRequestProcessorResultItem result;
              if (next != null) {
                result = next;
                next = null;
                return result;
              } else {
                next = getNext();
                if (next != null) {
                  result = next;
                  next = null;
                  return result;
                } else {
                  throw new NoSuchElementException();
                }
              }
            } else {
              throw new NoSuchElementException();
            }
          }

          private MtasUpdateRequestProcessorResultItem getNext() {
            if (!closed) {
              try {                
                Object o = objectInputStream.readObject();
                if (o instanceof MtasUpdateRequestProcessorResultItem) {
                  return (MtasUpdateRequestProcessorResultItem) o;
                } else {
                  forceClose();
                  return null;
                }
              } catch (ClassNotFoundException | IOException e) {
                log.debug(e.getClass().getSimpleName()
                    + " while retrieving data from " + fileName, e);
                forceClose();
                return null;
              }
            } else {
              return null;
            }
          }
        };
      } catch (IOException e) {
        log.error(e.getClass().getSimpleName() + " while processing " + fileName
            + " (" + e.getMessage() + ")", e);
        forceClose();
        throw new IOException(e.getMessage());
      } catch (ClassNotFoundException e) {
        log.error(e.getClass().getSimpleName() + " while processing " + fileName
            + " (" + e.getMessage() + ")", e);
        forceClose();
        throw new IOException("invalid tokenStream");
      }
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
    return new byte[0];
  }

  /**
   * Gets the iterator.
   *
   * @return the iterator
   */
  public Iterator<MtasUpdateRequestProcessorResultItem> getIterator() {
    return iterator;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    forceClose();
  }

  /**
   * Force close.
   */
  private void forceClose() {
    if (file != null) {
      if (file.exists() && file.canWrite() && !file.delete()) {
        log.debug("couldn't delete " + file.getName());
      }
      file = null;
    }
    try {
      objectInputStream.close();
    } catch (IOException e) {
      log.debug(e);
    }
    closed = true;
  }

}
