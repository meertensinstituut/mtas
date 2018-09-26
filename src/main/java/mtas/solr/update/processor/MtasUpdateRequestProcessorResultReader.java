package mtas.solr.update.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MtasUpdateRequestProcessorResultReader implements Closeable {
  private static final Log log = LogFactory
      .getLog(MtasUpdateRequestProcessorResultReader.class);

  private String storedStringValue;
  private FileInputStream fileInputStream;
  private ObjectInputStream objectInputStream;
  private File file;
  private Iterator<MtasUpdateRequestProcessorResultItem> iterator;
  private boolean closed;

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

  public String getStoredStringValue() {
    return storedStringValue;
  }

  public byte[] getStoredBinValue() {
    return new byte[0];
  }

  public Iterator<MtasUpdateRequestProcessorResultItem> getIterator() {
    return iterator;
  }

  @Override
  public void close() throws IOException {
    forceClose();
  }

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
