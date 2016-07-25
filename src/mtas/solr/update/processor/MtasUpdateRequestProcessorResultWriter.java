package mtas.solr.update.processor;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.apache.lucene.util.BytesRef;

/**
 * The Class MtasUpdateRequestProcessorResultWriter.
 */
public class MtasUpdateRequestProcessorResultWriter implements Closeable {

  private ObjectOutputStream objectOutputStream;

  private FileOutputStream fileOutputStream;

  private boolean closed;

  private int tokenNumber;

  private File file;

  /**
   * Instantiates a new mtas update request processor result writer.
   *
   * @param value
   *          the value
   * @throws IOException
   */
  public MtasUpdateRequestProcessorResultWriter(String value) {
    closed = false;
    tokenNumber = 0;
    file = null;
    fileOutputStream = null;
    objectOutputStream = null;
    try {
      file = File.createTempFile("MtasUpdateRequestProcessorResult", ".data");
      fileOutputStream = new FileOutputStream(file);
      objectOutputStream = new ObjectOutputStream(fileOutputStream);
      objectOutputStream.writeObject(value);
    } catch (IOException e) {
      forceCloseAndDelete();
    }
  }

  /**
   * Adds the item.
   *
   * @param term
   *          the term
   * @param offsetStart
   *          the offset start
   * @param offsetEnd
   *          the offset end
   * @param posIncr
   *          the pos incr
   * @param payload
   *          the payload
   * @param flags
   *          the flags
   * @throws IOException
   */
  public void addItem(String term, Integer offsetStart, Integer offsetEnd,
      Integer posIncr, BytesRef payload, Integer flags) {
    if (!closed) {
      tokenNumber++;
      MtasUpdateRequestProcessorResultItem item = new MtasUpdateRequestProcessorResultItem(
          term, offsetStart, offsetEnd, posIncr, payload, flags);
      try {
        objectOutputStream.writeObject(item);
        objectOutputStream.reset();
        objectOutputStream.flush();
      } catch (IOException e) {
        forceCloseAndDelete();
      }
    }
  }

  public int getTokenNumber() {
    return tokenNumber;
  }

  /**
   * Creates the file.
   *
   * @return the string
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public String getFileName() throws IOException {
    if (file != null) {
      return file.getAbsolutePath();
    } else {
      throw new IOException("no file");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      objectOutputStream.close();
      fileOutputStream.close();
      closed = true;
    }
  }

  public void forceCloseAndDelete() {
    try {
      if (objectOutputStream != null) {
        objectOutputStream.close();
        objectOutputStream = null;
      }
      if (fileOutputStream != null) {
        fileOutputStream.close();
        fileOutputStream = null;
      }
    } catch (IOException e) {
      // do nothing;
    }
    closed = true;
    tokenNumber = 0;
    if (file != null) {
      if (file.exists() && file.canWrite()) {
        file.delete();
      }
      file = null;
    }
  }

}
