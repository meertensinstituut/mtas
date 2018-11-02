package mtas.solr.update.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class MtasUpdateRequestProcessorResultWriter implements Closeable {
  private static final Log log = LogFactory
      .getLog(MtasUpdateRequestProcessorResultWriter.class);

  private ObjectOutputStream objectOutputStream;
  private FileOutputStream fileOutputStream;
  private boolean closed;
  private int tokenNumber;
  private File file;

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
      log.debug(e);
    }
  }

  void addItem(String term, Integer offsetStart, Integer offsetEnd,
               int posIncr, BytesRef payload, Integer flags) {
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
        log.debug(e);
      }
    }
  }

  public int getTokenNumber() {
    return tokenNumber;
  }

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
      log.debug(e);
    }
    closed = true;
    tokenNumber = 0;
    if (file != null) {
      if (file.exists() && file.canWrite() && !file.delete()) {
        log.debug("couldn't delete " + file.getName());
      }
      file = null;
    }
  }
}
