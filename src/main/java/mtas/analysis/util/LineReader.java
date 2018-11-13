package mtas.analysis.util;

import java.io.IOException;
import java.io.Reader;
import java.util.Objects;

/**
 * A LineReader reads lines from a Reader and tracks the position in the reader.
 */
public class LineReader implements AutoCloseable {
  private final Reader r;
  private int pos = 0;
  private boolean lastCR = false; // previous char was '\r'

  public LineReader(Reader r) {
    this.r = Objects.requireNonNull(r);
  }

  @Override
  public void close() throws IOException {
    r.close();
  }

  public int getPosition() {
    return pos;
  }

  public String readLine() throws IOException {
    StringBuilder sb = new StringBuilder();

    loop:
    while (true) {
      int c = r.read();
      if (c == -1) {
        break;
      }
      pos++;

      switch (c) {
        case '\r':
          lastCR = true;
          break loop;
        case '\n':
          if (lastCR) { // "\r\n"
            lastCR = false;
            continue;
          }
          lastCR = false;
          break loop;
        default:
          lastCR = false;
          sb.append((char) c);
      }
    }

    return sb.toString();
  }
}
