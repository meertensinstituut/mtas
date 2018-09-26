package mtas.parser.function.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class MtasFunctionParserFunctionResponseLong extends MtasFunctionParserFunctionResponse {
  private static Log log = LogFactory
      .getLog(MtasFunctionParserFunctionResponseLong.class);
  private long value;

  public MtasFunctionParserFunctionResponseLong(long l, boolean s) {
    super(s);
    value = l;
  }

  public long getValue() throws IOException {
    if (defined) {
      return value;
    } else {
      throw new IOException("undefined");
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MtasFunctionParserFunctionResponseLong other = (MtasFunctionParserFunctionResponseLong) obj;
    try {
      if (value == other.getValue()) {
        return defined;
      } else {
        return false;
      }
    } catch (IOException e) {
      log.debug(e);
      return !defined;
    }
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (int) ((h * 7) ^ value);
    return h;
  }

  @Override
  public String toString() {
    return defined ? "long:" + value : "long:undefined";
  }
}
