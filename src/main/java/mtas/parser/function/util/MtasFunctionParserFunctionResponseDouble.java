package mtas.parser.function.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class MtasFunctionParserFunctionResponseDouble extends MtasFunctionParserFunctionResponse {
  private static Log log = LogFactory
      .getLog(MtasFunctionParserFunctionResponseDouble.class);

  private double value;

  public MtasFunctionParserFunctionResponseDouble(double d, boolean s) {
    super(s);
    value = d;
  }

  public double getValue() throws IOException {
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
    MtasFunctionParserFunctionResponseDouble other = (MtasFunctionParserFunctionResponseDouble) obj;
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
    h = (h * 7) ^ (int) value;
    return h;
  }

  @Override
  public String toString() {
    return defined ? "double:" + value : "double:undefined";
  }
}
