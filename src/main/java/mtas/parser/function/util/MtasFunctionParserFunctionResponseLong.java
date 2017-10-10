package mtas.parser.function.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Class MtasFunctionParserFunctionResponseLong.
 */
public class MtasFunctionParserFunctionResponseLong
    extends MtasFunctionParserFunctionResponse {

  /** The log. */
  private static Log log = LogFactory
      .getLog(MtasFunctionParserFunctionResponseLong.class);

  /** The value. */
  private long value;

  /**
   * Instantiates a new mtas function parser function response long.
   *
   * @param l the l
   * @param s the s
   */
  public MtasFunctionParserFunctionResponseLong(long l, boolean s) {
    super(s);
    value = l;
  }

  /**
   * Gets the value.
   *
   * @return the value
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public long getValue() throws IOException {
    if (defined) {
      return value;
    } else {
      throw new IOException("undefined");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.parser.function.util.MtasFunctionParserFunctionResponse#equals(java.
   * lang.Object)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (int) ((h * 7) ^ value);
    return h;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return defined ? "long:" + value : "long:undefined";
  }

}
