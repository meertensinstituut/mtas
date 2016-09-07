package mtas.parser.function.util;

import java.io.IOException;

/**
 * The Class MtasFunctionParserFunctionResponseLong.
 */
public class MtasFunctionParserFunctionResponseLong
    extends MtasFunctionParserFunctionResponse {

  /** The value. */
  private long value;

  /**
   * Instantiates a new mtas function parser function response long.
   *
   * @param l
   *          the l
   * @param s
   *          the s
   */
  public MtasFunctionParserFunctionResponseLong(long l, boolean s) {
    super(s);
    value = l;
  }

  /**
   * Gets the value.
   *
   * @return the value
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
        if (defined)
          return true;
        else
          return false;
      } else {
        return false;
      }
    } catch (IOException e) {
      if (!defined)
        return true;
      else
        return false;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return defined ? "long:" + String.valueOf(value) : "long:undefined";
  }

}
