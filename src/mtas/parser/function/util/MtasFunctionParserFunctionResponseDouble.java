package mtas.parser.function.util;

import java.io.IOException;

/**
 * The Class MtasFunctionParserFunctionResponseDouble.
 */
public class MtasFunctionParserFunctionResponseDouble extends MtasFunctionParserFunctionResponse {

  /** The value. */
  private double value;
  
  /**
   * Instantiates a new mtas function parser function response double.
   *
   * @param d the d
   * @param s the s
   */
  public MtasFunctionParserFunctionResponseDouble(double d, boolean s) {
    super(s);    
    value = d;    
  }

  /**
   * Gets the value.
   *
   * @return the value
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public double getValue() throws IOException {
    if(defined) {
      return value;
    } else {
      throw new IOException("undefined");
    }
  }
  
  /* (non-Javadoc)
   * @see mtas.parser.function.util.MtasFunctionParserFunctionResponse#equals(java.lang.Object)
   */
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
      if(value == other.getValue()) {
        if(defined) 
          return true;
        else
          return false;
      } else {
        return false;
      }
    } catch (IOException e) {
      if(!defined)
        return true;
      else
        return false;
    }
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return defined?"double:"+String.valueOf(value):"double:undefined";
  }
  
}
