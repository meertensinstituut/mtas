package mtas.parser.function.util;

import java.io.IOException;

public class MtasFunctionParserFunctionResponseDouble extends MtasFunctionParserFunctionResponse {

  private double value;
  
  public MtasFunctionParserFunctionResponseDouble(double d, boolean s) {
    super(s);    
    value = d;    
  }

  public double getValue() throws IOException {
    if(defined) {
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
  
  @Override
  public String toString() {
    return defined?"double:"+String.valueOf(value):"double:undefined";
  }
  
}
