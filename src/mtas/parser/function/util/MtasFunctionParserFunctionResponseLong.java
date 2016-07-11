package mtas.parser.function.util;

import java.io.IOException;

public class MtasFunctionParserFunctionResponseLong extends MtasFunctionParserFunctionResponse {

  private long value;
  
  public MtasFunctionParserFunctionResponseLong(long l, boolean s) {
    super(s);    
    value = l;
  }

  public long getValue() throws IOException {
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
    MtasFunctionParserFunctionResponseLong other = (MtasFunctionParserFunctionResponseLong) obj;
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
    return defined?"long:"+String.valueOf(value):"long:undefined";
  }
  
}
