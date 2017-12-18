package mtas.codec.util.distance;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.util.BytesRef;

public abstract class Distance {

  protected final String prefix;
  protected final String base;
  protected final Double maximum;
  protected final Map<String,String> parameters;
  
  public static final String NAME = "distance";
  
  public Distance(String prefix, String base, Double maximum, Map<String,String> parameters) throws IOException {
    this.prefix = prefix;
    this.base = base;
    this.maximum = maximum;   
    this.parameters = parameters;
  }
    
  public abstract double compute(String key);
  
  public abstract boolean validate(BytesRef term);
  
}
