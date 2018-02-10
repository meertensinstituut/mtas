package mtas.codec.util.distance;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.util.BytesRef;

import mtas.analysis.token.MtasToken;

public abstract class Distance {

  protected final String prefix;
  protected final String base;
  public final Double minimum;
  public final Double maximum;
  protected final Map<String,String> parameters;
  protected final int prefixOffset;
  
  private static final double DOUBLE_TOLERANCE = 5E-16;
  
  public static final String NAME = "distance";
  
  public Distance(String prefix, String base, Double minimum, Double maximum, Map<String,String> parameters) throws IOException {
    this.prefix = prefix;
    this.base = base;
    this.minimum = minimum==null?null:minimum-DOUBLE_TOLERANCE;   
    this.maximum = maximum==null?null:maximum+DOUBLE_TOLERANCE;   
    this.parameters = parameters;
    prefixOffset = prefix.length() + MtasToken.DELIMITER.length();
  }
    
  public abstract double compute(BytesRef term);
  
  public abstract double compute(String key);
  
  public boolean validate(BytesRef term) {
    return validateMaximum(term) && validateMinimum(term);
  }
  
  public abstract boolean validateMaximum(BytesRef term);
  
  public abstract boolean validateMinimum(BytesRef term);
  
}
