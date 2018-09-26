package mtas.codec.util.distance;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

public class LevenshteinDistance extends Distance {
  protected final double[] initialState;
  protected final static double defaultDeletionDistance = 1.0;
  protected final static double defaultInsertionDistance = 1.0;
  protected final static double defaultReplaceDistance = 1.0;
  protected double deletionDistance;
  protected double insertionDistance;
  protected double replaceDistance;
  protected final static String PARAMETER_DELETIONDISTANCE = "deletionDistance";
  protected final static String PARAMETER_INSERTIONDISTANCE = "insertionDistance";
  protected final static String PARAMETER_REPLACEDISTANCE = "replaceDistance";

  public LevenshteinDistance(String prefix, String base, Double minimum, Double maximum,
      Map<String, String> parameters) throws IOException {
    super(prefix, base, minimum, maximum, parameters);
    deletionDistance = defaultDeletionDistance;
    insertionDistance = defaultInsertionDistance;
    replaceDistance = defaultReplaceDistance;
    if (parameters != null) {
      for (Entry<String, String> entry : parameters.entrySet()) {
        if (entry.getKey().equals(PARAMETER_DELETIONDISTANCE)) {
          deletionDistance = Double.parseDouble(entry.getValue());
        } else if (entry.getKey().equals(PARAMETER_INSERTIONDISTANCE)) {
          insertionDistance = Double.parseDouble(entry.getValue());
        } else if (entry.getKey().equals(PARAMETER_REPLACEDISTANCE)) {
          replaceDistance = Double.parseDouble(entry.getValue());
        }
      }
    }
    if (deletionDistance < 0 || insertionDistance < 0 || replaceDistance < 0) {
      throw new IOException("distances should be zero or positive");
    }
    initialState = new double[base.length() + 1];
    for (int i = 0; i <= base.length(); i++) {
      initialState[i] = i * insertionDistance;
    }
  }

  public boolean validateMaximum(BytesRef term) {
    if (maximum == null) {
      return true;
    } else {
      double[][] state = _start();
      char ch1;
      int i = term.offset + prefixOffset;
      for (; i < term.length; i++) {
        ch1 = (char) term.bytes[i];
        if (ch1 == 0x00) {
          break;
        }
        state = _step(state, ch1);
        if (!_can_match(state)) {
          return false;
        }
      }
      return _is_match(state);
    }
  }
  
  @Override
  public boolean validateMinimum(BytesRef term) {
    if (minimum == null) {
      return true;
    } else {
      return compute(term)>minimum;
    }
  }
  
  @Override
  public double compute(BytesRef term) {
    double[][] state = _start();
    char ch1;
    int i = term.offset + prefixOffset;
    for (; i < term.length; i++) {
      ch1 = (char) term.bytes[i];
      if (ch1 == 0x00) {
        break;
      }
      state = _step(state, ch1);
    }
    return _distance(state);
  }
  
  @Override
  public double compute(String key) {
    double[][] state = _start();
    for (char ch1 : key.toCharArray()) {
      if (ch1 == 0x00) {
        break;
      }
      state = _step(state, ch1);
    }
    return _distance(state);
  }

  private double[][] _start() {
    double[][] startState = new double[2][];
    startState[0] = new double[initialState.length];
    startState[1] = Arrays.copyOf(initialState, initialState.length);
    return startState;
  }

  private double[][] _step(double[][] state, char ch1) {
    double cost;
    _shift(state);
    state[1][0] = state[0][0] + deletionDistance;
    for (int i = 0; i < base.length(); i++) {
      cost = (base.charAt(i) == ch1) ? 0 : replaceDistance;
      state[1][i + 1] = Math.min(state[1][i] + insertionDistance,
          state[0][i] + cost);
      state[1][i + 1] = Math.min(state[1][i + 1],
          state[0][i + 1] + deletionDistance);
    }
    return state;
  }

  private void _shift(double[][] state) {
    double[] tmpState = state[0];
    state[0] = state[1];
    state[1] = tmpState;
  }

  private boolean _is_match(double[][] state) {
    return state[1][state[1].length - 1] < maximum;
  }

  private boolean _can_match(double[][] state) {
    for (double d : state[1]) {
      if (d < maximum) {
        return true;
      }
    }
    return false;
  }

  private double _distance(double[][] state) {
    return state[1][state[1].length - 1];
  }
}
