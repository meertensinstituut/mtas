package mtas.codec.util.collector;

import java.io.Serializable;

class MtasDataDoubleOperations implements MtasDataOperations<Double, Double>, Serializable {
  private static final long serialVersionUID = 1L;

  @Override
  public Double product11(Double arg1, Double arg2) {
    if (arg1 == null || arg2 == null) {
      return Double.NaN;
    } else {
      return arg1 * arg2;
    }
  }

  @Override
  public Double add11(Double arg1, Double arg2) {
    if (arg1 == null || arg2 == null) {
      return Double.NaN;
    } else {
      return arg1 + arg2;
    }
  }

  @Override
  public Double add22(Double arg1, Double arg2) {
    if (arg1 == null || arg2 == null) {
      return Double.NaN;
    } else {
      return arg1 + arg2;
    }
  }

  @Override
  public Double subtract12(Double arg1, Double arg2) {
    if (arg1 == null || arg2 == null) {
      return Double.NaN;
    } else {
      return arg1 - arg2;
    }
  }

  @Override
  public Double divide1(Double arg1, long arg2) {
    if (arg1 == null) {
      return Double.NaN;
    } else {
      return arg1 / arg2;
    }
  }

  @Override
  public Double divide2(Double arg1, long arg2) {
    if (arg1 == null) {
      return Double.NaN;
    } else {
      return arg1 / arg2;
    }
  }

  @Override
  public Double min11(Double arg1, Double arg2) {
    if (arg1 == null || arg2 == null) {
      return Double.NaN;
    } else {
      return Math.min(arg1, arg2);
    }
  }

  @Override
  public Double max11(Double arg1, Double arg2) {
    if (arg1 == null || arg2 == null) {
      return Double.NaN;
    } else {
      return Math.max(arg1, arg2);
    }
  }

  @Override
  public Double exp2(Double arg1) {
    if (arg1 == null) {
      return Double.NaN;
    } else {
      return Math.exp(arg1);
    }
  }

  @Override
  public Double sqrt2(Double arg1) {
    if (arg1 == null) {
      return Double.NaN;
    } else {
      return Math.sqrt(arg1);
    }
  }

  @Override
  public Double log1(Double arg1) {
    if (arg1 == null) {
      return Double.NaN;
    } else {
      return Math.log(arg1);
    }
  }

  @Override
  public Double[] createVector1(int length) {
    return new Double[length];
  }

  @Override
  public Double[] createVector2(int length) {
    return new Double[length];
  }

  @Override
  public Double[][] createMatrix1(int length) {
    return new Double[length][];
  }

  @Override
  public Double getZero1() {
    return Double.valueOf(0);
  }

  @Override
  public Double getZero2() {
    return Double.valueOf(0);
  }

}
