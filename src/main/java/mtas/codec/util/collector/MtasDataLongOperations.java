package mtas.codec.util.collector;

import java.io.Serializable;

class MtasDataLongOperations implements MtasDataOperations<Long, Double>, Serializable {
  private static final long serialVersionUID = 1L;

  @Override
  public Long product11(Long arg1, Long arg2) {
    if (arg1 == null || arg2 == null) {
      return null;
    } else {
      return arg1 * arg2;
    }
  }

  @Override
  public Long add11(Long arg1, Long arg2) {
    if (arg1 == null || arg2 == null) {
      return null;
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
  public Double subtract12(Long arg1, Double arg2) {
    if (arg1 == null || arg2 == null) {
      return Double.NaN;
    } else {
      return arg1.doubleValue() - arg2;
    }
  }

  @Override
  public Double divide1(Long arg1, long arg2) {
    if (arg1 == null) {
      return Double.NaN;
    } else {
      return arg1 / (double) arg2;
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
  public Long min11(Long arg1, Long arg2) {
    if (arg1 == null || arg2 == null) {
      return null;
    } else {
      return Math.min(arg1, arg2);
    }
  }

  @Override
  public Long max11(Long arg1, Long arg2) {
    if (arg1 == null || arg2 == null) {
      return null;
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
  public Double log1(Long arg1) {
    if (arg1 == null) {
      return Double.NaN;
    } else {
      return Math.log(arg1);
    }
  }

  @Override
  public Long[] createVector1(int length) {
    return new Long[length];
  }

  @Override
  public Double[] createVector2(int length) {
    return new Double[length];
  }

  @Override
  public Long[][] createMatrix1(int length) {
    return new Long[length][];
  }

  @Override
  public Long getZero1() {
    return Long.valueOf(0);
  }

  @Override
  public Double getZero2() {
    return Double.valueOf(0);
  }
}
