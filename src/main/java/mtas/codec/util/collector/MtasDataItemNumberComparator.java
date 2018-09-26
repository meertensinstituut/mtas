package mtas.codec.util.collector;

import mtas.codec.util.CodecUtil;

import java.io.IOException;
import java.io.Serializable;

public final class MtasDataItemNumberComparator<T extends Number & Comparable<T>>
    implements Comparable<T>, Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  T value;
  String sortDirection;

  public MtasDataItemNumberComparator(T value, String sortDirection) {
    this.value = value;
    this.sortDirection = sortDirection;
  }

  @Override
  public MtasDataItemNumberComparator<T> clone() {
    return new MtasDataItemNumberComparator<>(this.value, this.sortDirection);
  }

  @Override
  public int compareTo(T compareValue) {
    return value.compareTo(compareValue);
  }

  public T getValue() {
    return value;
  }

  public String toString() {
    return value.toString();
  }

  @SuppressWarnings("unchecked")
  public void add(T newValue) throws IOException {
    if (value instanceof Integer && newValue instanceof Integer) {
      value = (T) Integer.valueOf(value.intValue() + newValue.intValue());
    } else if (value instanceof Long && newValue instanceof Long) {
      value = (T) Long.valueOf(value.longValue() + newValue.longValue());
    } else if (value instanceof Float && newValue instanceof Float) {
      value = (T) Float.valueOf(value.floatValue() + newValue.floatValue());
    } else if (value instanceof Double && newValue instanceof Double) {
      value = (T) Double.valueOf(value.doubleValue() + newValue.longValue());
    } else {
      throw new IOException("incompatible NumberComparators");
    }
  }

  @SuppressWarnings("unchecked")
  public void subtract(T newValue) throws IOException {
    if (value instanceof Integer && newValue instanceof Integer) {
      value = (T) Integer.valueOf(value.intValue() - newValue.intValue());
    } else if (value instanceof Long && newValue instanceof Long) {
      value = (T) Long.valueOf(value.longValue() - newValue.longValue());
    } else if (value instanceof Float && newValue instanceof Float) {
      value = (T) Float.valueOf(value.floatValue() - newValue.floatValue());
    } else if (value instanceof Double && newValue instanceof Double) {
      value = (T) Double.valueOf(value.doubleValue() - newValue.longValue());
    } else {
      throw new IOException("incompatible NumberComparators");
    }
  }

  public MtasDataItemNumberComparator<T> recomputeBoundary(int n)
      throws IOException {
    if (sortDirection.equals(CodecUtil.SORT_DESC)) {
      if (value instanceof Integer) {
        return new MtasDataItemNumberComparator(
            Math.floorDiv((Integer) value, n), sortDirection);
      } else if (value instanceof Long) {
        return new MtasDataItemNumberComparator(Math.floorDiv((Long) value, n),
            sortDirection);
      } else if (value instanceof Float) {
        return new MtasDataItemNumberComparator(((Float) value) / n,
            sortDirection);
      } else if (value instanceof Double) {
        return new MtasDataItemNumberComparator(((Double) value) / n,
            sortDirection);
      } else {
        throw new IOException("unknown NumberComparator");
      }
    } else if (sortDirection.equals(CodecUtil.SORT_ASC)) {
      return new MtasDataItemNumberComparator(getValue(), sortDirection);
    } else {
      throw new IOException("unknown sortDirection " + sortDirection);
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
    MtasDataItemNumberComparator<?> that = (MtasDataItemNumberComparator<?>) obj;
    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ value.hashCode();
    return h;
  }
}