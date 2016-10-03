package mtas.codec.util.collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import mtas.codec.util.CodecUtil;

/**
 * The Class MtasDataItem.
 *
 * @param <T1> the generic type
 * @param <T2> the generic type
 */
public abstract class MtasDataItem<T1 extends Number & Comparable<T1>, T2 extends Number & Comparable<T2>>
    implements Serializable, Comparable<MtasDataItem<T1, T2>> {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The sub. */
  protected MtasDataCollector<?, ?> sub;

  /** The stats items. */
  protected TreeSet<String> statsItems;

  /** The sort direction. */
  protected String sortType, sortDirection;

  /** The error number. */
  protected int errorNumber;

  /** The error list. */
  protected HashMap<String, Integer> errorList;

  /** The comparable sort value. */
  protected NumberComparator<?> comparableSortValue;

  /** The recompute comparable sort value. */
  protected boolean recomputeComparableSortValue;

  /** The source number. */
  protected int sourceNumber;

  /**
   * Instantiates a new mtas data item.
   *
   * @param sub the sub
   * @param statsItems the stats items
   * @param sortType the sort type
   * @param sortDirection the sort direction
   * @param errorNumber the error number
   * @param errorList the error list
   * @param sourceNumber the source number
   */
  public MtasDataItem(MtasDataCollector<?, ?> sub, TreeSet<String> statsItems,
      String sortType, String sortDirection, int errorNumber,
      HashMap<String, Integer> errorList, int sourceNumber) {
    this.sub = sub;
    this.statsItems = statsItems;
    this.sortType = sortType;
    this.sortDirection = sortDirection;
    this.errorNumber = errorNumber;
    this.errorList = errorList;
    this.sourceNumber = sourceNumber;
    this.comparableSortValue = null;
    this.recomputeComparableSortValue = true;
  }

  /**
   * Adds the.
   *
   * @param newItem the new item
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void add(MtasDataItem<T1, T2> newItem) throws IOException;

  /**
   * Rewrite.
   *
   * @param showDebugInfo the show debug info
   * @return the map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract Map<String, Object> rewrite(boolean showDebugInfo)
      throws IOException;

  /**
   * Gets the sub.
   *
   * @return the sub
   */
  public MtasDataCollector<?, ?> getSub() {
    return sub;
  }

  /**
   * Gets the compare value type.
   *
   * @return the compare value type
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected abstract int getCompareValueType() throws IOException;

  /**
   * Compute comparable value.
   */
  private void computeComparableValue() {
    recomputeComparableSortValue = false;
    try {
      int type = getCompareValueType();
      switch (type) {
      case 0:
        comparableSortValue = getCompareValue0();
        break;
      case 1:
        comparableSortValue = getCompareValue1();
        break;
      case 2:
        comparableSortValue = getCompareValue2();
        break;
      default:
        comparableSortValue = null;
        break;
      }
    } catch (IOException e) {
      comparableSortValue = null;
    }
  }

  /**
   * Gets the comparable value.
   *
   * @return the comparable value
   */
  protected final NumberComparator<?> getComparableValue() {
    if (recomputeComparableSortValue) {
      computeComparableValue();
    }
    return comparableSortValue;
  }

  /**
   * Gets the compare value0.
   *
   * @return the compare value0
   */
  protected abstract NumberComparator<Long> getCompareValue0();

  /**
   * Gets the compare value1.
   *
   * @return the compare value1
   */
  protected abstract NumberComparator<T1> getCompareValue1();

  /**
   * Gets the compare value2.
   *
   * @return the compare value2
   */
  protected abstract NumberComparator<T2> getCompareValue2();

  /**
   * The Class NumberComparator.
   *
   * @param <T> the generic type
   */
  public class NumberComparator<T extends Number & Comparable<T>>
      implements Comparable<T>, Serializable, Cloneable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The value. */
    T value;

    /**
     * Instantiates a new number comparator.
     *
     * @param value the value
     */
    public NumberComparator(T value) {
      this.value = value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#clone()
     */
    @Override
    public NumberComparator<T> clone() {
      return new NumberComparator<T>(this.value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(T compareValue) {
      return value.compareTo(compareValue);
    }

    /**
     * Gets the value.
     *
     * @return the value
     */
    public T getValue() {
      return value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    public String toString() {
      return value.toString();
    }

    /**
     * Adds the.
     *
     * @param newValue the new value
     * @throws IOException Signals that an I/O exception has occurred.
     */
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

    /**
     * Subtract.
     *
     * @param newValue the new value
     * @throws IOException Signals that an I/O exception has occurred.
     */
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

    /**
     * Recompute boundary.
     *
     * @param n the n
     * @return the number comparator
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public NumberComparator<T> recomputeBoundary(int n) throws IOException {
      if (sortDirection.equals(CodecUtil.SORT_DESC)) {
        if (value instanceof Integer) {
          return new NumberComparator(Math.floorDiv((Integer) value, n));
        } else if (value instanceof Long) {
          return new NumberComparator(Math.floorDiv((Long) value, n));
        } else if (value instanceof Float) {
          return new NumberComparator(((Float) value) / n);
        } else if (value instanceof Double) {
          return new NumberComparator(((Double) value) / n);
        } else {
          throw new IOException("unknown NumberComparator");
        }
      } else if (sortDirection.equals(CodecUtil.SORT_ASC)) {
        return new NumberComparator(getValue());
      } else {
        throw new IOException("unknown sortDirection " + sortDirection);
      }
    }

  }

}