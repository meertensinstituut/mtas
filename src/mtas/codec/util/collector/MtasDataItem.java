package mtas.codec.util.collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import mtas.codec.util.DataCollector.MtasDataCollector;

/**
 * The Class MtasDataItem.
 *
 * @param <T> the generic type
 */
public abstract class MtasDataItem<T extends Number>
    implements Serializable, Comparable<MtasDataItem<T>> {

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

  /**
   * Instantiates a new mtas data item.
   *
   * @param sub the sub
   * @param statsItems the stats items
   * @param sortType the sort type
   * @param sortDirection the sort direction
   * @param errorNumber the error number
   * @param errorList the error list
   */
  public MtasDataItem(MtasDataCollector<?, ?> sub, TreeSet<String> statsItems,
      String sortType, String sortDirection, int errorNumber,
      HashMap<String, Integer> errorList) {
    this.sub = sub;
    this.statsItems = statsItems;
    this.sortType = sortType;
    this.sortDirection = sortDirection;
    this.errorNumber = errorNumber;
    this.errorList = errorList;
  }

  /**
   * Adds the.
   *
   * @param newItem the new item
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void add(MtasDataItem<T> newItem) throws IOException;

  /**
   * Rewrite.
   *
   * @return the map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract Map<String, Object> rewrite() throws IOException;

  /**
   * Gets the sub.
   *
   * @return the sub
   */
  public MtasDataCollector<?, ?> getSub() {
    return sub;
  }

}