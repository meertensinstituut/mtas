package mtas.codec.tree;

import java.util.ArrayList;

import mtas.codec.util.CodecSearchTree.MtasTreeHit;

/**
 * The Class IntervalTreeNodeData.
 *
 * @param <T> the generic type
 */
public class IntervalTreeNodeData<T> {
  
  /** The hit end. */
  public int start, end, hitStart, hitEnd;
  
  /** The list. */
  public ArrayList<MtasTreeHit<T>> list;
 
  /**
   * Instantiates a new interval tree node data.
   *
   * @param start the start
   * @param end the end
   * @param hitStart the hit start
   * @param hitEnd the hit end
   */
  public IntervalTreeNodeData(int start, int end, int hitStart, int hitEnd) {
    this.start = start;
    this.end = end;
    this.hitStart = hitStart;
    this.hitEnd = hitEnd;
    list = new ArrayList<MtasTreeHit<T>>();
  }
  
}
