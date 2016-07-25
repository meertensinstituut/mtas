package mtas.codec.tree;

import java.util.ArrayList;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;

/**
 * The Class IntervalTree.
 *
 * @param <N> the number type
 */
abstract public class IntervalTree<T, N extends IntervalTreeNode<T, N>> {
  
  /** The current. */
  protected N root, current;
  
  
  /**
   * Instantiates a new interval tree.
   */
  public IntervalTree() {
    root = null;
  }
  
  /**
   * Close.
   *
   * @return the n
   */
  final public N close() {
    if(root==null) {
      addRangeEmpty(0,0);
    }
    return root;
  } 
  
  /**
   * Adds the single point.
   *
   * @param position the position
   * @param list the list
   */
  abstract protected void addSinglePoint(int position, ArrayList<MtasTreeHit<T>> list);
  
  /**
   * Adds the range.
   *
   * @param left the left
   * @param right the right
   * @param list the list
   */
  abstract protected void addRange(int left, int right, ArrayList<MtasTreeHit<T>> list);
  
  /**
   * Adds the range empty.
   *
   * @param left the left
   * @param right the right
   */
  abstract protected void addRangeEmpty(int left, int right);

  /**
   * Prints the balance.
   */
  final public void printBalance() {
    printBalance(1, root);
  }

  /**
   * Prints the balance.
   *
   * @param p the p
   * @param n the n
   */
  final private void printBalance(Integer p, N n) {    
    if(n!=null) {
      printBalance((p+1), n.leftChild);
      System.out.print(String.format("%"+(3*p)+"s", ""));
      if(n.left==n.right) {
        System.out.println("["+n.left+"] ("+n.max+") : "+n.lists.size()+" lists");
      } else {
        System.out.println("["+n.left+"-"+n.right+"] ("+n.max+") : "+n.lists.size()+" lists");        
      }
      printBalance((p+1), n.rightChild);
    }
  }
  
  /**
   * Gets the root.
   *
   * @return the root
   */
  final public N getRoot() {
    return root;
  }
  
  /**
   * Gets the current.
   *
   * @return the current
   */
  final public N getCurrent() {
    return current;
  }
  
  /**
   * Sets the current.
   *
   * @param node the new current
   */
  final public void setCurrent(N node) {
    current = node;
  }

}
