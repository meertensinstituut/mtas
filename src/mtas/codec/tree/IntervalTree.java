package mtas.codec.tree;

import java.util.TreeMap;
import java.util.TreeSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;

import mtas.analysis.token.MtasPosition;
import mtas.analysis.token.MtasToken;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;

/**
 * The Class MtasTree.
 *
 * @param <N> the number type
 */
abstract public class IntervalTree<N extends IntervalTreeNode<N>> {
  
  /** The root. */
  protected N root, current;
  
  /** The closed. */
  private Boolean closed;
  
  public IntervalTree() {
    root = null;
    closed = false; 
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
    closed = true;
    return root;
  } 
  
  /**
   * Adds the token single point.
   *
   * @param position the position
   * @param id the id
   * @param ref the ref
   */
  abstract protected void addSinglePoint(int position, ArrayList<MtasTreeHit<?>> list);
  
  /**
   * Adds the token range.
   *
   * @param left the left
   * @param right the right
   * @param id the id
   * @param ref the ref
   */
  abstract protected void addRange(int left, int right, ArrayList<MtasTreeHit<?>> list);
  
  /**
   * Adds the token range empty.
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
  
  final public N getRoot() {
    return root;
  }
  
  final public N getCurrent() {
    return current;
  }
  
  final public void setCurrent(N node) {
    current = node;
  }

}
