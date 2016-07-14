package mtas.codec.tree;

import java.util.ArrayList;

import mtas.codec.util.CodecSearchTree.MtasTreeHit;

/**
 * The Class IntervalTreeNode.
 *
 * @param <N> the number type
 */
abstract public class IntervalTreeNode<N extends IntervalTreeNode<N>> {

  /**
   * Self.
   *
   * @return the n
   */
  protected abstract N self();
  
    /** The left. */
    public int left;
    
    /** The right. */
    public int right;
    
    /** The max. */
    public int max;
    
    /** The min. */
    public int min;
    
    /** The left child. */
    public N leftChild;
    
    /** The right child. */
    public N rightChild;
    
    
    /** The lists. */
    public ArrayList<ArrayList<MtasTreeHit<?>>> lists;

    // node with start and end position
    /**
     * Instantiates a new interval tree node.
     *
     * @param left the left
     * @param right the right
     */
    public IntervalTreeNode(int left, int right) {
      this.left = left;
      this.right = right;
      min = left;
      max = right;
      lists = new ArrayList<ArrayList<MtasTreeHit<?>>>();
    }

    // add id to node
    /**
     * Adds the list.
     *
     * @param list the list
     */
    final public void addList(ArrayList<MtasTreeHit<?>> list) {
      if(list!=null) {
        lists.add(list);
      }
    }  
    
    

  }




