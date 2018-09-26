package mtas.codec.tree;

import java.util.ArrayList;

import mtas.codec.util.CodecSearchTree.MtasTreeHit;

abstract public class IntervalTreeNode<T, N extends IntervalTreeNode<T, N>> {
  public int left;
  public int right;
  public int max;
  public int min;
  public N leftChild;
  public N rightChild;
  public ArrayList<ArrayList<MtasTreeHit<T>>> lists;

  // node with start and end position
  public IntervalTreeNode(int left, int right) {
    this.left = left;
    this.right = right;
    min = left;
    max = right;
    lists = new ArrayList<ArrayList<MtasTreeHit<T>>>();
  }

  // add id to node
  final public void addList(ArrayList<MtasTreeHit<T>> list) {
    if (list != null) {
      lists.add(list);
    }
  }
}
