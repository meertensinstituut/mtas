package mtas.codec.tree;

public class IntervalRBTreeNode<T> extends IntervalTreeNode<T, IntervalRBTreeNode<T>> {
  static final int BLACK = 1;
  static final int RED = 0;

  public int color;
  public int n;

  // node with start and end position
  public IntervalRBTreeNode(int left, int right, int color, int n) {
    super(left, right);
    this.color = color;
    this.n = n;
  }
}
