package mtas.codec.tree;

import mtas.codec.tree.MtasTreeNode;

public class MtasRBTreeNode extends MtasTreeNode<MtasRBTreeNode> {
  static final int BLACK = 1;
  static final int RED = 0;

  public int color;
  public int n;

  // node with start and end position
  public MtasRBTreeNode(int left, int right, int color, int n) {
    super(left, right);
    this.color = color;
    this.n = n;
  }
}
