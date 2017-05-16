package mtas.codec.tree;

import mtas.codec.tree.MtasTreeNode;

/**
 * The Class MtasAVLTreeNode.
 */
public class MtasAVLTreeNode extends MtasTreeNode<MtasAVLTreeNode> {

  /** The balance. */
  public int balance;

  /** The parent. */
  public MtasAVLTreeNode parent;

  // node with start and end position
  /**
   * Instantiates a new mtas AVL tree node.
   *
   * @param left the left
   * @param right the right
   * @param parent the parent
   */
  public MtasAVLTreeNode(int left, int right, MtasAVLTreeNode parent) {
    super(left, right);
    this.parent = parent;
  }

}
