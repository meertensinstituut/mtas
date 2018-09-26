package mtas.codec.tree;

import java.util.HashMap;

abstract public class MtasTreeNode<N extends MtasTreeNode<N>> {
  public int left;
  public int right;
  public int max;

  public N leftChild;
  public N rightChild;

  public HashMap<Integer, MtasTreeNodeId> ids;

  /**
   * node with start and end position
   */
  public MtasTreeNode(int left, int right) {
    this.left = left;
    this.right = right;
    this.max = right;
    this.ids = new HashMap<Integer, MtasTreeNodeId>();
  }

  /**
   * add id to node
   */
  final public void addIdAndRef(Integer id, Long ref, int additionalId,
      long additionalRef) {
    if (id != null) {
      MtasTreeNodeId tnId = new MtasTreeNodeId(ref, additionalId,
          additionalRef);
      ids.put(id, tnId);
    }
  }

}
