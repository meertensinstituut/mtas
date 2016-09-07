package mtas.codec.tree;

import java.util.HashMap;
import mtas.codec.tree.MtasTree;
import mtas.codec.tree.MtasAVLTreeNode;

/**
 * The Class MtasAVLTree.
 */
public class MtasAVLTree extends MtasTree<MtasAVLTreeNode> {

  /** The index. */
  private final HashMap<String, MtasAVLTreeNode> index;

  /**
   * Instantiates a new mtas avl tree.
   *
   * @param singlePoint
   *          the single point
   * @param storePrefixId
   *          the store prefix id
   */
  public MtasAVLTree(boolean singlePoint, boolean storePrefixId) {
    super(singlePoint, storePrefixId);
    index = new HashMap<>();
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.tree.MtasTree#addTokenRangeEmpty(int, int)
   */
  @Override
  final protected void addRangeEmpty(int left, int right, int additionalId,
      long additionalRef) {
    String key = ((Integer) left).toString() + "_"
        + ((Integer) right).toString();
    if (index.containsKey(key)) {
      // do nothing (empty...)
    } else {
      addRange(left, right, additionalId, additionalRef, null, null);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.tree.MtasTree#addTokenSinglePoint(int, java.lang.Integer,
   * java.lang.Long)
   */
  @Override
  final protected void addSinglePoint(int position, int additionalId,
      long additionalRef, Integer id, Long ref) {
    addRange(position, position, additionalId, additionalRef, id, ref);
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.tree.MtasTree#addTokenRange(int, int, java.lang.Integer,
   * java.lang.Long)
   */
  @Override
  final protected void addRange(int left, int right, int additionalId,
      long additionalRef, Integer id, Long ref) {
    String key = ((Integer) left).toString() + "_"
        + ((Integer) right).toString();
    if (index.containsKey(key)) {
      index.get(key).addIdAndRef(id, ref, additionalId, additionalRef);
      return;
    }
    if (root == null) {
      root = new MtasAVLTreeNode(left, right, null);
      root.addIdAndRef(id, ref, additionalId, additionalRef);
      index.put(key, root);
    } else {
      MtasAVLTreeNode n = root;
      MtasAVLTreeNode parent;
      while (true) {
        parent = n;
        boolean goLeft = n.left > left;
        n = goLeft ? n.leftChild : n.rightChild;
        if (n == null) {
          if (goLeft) {
            parent.leftChild = new MtasAVLTreeNode(left, right, parent);
            updateMax(parent, parent.leftChild.max);
            parent.leftChild.addIdAndRef(id, ref, additionalId, additionalRef);
            index.put(key, parent.leftChild);
          } else {
            parent.rightChild = new MtasAVLTreeNode(left, right, parent);
            updateMax(parent, parent.rightChild.max);
            parent.rightChild.addIdAndRef(id, ref, additionalId, additionalRef);
            index.put(key, parent.rightChild);
          }
          rebalance(parent);
          break;
        }
      }
    }

  }

  /**
   * Update max.
   *
   * @param n
   *          the n
   * @param max
   *          the max
   */
  private void updateMax(MtasAVLTreeNode n, int max) {
    if (n != null) {
      if (n.max < max) {
        n.max = max;
        updateMax(n.parent, max);
      }
    }
  }

  /**
   * Rebalance.
   *
   * @param n
   *          the n
   */
  private void rebalance(MtasAVLTreeNode n) {
    setBalance(n);
    if (n.balance == -2) {
      if (height(n.leftChild.leftChild) >= height(n.leftChild.rightChild)) {
        n = rotateRight(n);
      } else {
        n = rotateLeftThenRight(n);
      }
    } else if (n.balance == 2) {
      if (height(n.rightChild.rightChild) >= height(n.rightChild.leftChild)) {
        n = rotateLeft(n);
      } else {
        n = rotateRightThenLeft(n);
      }
    }
    if (n.parent != null) {
      rebalance(n.parent);
    } else {
      root = n;
    }
  }

  /**
   * Rotate left.
   *
   * @param a
   *          the a
   * @return the mtas avl tree node
   */
  private MtasAVLTreeNode rotateLeft(MtasAVLTreeNode a) {
    MtasAVLTreeNode b = a.rightChild;
    b.parent = a.parent;
    a.rightChild = b.leftChild;
    if (a.rightChild != null) {
      a.rightChild.parent = a;
    }
    b.leftChild = a;
    a.parent = b;
    if (b.parent != null) {
      if ((b.parent.rightChild != null) && b.parent.rightChild.equals(a)) {
        b.parent.rightChild = b;
      } else {
        b.parent.leftChild = b;
      }
    }
    setMax(a);
    setMax(b);
    setBalance(a, b);
    return b;
  }

  /**
   * Rotate right.
   *
   * @param a
   *          the a
   * @return the mtas avl tree node
   */
  private MtasAVLTreeNode rotateRight(MtasAVLTreeNode a) {
    MtasAVLTreeNode b = a.leftChild;
    b.parent = a.parent;
    a.leftChild = b.rightChild;
    if (a.leftChild != null) {
      a.leftChild.parent = a;
    }
    b.rightChild = a;
    a.parent = b;
    if (b.parent != null) {
      if ((b.parent.rightChild != null) && b.parent.rightChild.equals(a)) {
        b.parent.rightChild = b;
      } else {
        b.parent.leftChild = b;
      }
    }
    setMax(a);
    setMax(b);
    setBalance(a, b);
    return b;
  }

  /**
   * Rotate left then right.
   *
   * @param n
   *          the n
   * @return the mtas avl tree node
   */
  private MtasAVLTreeNode rotateLeftThenRight(MtasAVLTreeNode n) {
    n.leftChild = rotateLeft(n.leftChild);
    return rotateRight(n);
  }

  /**
   * Rotate right then left.
   *
   * @param n
   *          the n
   * @return the mtas avl tree node
   */
  private MtasAVLTreeNode rotateRightThenLeft(MtasAVLTreeNode n) {
    n.rightChild = rotateRight(n.rightChild);
    return rotateLeft(n);
  }

  /**
   * Height.
   *
   * @param n
   *          the n
   * @return the int
   */
  private int height(MtasAVLTreeNode n) {
    if (n == null) {
      return -1;
    } else {
      return 1 + Math.max(height(n.leftChild), height(n.rightChild));
    }
  }

  /**
   * Sets the balance.
   *
   * @param nodes
   *          the new balance
   */
  private void setBalance(MtasAVLTreeNode... nodes) {
    for (MtasAVLTreeNode n : nodes) {
      n.balance = height(n.rightChild) - height(n.leftChild);
    }
  }

  /**
   * Sets the max.
   *
   * @param n
   *          the new max
   */
  private void setMax(MtasAVLTreeNode n) {
    n.max = n.right;
    if (n.leftChild != null) {
      n.max = Math.max(n.max, n.leftChild.max);
    }
    if (n.rightChild != null) {
      n.max = Math.max(n.max, n.rightChild.max);
    }
  }

}
