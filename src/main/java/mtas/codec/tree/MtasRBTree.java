package mtas.codec.tree;

import java.util.HashMap;
import mtas.codec.tree.MtasTree;
import mtas.codec.tree.MtasRBTreeNode;

public class MtasRBTree extends MtasTree<MtasRBTreeNode> {
  private final HashMap<String, MtasRBTreeNode> index;

  public MtasRBTree(boolean singlePoint, boolean storePrefixId) {
    super(singlePoint, storePrefixId);
    index = new HashMap<>();
  }

  @Override
  final protected void addRangeEmpty(int left, int right, int additionalId,
      long additionalRef) {
    String key = left + "_" + right;
    if (index.containsKey(key)) {
      // do nothing (empty...)
    } else {
      root = addRange(root, left, right, additionalId, additionalRef, null,
          null);
      root.color = MtasRBTreeNode.BLACK;
    }
  }

  @Override
  final protected void addSinglePoint(int position, int additionalId,
      long additionalRef, Integer id, Long ref) {
    addRange(position, position, additionalId, additionalRef, id, ref);
  }

  @Override
  final protected void addRange(int left, int right, int additionalId,
      long additionalRef, Integer id, Long ref) {
    String key = left + "_" + right;
    if (index.containsKey(key)) {
      index.get(key).addIdAndRef(id, ref, additionalId, additionalRef);
    } else {
      root = addRange(root, left, right, additionalId, additionalRef, id, ref);
      root.color = MtasRBTreeNode.BLACK;
    }
  }

  private MtasRBTreeNode addRange(MtasRBTreeNode n, Integer left, Integer right,
      int additionalId, long additionalRef, Integer id, Long ref) {
    MtasRBTreeNode localN = n;
    if (localN == null) {
      String key = left.toString() + "_" + right.toString();
      localN = new MtasRBTreeNode(left, right, MtasRBTreeNode.RED, 1);
      localN.addIdAndRef(id, ref, additionalId, additionalRef);
      index.put(key, localN);
    } else {
      if (left <= localN.left) {
        localN.leftChild = addRange(localN.leftChild, left, right, additionalId,
            additionalRef, id, ref);
        updateMax(localN, localN.leftChild);
      } else {
        localN.rightChild = addRange(localN.rightChild, left, right,
            additionalId, additionalRef, id, ref);
        updateMax(localN, localN.rightChild);
      }
      if (isRed(localN.rightChild) && !isRed(localN.leftChild)) {
        localN = rotateLeft(localN);
      }
      if (isRed(localN.leftChild) && isRed(localN.leftChild.leftChild)) {
        localN = rotateRight(localN);
      }
      if (isRed(localN.leftChild) && isRed(localN.rightChild)) {
        flipColors(localN);
      }
      localN.n = size(localN.leftChild) + size(localN.rightChild) + 1;
    }
    return localN;
  }

  private void updateMax(MtasRBTreeNode n, MtasRBTreeNode c) {
    if (c != null) {
      if (n.max < c.max) {
        n.max = c.max;
      }
    }
  }

  // make a left-leaning link lean to the right
  private MtasRBTreeNode rotateRight(MtasRBTreeNode n) {
    assert (n != null) && isRed(n.leftChild);
    MtasRBTreeNode x = n.leftChild;
    n.leftChild = x.rightChild;
    x.rightChild = n;
    x.color = x.rightChild.color;
    x.rightChild.color = MtasRBTreeNode.RED;
    x.n = n.n;
    n.n = size(n.leftChild) + size(n.rightChild) + 1;
    setMax(n);
    setMax(x);
    return x;
  }

  // make a right-leaning link lean to the left
  private MtasRBTreeNode rotateLeft(MtasRBTreeNode n) {
    assert (n != null) && isRed(n.rightChild);
    MtasRBTreeNode x = n.rightChild;
    n.rightChild = x.leftChild;
    x.leftChild = n;
    x.color = x.leftChild.color;
    x.leftChild.color = MtasRBTreeNode.RED;
    x.n = n.n;
    n.n = size(n.leftChild) + size(n.rightChild) + 1;
    setMax(n);
    setMax(x);
    return x;
  }

  // flip the colors of a node and its two children
  private void flipColors(MtasRBTreeNode n) {
    // n must have opposite color of its two children
    assert (n != null) && (n.leftChild != null) && (n.rightChild != null);
    assert (!isRed(n) && isRed(n.leftChild) && isRed(n.rightChild))
        || (isRed(n) && !isRed(n.leftChild) && !isRed(n.rightChild));
    n.color ^= 1;
    n.leftChild.color ^= 1;
    n.rightChild.color ^= 1;
  }

  private boolean isRed(MtasRBTreeNode n) {
    if (n == null) {
      return false;
    }
    return n.color == MtasRBTreeNode.RED;
  }

  private int size(MtasRBTreeNode n) {
    if (n == null)
      return 0;
    return n.n;
  }

  private void setMax(MtasRBTreeNode n) {
    n.max = n.right;
    if (n.leftChild != null) {
      n.max = Math.max(n.max, n.leftChild.max);
    }
    if (n.rightChild != null) {
      n.max = Math.max(n.max, n.rightChild.max);
    }
  }
}
