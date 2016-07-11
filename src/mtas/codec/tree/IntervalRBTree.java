package mtas.codec.tree;

import java.util.ArrayList;
import java.util.HashMap;

import mtas.codec.util.CodecSearchTree.MtasTreeHit;

public class IntervalRBTree extends IntervalTree<IntervalRBTreeNode> {

  /** The index. */
  private final HashMap<String, IntervalRBTreeNode> index;

  public IntervalRBTree() {
    super();
    index = new HashMap<>();
  }

  public IntervalRBTree(ArrayList<IntervalTreeNodeData> positionsHits) {
    this();
    for (IntervalTreeNodeData positionsHit : positionsHits) {
      addRange(positionsHit.start, positionsHit.end,
          positionsHit.list);
    }
    close();
  }

  @Override
  final protected void addRangeEmpty(int left, int right) {
    String key = ((Integer) left).toString() + "_"
        + ((Integer) right).toString();
    if (index.containsKey(key)) {
      // do nothing (empty...)
    } else {
      root = addRange(root, left, right, null);
      root.color = IntervalRBTreeNode.BLACK;
    }
  }

  @Override
  final protected void addSinglePoint(int position, ArrayList<MtasTreeHit<?>> list) {
    addRange(position, position, list);
  }

  @Override
  final protected void addRange(int left, int right, ArrayList<MtasTreeHit<?>> list) {
    String key = ((Integer) left).toString() + "_"
        + ((Integer) right).toString();
    if (index.containsKey(key)) {
      index.get(key).addList(list);
    } else {
      root = addRange(root, left, right, list);
      root.color = IntervalRBTreeNode.BLACK;
    }
  }

  /**
   * Adds the range.
   *
   * @param n
   *          the n
   * @param left
   *          the left
   * @param right
   *          the right
   * @param id
   *          the id
   * @param ref
   *          the ref
   * @return the mtas rb tree node
   */
  private IntervalRBTreeNode addRange(IntervalRBTreeNode n, Integer left,
      Integer right, ArrayList<MtasTreeHit<?>> list) {
    if (n == null) {
      String key = left.toString() + "_" + right.toString();
      n = new IntervalRBTreeNode(left, right, IntervalRBTreeNode.RED, 1);
      n.addList(list);
      index.put(key, n);
    } else {
      if (left <= n.left) {
        n.leftChild = addRange(n.leftChild, left, right, list);
        updateMaxMin(n, n.leftChild);
      } else {
        n.rightChild = addRange(n.rightChild, left, right, list);
        updateMaxMin(n, n.rightChild);
      }
      if (isRed(n.rightChild) && !isRed(n.leftChild)) {
        n = rotateLeft(n);
      }
      if (isRed(n.leftChild) && isRed(n.leftChild.leftChild)) {
        n = rotateRight(n);
      }
      if (isRed(n.leftChild) && isRed(n.rightChild)) {
        flipColors(n);
      }
      n.n = size(n.leftChild) + size(n.rightChild) + 1;
    }
    return n;
  }

  /**
   * Update max.
   *
   * @param n
   *          the n
   * @param c
   *          the c
   */
  private void updateMaxMin(IntervalRBTreeNode n, IntervalRBTreeNode c) {
    if (c != null) {
      if (n.max < c.max) {
        n.max = c.max;
      }
      if (n.min > c.min) {
        n.min = c.min;
      }
    }
  }

  // make a left-leaning link lean to the right
  /**
   * Rotate right.
   *
   * @param n
   *          the n
   * @return the mtas rb tree node
   */
  private IntervalRBTreeNode rotateRight(IntervalRBTreeNode n) {
    assert (n != null) && isRed(n.leftChild);
    IntervalRBTreeNode x = n.leftChild;
    n.leftChild = x.rightChild;
    x.rightChild = n;
    x.color = x.rightChild.color;
    x.rightChild.color = IntervalRBTreeNode.RED;
    x.n = n.n;
    n.n = size(n.leftChild) + size(n.rightChild) + 1;
    setMaxMin(n);
    setMaxMin(x);
    return x;
  }

  // make a right-leaning link lean to the left
  /**
   * Rotate left.
   *
   * @param n
   *          the n
   * @return the mtas rb tree node
   */
  private IntervalRBTreeNode rotateLeft(IntervalRBTreeNode n) {
    assert (n != null) && isRed(n.rightChild);
    IntervalRBTreeNode x = n.rightChild;
    n.rightChild = x.leftChild;
    x.leftChild = n;
    x.color = x.leftChild.color;
    x.leftChild.color = IntervalRBTreeNode.RED;
    x.n = n.n;
    n.n = size(n.leftChild) + size(n.rightChild) + 1;
    setMaxMin(n);
    setMaxMin(x);
    return x;
  }

  // flip the colors of a node and its two children
  /**
   * Flip colors.
   *
   * @param n
   *          the n
   */
  private void flipColors(IntervalRBTreeNode n) {
    // n must have opposite color of its two children
    assert (n != null) && (n.leftChild != null) && (n.rightChild != null);
    assert (!isRed(n) && isRed(n.leftChild) && isRed(n.rightChild))
        || (isRed(n) && !isRed(n.leftChild) && !isRed(n.rightChild));
    n.color ^= 1;
    n.leftChild.color ^= 1;
    n.rightChild.color ^= 1;
  }

  /**
   * Checks if is red.
   *
   * @param n
   *          the n
   * @return true, if is red
   */
  private boolean isRed(IntervalRBTreeNode n) {
    if (n == null) {
      return false;
    }
    return n.color == IntervalRBTreeNode.RED;
  }

  /**
   * Size.
   *
   * @param n
   *          the n
   * @return the int
   */
  private int size(IntervalRBTreeNode n) {
    if (n == null)
      return 0;
    return n.n;
  }

  /**
   * Sets the max.
   *
   * @param n
   *          the new max
   */
  private void setMaxMin(IntervalRBTreeNode n) {
    n.min = n.left;
    n.max = n.right;
    if (n.leftChild != null) {
      n.min = Math.min(n.min, n.leftChild.min);
      n.max = Math.max(n.max, n.leftChild.max);
    }
    if (n.rightChild != null) {
      n.min = Math.min(n.min, n.rightChild.min);
      n.max = Math.max(n.max, n.rightChild.max);
    }
  }

}
