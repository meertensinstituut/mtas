package mtas.codec.tree;

import java.util.ArrayList;
import java.util.HashMap;

import mtas.codec.util.CodecSearchTree.MtasTreeHit;

/**
 * The Class IntervalRBTree.
 *
 * @param <T>
 *          the generic type
 */
public class IntervalRBTree<T> extends IntervalTree<T, IntervalRBTreeNode<T>> {

  /** The index. */
  private final HashMap<String, IntervalRBTreeNode<T>> index;

  /**
   * Instantiates a new interval rb tree.
   */
  public IntervalRBTree() {
    super();
    index = new HashMap<>();
  }

  /**
   * Instantiates a new interval rb tree.
   *
   * @param positionsHits
   *          the positions hits
   */
  public IntervalRBTree(ArrayList<IntervalTreeNodeData<T>> positionsHits) {
    this();
    for (IntervalTreeNodeData<T> positionsHit : positionsHits) {
      addRange(positionsHit.start, positionsHit.end, positionsHit.list);
    }
    close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.tree.IntervalTree#addRangeEmpty(int, int)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.tree.IntervalTree#addSinglePoint(int, java.util.ArrayList)
   */
  @Override
  final protected void addSinglePoint(int position,
      ArrayList<MtasTreeHit<T>> list) {
    addRange(position, position, list);
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.codec.tree.IntervalTree#addRange(int, int, java.util.ArrayList)
   */
  @Override
  final protected void addRange(int left, int right,
      ArrayList<MtasTreeHit<T>> list) {
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
   * @param list
   *          the list
   * @return the interval rb tree node
   */
  private IntervalRBTreeNode<T> addRange(IntervalRBTreeNode<T> n, Integer left,
      Integer right, ArrayList<MtasTreeHit<T>> list) {
    if (n == null) {
      String key = left.toString() + "_" + right.toString();
      n = new IntervalRBTreeNode<T>(left, right, IntervalRBTreeNode.RED, 1);
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
   * Update max min.
   *
   * @param n
   *          the n
   * @param c
   *          the c
   */
  private void updateMaxMin(IntervalRBTreeNode<T> n, IntervalRBTreeNode<T> c) {
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
   * @return the interval rb tree node
   */
  private IntervalRBTreeNode<T> rotateRight(IntervalRBTreeNode<T> n) {
    assert (n != null) && isRed(n.leftChild);
    IntervalRBTreeNode<T> x = n.leftChild;
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
   * @return the interval rb tree node
   */
  private IntervalRBTreeNode<T> rotateLeft(IntervalRBTreeNode<T> n) {
    assert (n != null) && isRed(n.rightChild);
    IntervalRBTreeNode<T> x = n.rightChild;
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
  private void flipColors(IntervalRBTreeNode<T> n) {
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
  private boolean isRed(IntervalRBTreeNode<T> n) {
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
  private int size(IntervalRBTreeNode<T> n) {
    if (n == null)
      return 0;
    return n.n;
  }

  /**
   * Sets the max min.
   *
   * @param n
   *          the new max min
   */
  private void setMaxMin(IntervalRBTreeNode<T> n) {
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
