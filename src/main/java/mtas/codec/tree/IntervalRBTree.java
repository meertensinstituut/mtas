package mtas.codec.tree;

import java.util.ArrayList;
import java.util.HashMap;

import mtas.codec.util.CodecSearchTree.MtasTreeHit;

public class IntervalRBTree<T> extends IntervalTree<T, IntervalRBTreeNode<T>> {
  private final HashMap<String, IntervalRBTreeNode<T>> index;

  public IntervalRBTree() {
    super();
    index = new HashMap<>();
  }

  public IntervalRBTree(ArrayList<IntervalTreeNodeData<T>> positionsHits) {
    this();
    for (IntervalTreeNodeData<T> positionsHit : positionsHits) {
      addRange(positionsHit.start, positionsHit.end, positionsHit.list);
    }
    close();
  }

  @Override
  final protected void addRangeEmpty(int left, int right) {
    String key = left + "_" + right;
    if (index.containsKey(key)) {
      // do nothing (empty...)
    } else {
      root = addRange(root, left, right, null);
      root.color = IntervalRBTreeNode.BLACK;
    }
  }

  @Override
  final protected void addSinglePoint(int position,
      ArrayList<MtasTreeHit<T>> list) {
    addRange(position, position, list);
  }

  @Override
  final protected void addRange(int left, int right,
      ArrayList<MtasTreeHit<T>> list) {
    String key = left + "_" + right;
    if (index.containsKey(key)) {
      index.get(key).addList(list);
    } else {
      root = addRange(root, left, right, list);
      root.color = IntervalRBTreeNode.BLACK;
    }
  }

  private IntervalRBTreeNode<T> addRange(IntervalRBTreeNode<T> n, Integer left,
      Integer right, ArrayList<MtasTreeHit<T>> list) {
    IntervalRBTreeNode<T> localN = n;
    if (localN == null) {
      String key = left.toString() + "_" + right.toString();
      localN = new IntervalRBTreeNode<>(left, right, IntervalRBTreeNode.RED, 1);
      localN.addList(list);
      index.put(key, localN);
    } else {
      if (left <= localN.left) {
        localN.leftChild = addRange(localN.leftChild, left, right, list);
        updateMaxMin(localN, localN.leftChild);
      } else {
        localN.rightChild = addRange(localN.rightChild, left, right, list);
        updateMaxMin(localN, localN.rightChild);
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
  private void flipColors(IntervalRBTreeNode<T> n) {
    // n must have opposite color of its two children
    assert (n != null) && (n.leftChild != null) && (n.rightChild != null);
    assert (!isRed(n) && isRed(n.leftChild) && isRed(n.rightChild))
        || (isRed(n) && !isRed(n.leftChild) && !isRed(n.rightChild));
    n.color ^= 1;
    n.leftChild.color ^= 1;
    n.rightChild.color ^= 1;
  }

  private boolean isRed(IntervalRBTreeNode<T> n) {
    if (n == null) {
      return false;
    }
    return n.color == IntervalRBTreeNode.RED;
  }

  private int size(IntervalRBTreeNode<T> n) {
    if (n == null)
      return 0;
    return n.n;
  }

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
