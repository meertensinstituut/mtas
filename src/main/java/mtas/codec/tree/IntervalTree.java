package mtas.codec.tree;

import java.util.ArrayList;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;

abstract public class IntervalTree<T, N extends IntervalTreeNode<T, N>> {
  protected N root;
  protected N current;

  public IntervalTree() {
    root = null;
  }

  final public N close() {
    if (root == null) {
      addRangeEmpty(0, 0);
    }
    return root;
  }

  abstract protected void addSinglePoint(int position,
      ArrayList<MtasTreeHit<T>> list);

  abstract protected void addRange(int left, int right,
      ArrayList<MtasTreeHit<T>> list);

  abstract protected void addRangeEmpty(int left, int right);

  @Override
  public String toString() {
    return printBalance(1, root);
  }

  final private String printBalance(Integer p, N n) {
    StringBuilder text = new StringBuilder();
    if (n != null) {
      text.append(printBalance((p + 1), n.leftChild));
      String format = "%" + (3 * p) + "s";
      text.append(String.format(format, ""));
      if (n.left == n.right) {
        text.append("[" + n.left + "] (" + n.max + ") : " + n.lists.size()
            + " lists\n");
      } else {
        text.append("[" + n.left + "-" + n.right + "] (" + n.max + ") : "
            + n.lists.size() + " lists\n");
      }
      text.append(printBalance((p + 1), n.rightChild));
    }
    return text.toString();
  }

  final public N getRoot() {
    return root;
  }

  final public N getCurrent() {
    return current;
  }

  final public void setCurrent(N node) {
    current = node;
  }
}
