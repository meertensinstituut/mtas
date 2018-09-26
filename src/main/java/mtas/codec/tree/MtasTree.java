package mtas.codec.tree;

import mtas.analysis.token.MtasPosition;
import mtas.analysis.token.MtasToken;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

abstract public class MtasTree<N extends MtasTreeNode<N>> {
  final public static byte SINGLE_POSITION_TREE = 1;
  final public static byte STORE_ADDITIONAL_ID = 2;

  protected N root;
  private Boolean closed;

  protected Boolean singlePoint;
  protected Boolean storePrefixAndTermRef;

  public MtasTree(boolean singlePoint, boolean storePrefixAndTermRef) {
    root = null;
    closed = false;
    this.singlePoint = singlePoint;
    this.storePrefixAndTermRef = storePrefixAndTermRef;
  }

  final public void addIdFromDoc(Integer docId, Long reference) {
    if (!closed && (docId != null)) {
      addSinglePoint(docId, 0, 0, docId, reference);
    }
  }

  final public void addParentFromToken(MtasToken token) throws IOException {
    if (!closed && (token != null)) {
      if (token.checkParentId()) {
        addSinglePoint(token.getParentId(), token.getPrefixId(),
            token.getTermRef(), token.getId(), token.getTokenRef());
      }
    }
  }

  final public void addPositionAndObjectFromToken(MtasToken token)
      throws IOException {
    addPositionFromToken(token, token.getTokenRef());
  }

  // final public <T> void addPositionAndTermFromToken(MtasToken<T> token) {
  // addPositionFromToken(token, token.getTermRef());
  // }

  final private void addPositionFromToken(MtasToken token, Long ref)
      throws IOException {
    int prefixId = storePrefixAndTermRef ? token.getPrefixId() : 0;
    if (!closed && (token != null)) {
      if (token.checkPositionType(MtasPosition.POSITION_SINGLE)) {
        addSinglePoint(token.getPositionStart(), prefixId, token.getTermRef(),
            token.getId(), ref);
      } else if (token.checkPositionType(MtasPosition.POSITION_RANGE)) {
        addRange(token.getPositionStart(), token.getPositionEnd(), prefixId,
            token.getTermRef(), token.getId(), ref);
      } else if (token.checkPositionType(MtasPosition.POSITION_SET)) {
        // split set into minimum number of single points and ranges
        SortedMap<Integer, Integer> list = new TreeMap<>();
        int[] positions = token.getPositions();
        Integer lastPoint = null;
        Integer startPoint = null;
        for (int position : positions) {
          if (lastPoint == null) {
            startPoint = position;
            lastPoint = position;
          } else if ((position - lastPoint) != 1) {
            list.put(startPoint, lastPoint);
            startPoint = position;
          }
          lastPoint = position;
        }
        if (lastPoint != null) {
          list.put(startPoint, lastPoint);
        }
        for (Entry<Integer, Integer> entry : list.entrySet()) {
          if (entry.getKey().equals(entry.getValue())) {
            addSinglePoint(entry.getKey(), prefixId, token.getTermRef(),
                token.getId(), ref);
          } else {
            addRange(entry.getKey(), entry.getValue(), prefixId,
                token.getTermRef(), token.getId(), ref);
          }
        }
      }
    }
  }

  final public N close() {
    if (root == null) {
      addRangeEmpty(0, 0, 0, 0);
    }
    closed = true;
    return root;
  }

  abstract protected void addSinglePoint(int position, int additionalId,
      long additionalRef, Integer id, Long ref);

  abstract protected void addRange(int left, int right, int additionalId,
      long additionalRef, Integer id, Long ref);

  abstract protected void addRangeEmpty(int left, int right, int additionalId,
      long additionalRef);

  final public boolean isSinglePoint() {
    return singlePoint;
  }

  final public boolean isStorePrefixAndTermRef() {
    return storePrefixAndTermRef;
  }

  final public void printBalance() {
    printBalance(1, root);
  }

  final private void printBalance(Integer p, N n) {
    if (n != null) {
      printBalance((p + 1), n.leftChild);
      String format = "%" + (3 * p) + "s";
      System.out.print(String.format(format, ""));
      if (n.left == n.right) {
        System.out.println(
            "[" + n.left + "] (" + n.max + ") : " + n.ids.size() + " tokens");
      } else {
        System.out.println("[" + n.left + "-" + n.right + "] (" + n.max + ") : "
            + n.ids.size() + " tokens");
      }
      printBalance((p + 1), n.rightChild);
    }
  }
}
