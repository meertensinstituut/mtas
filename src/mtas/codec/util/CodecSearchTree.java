package mtas.codec.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import mtas.codec.tree.IntervalTree;
import mtas.codec.tree.IntervalTreeNode;
import mtas.codec.tree.MtasTree;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;

import org.apache.lucene.store.IndexInput;

/**
 * The Class CodecSearchTree.
 */
public class CodecSearchTree {

  /**
   * Advance mtas tree.
   *
   * @param position the position
   * @param in the in
   * @param ref the ref
   * @param objectRefApproxOffset the object ref approx offset
   * @return the array list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static ArrayList<MtasTreeHit<?>> advanceMtasTree(int position,
      IndexInput in, long ref, long objectRefApproxOffset) throws IOException {
    ArrayList<MtasTreeHit<?>> list = new ArrayList<MtasTreeHit<?>>();
    ArrayList<MtasTreeItem> checkList = new ArrayList<MtasTreeItem>();
    AtomicBoolean isSinglePoint = new AtomicBoolean(false);
    AtomicBoolean isStoreAdditonalId = new AtomicBoolean(false);
    AtomicLong nodeRefApproxOffset = new AtomicLong(-1);
    checkList.add(getMtasTreeItem(ref, isSinglePoint, isStoreAdditonalId,
        nodeRefApproxOffset, in, objectRefApproxOffset));
    ArrayList<Long> history = new ArrayList<Long>();
    do {
      MtasTreeItem checkItem = checkList.remove(checkList.size() - 1);
      advanceMtasTree(checkItem, position, in, isSinglePoint,
          isStoreAdditonalId, objectRefApproxOffset, list, nodeRefApproxOffset,
          checkList);
      history.add(checkItem.ref);
      if (history.size() > 1000) {
        throw new IOException(
            "ADVANCE " + position + " " + checkList + "\n" + history);
      }
    } while (checkList.size() > 0);
    return list;
  }

  /**
   * Advance mtas tree.
   *
   * @param treeItem the tree item
   * @param position the position
   * @param in the in
   * @param isSinglePoint the is single point
   * @param isStoreAdditionalId the is store additional id
   * @param objectRefApproxOffset the object ref approx offset
   * @param list the list
   * @param nodeRefApproxOffset the node ref approx offset
   * @param checkList the check list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void advanceMtasTree(MtasTreeItem treeItem, int position,
      IndexInput in, AtomicBoolean isSinglePoint,
      AtomicBoolean isStoreAdditionalId, long objectRefApproxOffset,
      ArrayList<MtasTreeHit<?>> list, AtomicLong nodeRefApproxOffset,
      ArrayList<MtasTreeItem> checkList) throws IOException {
    if (position <= treeItem.max) {
      // check current node
      if (position <= treeItem.left) {
        if (list.size() > 0) {
          if (list.get(0).startPosition > treeItem.left) {
            list.clear();
          }
        }
        for (int i = 0; i < treeItem.objectRefs.length; i++) {
          list.add(new MtasTreeHit<>(treeItem.left, treeItem.right,
              treeItem.objectRefs[i],
              treeItem.objectIds == null ? 0 : treeItem.objectIds[i]));
        }
        // check leftChild
        if (!treeItem.leftChild.equals(treeItem.ref)) {
          MtasTreeItem treeItemLeft = getMtasTreeItem(treeItem.leftChild,
              isSinglePoint, isStoreAdditionalId, nodeRefApproxOffset, in,
              objectRefApproxOffset);
          if (position <= treeItemLeft.max) {
            checkList.add(treeItemLeft);
          }
        }
      } else {
        // check right
        if (position <= treeItem.max) {
          if (!treeItem.rightChild.equals(treeItem.ref)) {
            MtasTreeItem treeItemRight = getMtasTreeItem(treeItem.rightChild,
                isSinglePoint, isStoreAdditionalId, nodeRefApproxOffset, in,
                objectRefApproxOffset);
            if (position <= treeItemRight.max) {
              checkList.add(treeItemRight);
            }
          }
        }
      }
    }
  }

  /**
   * Search mtas tree.
   *
   * @param position the position
   * @param in the in
   * @param ref the ref
   * @param objectRefApproxOffset the object ref approx offset
   * @return the array list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static ArrayList<MtasTreeHit<?>> searchMtasTree(int position,
      IndexInput in, long ref, long objectRefApproxOffset) throws IOException {
    return searchMtasTree(position, position, in, ref, objectRefApproxOffset);
  }

  /**
   * Search mtas tree.
   *
   * @param startPosition the start position
   * @param endPosition the end position
   * @param in the in
   * @param ref the ref
   * @param objectRefApproxOffset the object ref approx offset
   * @return the array list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static ArrayList<MtasTreeHit<?>> searchMtasTree(int startPosition,
      int endPosition, IndexInput in, long ref, long objectRefApproxOffset)
      throws IOException {
    int boundary = 1000 + 10 * (endPosition - startPosition);
    ArrayList<MtasTreeHit<?>> list = new ArrayList<MtasTreeHit<?>>();
    ArrayList<MtasTreeItem> checkList = new ArrayList<MtasTreeItem>();
    AtomicBoolean isSinglePoint = new AtomicBoolean(false);
    AtomicBoolean isStoreAdditionalId = new AtomicBoolean(false);
    AtomicLong nodeRefApproxOffset = new AtomicLong(-1);
    checkList.add(getMtasTreeItem(ref, isSinglePoint, isStoreAdditionalId,
        nodeRefApproxOffset, in, objectRefApproxOffset));
    ArrayList<Long> history = new ArrayList<Long>();
    do {
      MtasTreeItem checkItem = checkList.remove(checkList.size() - 1);
      searchMtasTree(checkItem, startPosition, endPosition, in, isSinglePoint,
          isStoreAdditionalId, objectRefApproxOffset, list, nodeRefApproxOffset,
          checkList);
      history.add(checkItem.ref);
      if (history.size() > boundary) {
        throw new IOException("Too many items collected from tree");
      }
    } while (checkList.size() > 0);
    return list;
  }

  /**
   * Search mtas tree.
   *
   * @param treeItem the tree item
   * @param startPosition the start position
   * @param endPosition the end position
   * @param in the in
   * @param isSinglePoint the is single point
   * @param isStoreAdditionalId the is store additional id
   * @param objectRefApproxOffset the object ref approx offset
   * @param list the list
   * @param nodeRefApproxOffset the node ref approx offset
   * @param checkList the check list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void searchMtasTree(MtasTreeItem treeItem, int startPosition,
      int endPosition, IndexInput in, AtomicBoolean isSinglePoint,
      AtomicBoolean isStoreAdditionalId, long objectRefApproxOffset,
      ArrayList<MtasTreeHit<?>> list, AtomicLong nodeRefApproxOffset,
      ArrayList<MtasTreeItem> checkList) throws IOException {
    if (startPosition <= treeItem.max) {
      // match current node
      if ((endPosition >= treeItem.left) && (startPosition <= treeItem.right)) {
        for (int i = 0; i < treeItem.objectRefs.length; i++) {
          list.add(new MtasTreeHit<>(treeItem.left, treeItem.right,
              treeItem.objectRefs[i],
              treeItem.objectIds == null ? 0 : treeItem.objectIds[i]));
        }
      }
      // check leftChild
      if (!treeItem.leftChild.equals(treeItem.ref)) {
        MtasTreeItem treeItemLeft = getMtasTreeItem(treeItem.leftChild,
            isSinglePoint, isStoreAdditionalId, nodeRefApproxOffset, in,
            objectRefApproxOffset);
        if (treeItemLeft.max >= startPosition) {
          checkList.add(treeItemLeft);
        }
      }
      // check rightChild
      if (treeItem.left < endPosition) {
        if (!treeItem.rightChild.equals(treeItem.ref)) {
          MtasTreeItem treeItemRight = getMtasTreeItem(treeItem.rightChild,
              isSinglePoint, isStoreAdditionalId, nodeRefApproxOffset, in,
              objectRefApproxOffset);
          if ((treeItemRight.left >= endPosition)
              || (treeItemRight.max >= startPosition)) {
            checkList.add(treeItemRight);
          }
        }
      }
    }
  }

  /**
   * Gets the mtas tree item.
   *
   * @param ref the ref
   * @param isSinglePoint the is single point
   * @param isStoreAdditionalId the is store additional id
   * @param nodeRefApproxOffset the node ref approx offset
   * @param in the in
   * @param objectRefApproxOffset the object ref approx offset
   * @return the mtas tree item
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static MtasTreeItem getMtasTreeItem(Long ref,
      AtomicBoolean isSinglePoint, AtomicBoolean isStoreAdditionalId,
      AtomicLong nodeRefApproxOffset, IndexInput in, long objectRefApproxOffset)
      throws IOException {
    Boolean isRoot = false;
    if (nodeRefApproxOffset.get() < 0) {
      isRoot = true;
    }
    in.seek(ref);
    if (isRoot) {
      nodeRefApproxOffset.set(in.readVLong());
      Byte flag = in.readByte();
      if ((flag
          & MtasTree.SINGLE_POSITION_TREE) == MtasTree.SINGLE_POSITION_TREE) {
        isSinglePoint.set(true);
      }
      if ((flag
          & MtasTree.STORE_ADDITIONAL_ID) == MtasTree.STORE_ADDITIONAL_ID) {
        isStoreAdditionalId.set(true);
      }
    }
    int left = in.readVInt();
    int right = in.readVInt();
    int max = in.readVInt();
    Long leftChild = in.readVLong() + nodeRefApproxOffset.get();
    Long rightChild = in.readVLong() + nodeRefApproxOffset.get();
    int size = 1;
    if (!isSinglePoint.get()) {
      size = in.readVInt();
    }
    // initialize
    long[] objectRefs = new long[size];
    int[] objectIds = null;
    // get first
    long objectRef = in.readVLong();
    long objectRefPrevious = objectRef + objectRefApproxOffset;
    objectRefs[0] = objectRefPrevious;
    if (isStoreAdditionalId.get()) {
      objectIds = new int[size];
      objectIds[0] = in.readVInt();
    }
    // get others
    for (int t = 1; t < size; t++) {
      objectRef = objectRefPrevious + in.readVLong();
      objectRefs[t] = objectRef;
      objectRefPrevious = objectRef;
      if (isStoreAdditionalId.get()) {
        objectIds[t] = in.readVInt();
      }
    }
    return new MtasTreeItem(left, right, max, objectRefs, objectIds, ref,
        leftChild, rightChild);
  }

  /**
   * The Class MtasTreeItem.
   */
  private static class MtasTreeItem {
    
    /** The max. */
    public int left, right, max;
    
    /** The object refs. */
    public long[] objectRefs;
    
    /** The object ids. */
    public int[] objectIds;
    
    /** The right child. */
    public Long ref, leftChild, rightChild;

    /**
     * Instantiates a new mtas tree item.
     *
     * @param left the left
     * @param right the right
     * @param max the max
     * @param objectRefs the object refs
     * @param objectIds the object ids
     * @param ref the ref
     * @param leftChild the left child
     * @param rightChild the right child
     */
    public MtasTreeItem(int left, int right, int max, long[] objectRefs,
        int[] objectIds, Long ref, Long leftChild, Long rightChild) {
      this.left = left;
      this.right = right;
      this.max = max;
      this.objectRefs = objectRefs;
      this.objectIds = objectIds;
      this.ref = ref;
      this.leftChild = leftChild;
      this.rightChild = rightChild;
    }
  }

  /**
   * The Class MtasTreeHit.
   *
   * @param <T> the generic type
   */
  public static class MtasTreeHit<T> {
    
    /** The start position. */
    public int startPosition;
    
    /** The end position. */
    public int endPosition;
    
    /** The ref. */
    public long ref;
    
    /** The additional id. */
    public int additionalId;
    
    /** The data. */
    public T data;

    /**
     * Instantiates a new mtas tree hit.
     *
     * @param startPosition the start position
     * @param endPosition the end position
     * @param ref the ref
     * @param additionalId the additional id
     */
    public MtasTreeHit(int startPosition, int endPosition, long ref,
        int additionalId) {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
      this.ref = ref;
      this.additionalId = additionalId;
    }

    /**
     * Instantiates a new mtas tree hit.
     *
     * @param startPosition the start position
     * @param endPosition the end position
     * @param ref the ref
     * @param additionalId the additional id
     * @param data the data
     */
    public MtasTreeHit(int startPosition, int endPosition, long ref,
        int additionalId, T data) {
      this(startPosition, endPosition, ref, additionalId);
      this.data = data;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return "hit[" + startPosition + "," + endPosition + "," + ref + ","
          + additionalId + "] - " + data;
    }
  }

  /**
   * Search mtas tree with interval tree.
   *
   * @param additionalIds the additional ids
   * @param intervalTree the interval tree
   * @param in the in
   * @param ref the ref
   * @param objectRefApproxOffset the object ref approx offset
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void searchMtasTreeWithIntervalTree(
      Collection<Integer> additionalIds, IntervalTree<?> intervalTree, IndexInput in, long ref,
      long objectRefApproxOffset) throws IOException {
    ArrayList<IntervalItem> checkList = new ArrayList<IntervalItem>();
    AtomicBoolean isSinglePoint = new AtomicBoolean(false);
    AtomicBoolean isStoreAdditionalId = new AtomicBoolean(false);
    AtomicLong nodeRefApproxOffset = new AtomicLong(-1);
    checkList.add(new IntervalItem(
        getMtasTreeItem(ref, isSinglePoint, isStoreAdditionalId,
            nodeRefApproxOffset, in, objectRefApproxOffset),
        intervalTree.getRoot()));
    IntervalTreeNode<?> intervalTreeNode = intervalTree.getRoot();
    do {
      IntervalItem checkItem = checkList.remove(checkList.size() - 1);
      searchMtasTreeWithIntervalTree(additionalIds, checkItem, in, isSinglePoint,
          isStoreAdditionalId, objectRefApproxOffset, nodeRefApproxOffset,
          checkList);
    } while (checkList.size() > 0);
  }

  /**
   * Search mtas tree with interval tree.
   *
   * @param additionalIds the additional ids
   * @param checkItem the check item
   * @param in the in
   * @param isSinglePoint the is single point
   * @param isStoreAdditionalId the is store additional id
   * @param objectRefApproxOffset the object ref approx offset
   * @param nodeRefApproxOffset the node ref approx offset
   * @param checkList the check list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void searchMtasTreeWithIntervalTree(Collection<Integer> additionalIds, IntervalItem checkItem,
      IndexInput in, AtomicBoolean isSinglePoint,
      AtomicBoolean isStoreAdditionalId, long objectRefApproxOffset,
      AtomicLong nodeRefApproxOffset, ArrayList<IntervalItem> checkList)
      throws IOException {
    MtasTreeItem treeItem = checkItem.mtasTreeItem;
    IntervalTreeNode<?> intervalTreeNode = checkItem.intervalTreeNode;
    if (intervalTreeNode.min <= treeItem.max) {
      // advance intervalTree
      while (intervalTreeNode.left > treeItem.max) {
        if (intervalTreeNode.rightChild == null) {
          if (intervalTreeNode.leftChild == null) {
            return;
          } else {
            intervalTreeNode = intervalTreeNode.leftChild;
          }
        } else if (intervalTreeNode.leftChild == null) {
          intervalTreeNode = intervalTreeNode.rightChild;
        } else {
          if (intervalTreeNode.rightChild.min > treeItem.max) {
            intervalTreeNode = intervalTreeNode.leftChild;
          } else {
            break;
          }
        }
      }
      // find intervals matching current node
      searchMtasTreeItemWithIntervalTree(additionalIds, treeItem, intervalTreeNode);
      // check leftChild
      if (!treeItem.leftChild.equals(treeItem.ref)) {
        MtasTreeItem treeItemLeft = getMtasTreeItem(treeItem.leftChild,
            isSinglePoint, isStoreAdditionalId, nodeRefApproxOffset, in,
            objectRefApproxOffset);
        checkList.add(new IntervalItem(treeItemLeft, intervalTreeNode));
      }
      // check rightChild
      if (!treeItem.rightChild.equals(treeItem.ref)) {
        MtasTreeItem treeItemRight = getMtasTreeItem(treeItem.rightChild,
            isSinglePoint, isStoreAdditionalId, nodeRefApproxOffset, in,
            objectRefApproxOffset);
        checkList.add(new IntervalItem(treeItemRight, intervalTreeNode));
      }
    }
  }

  /**
   * Search mtas tree item with interval tree.
   *
   * @param additionalIds the additional ids
   * @param treeItem the tree item
   * @param intervalTreeNode the interval tree node
   */
  private static void searchMtasTreeItemWithIntervalTree(Collection<Integer> additionalIds, MtasTreeItem treeItem,
      IntervalTreeNode<?> intervalTreeNode) {
    ArrayList<IntervalTreeNode<?>> checkList = new ArrayList<IntervalTreeNode<?>>();
    checkList.add(intervalTreeNode);
    do {
      IntervalTreeNode<?> checkItem = checkList.remove(checkList.size() - 1);
      searchMtasTreeItemWithIntervalTree(additionalIds, checkItem, treeItem.left,
          treeItem.right, treeItem.objectRefs, treeItem.objectIds, checkList);
    } while (checkList.size() > 0);
  }

  /**
   * Search mtas tree item with interval tree.
   *
   * @param requiredAdditionalIds the required additional ids
   * @param intervalTreeItem the interval tree item
   * @param startPosition the start position
   * @param endPosition the end position
   * @param refs the refs
   * @param additionalIds the additional ids
   * @param checkList the check list
   */
  private static void searchMtasTreeItemWithIntervalTree(Collection<Integer> requiredAdditionalIds,
      IntervalTreeNode<?> intervalTreeItem, int startPosition, int endPosition,
      long[] refs, int[] additionalIds,
      ArrayList<IntervalTreeNode<?>> checkList) {
    if (startPosition <= intervalTreeItem.max) {
      // match current node
      if ((endPosition >= intervalTreeItem.left)
          && (startPosition <= intervalTreeItem.right)) {
        //System.out.print("[" + startPosition + "-" + endPosition + "] ");
        if(requiredAdditionalIds==null || additionalIds==null) {
          for (int i = 0; i < refs.length; i++) {
            MtasTreeHit<?> hit = new MtasTreeHit<String>(startPosition,
                endPosition, refs[i], 0, null);
            for (ArrayList<MtasTreeHit<?>> list : intervalTreeItem.lists) {
              list.add(hit);
            }
          }
        } else {
          for (int i = 0; i < refs.length; i++) {
            //if(requiredAdditionalIds.contains(additionalIds[i])) {
              MtasTreeHit<?> hit = new MtasTreeHit<String>(startPosition,
                  endPosition, refs[i], additionalIds[i], null);
              for (ArrayList<MtasTreeHit<?>> list : intervalTreeItem.lists) {
                list.add(hit);
              }
            //}  
          }
        }  
      }
      // check leftChild
      if (intervalTreeItem.leftChild != null) {
        IntervalTreeNode treeItemLeft = intervalTreeItem.leftChild;
        if (treeItemLeft.max >= startPosition) {
          checkList.add(treeItemLeft);
        }
      }
      // check rightChild
      if (intervalTreeItem.left < endPosition) {
        if (intervalTreeItem.rightChild != null) {
          IntervalTreeNode treeItemRight = intervalTreeItem.rightChild;
          if ((treeItemRight.left >= endPosition)
              || (treeItemRight.max >= startPosition)) {
            checkList.add(treeItemRight);
          }
        }
      }
    }

  }

  /**
   * The Class IntervalItem.
   */
  private static class IntervalItem {
    
    /** The mtas tree item. */
    public MtasTreeItem mtasTreeItem;
    
    /** The interval tree node. */
    public IntervalTreeNode<?> intervalTreeNode;

    /**
     * Instantiates a new interval item.
     *
     * @param mtasTreeItem the mtas tree item
     * @param intervalTreeNode the interval tree node
     */
    public IntervalItem(MtasTreeItem mtasTreeItem,
        IntervalTreeNode<?> intervalTreeNode) {
      this.mtasTreeItem = mtasTreeItem;
      this.intervalTreeNode = intervalTreeNode;
    }
  }

}
