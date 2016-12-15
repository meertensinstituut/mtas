package mtas.search.spans.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.lucene.search.spans.Spans;

/**
 * The Class MtasIgnoreItem.
 */
public class MtasIgnoreItem {
  
  /** The Constant DEFAULT_MAXIMUM_IGNORE_LENGTH. */
  static final int DEFAULT_MAXIMUM_IGNORE_LENGTH = 10;

  /** The ignore spans. */
  Spans ignoreSpans;
  
  /** The current doc id. */
  int currentDocId;
  
  /** The current position. */
  int currentPosition;
  
  /** The minimum position. */
  int minimumPosition;
  
  /** The maximum ignore length. */
  int maximumIgnoreLength;
  
  /** The base list. */
  HashMap<Integer, HashSet<Integer>> baseList;
  
  /** The full list. */
  HashMap<Integer, HashSet<Integer>> fullList;
  
  /** The max base end position. */
  HashMap<Integer, Integer> maxBaseEndPosition;
  
  /** The max full end position. */
  HashMap<Integer, Integer> maxFullEndPosition;

  /**
   * Instantiates a new mtas ignore item.
   *
   * @param ignoreSpans the ignore spans
   * @param maximumIgnoreLength the maximum ignore length
   */
  public MtasIgnoreItem(Spans ignoreSpans, Integer maximumIgnoreLength) {
    this.ignoreSpans = ignoreSpans;
    currentDocId = -1;
    currentPosition = -1;
    minimumPosition = -1;
    baseList = new HashMap<Integer, HashSet<Integer>>();
    fullList = new HashMap<Integer, HashSet<Integer>>();
    maxBaseEndPosition = new HashMap<Integer, Integer>();
    maxFullEndPosition = new HashMap<Integer, Integer>();
    if (maximumIgnoreLength == null) {
      this.maximumIgnoreLength = DEFAULT_MAXIMUM_IGNORE_LENGTH;
    } else {
      this.maximumIgnoreLength = maximumIgnoreLength;
    }
  }

  /**
   * Advance to doc.
   *
   * @param docId the doc id
   * @return true, if successful
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public boolean advanceToDoc(int docId) throws IOException {
    if (ignoreSpans == null || currentDocId == Spans.NO_MORE_DOCS) {
      return false;
    } else if (currentDocId == docId) {
      return true;
    } else {
      baseList.clear();
      fullList.clear();
      maxBaseEndPosition.clear();
      maxFullEndPosition.clear();
      if (currentDocId < docId) {
        currentDocId = ignoreSpans.advance(docId);
        currentPosition = -1;
        minimumPosition = -1;
      }
      return currentDocId == docId;
    }
  }

  /**
   * Gets the max size.
   *
   * @param docId the doc id
   * @param position the position
   * @return the max size
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public int getMaxSize(int docId, int position) throws IOException {
    if (ignoreSpans != null && docId == currentDocId) {
      if (position < minimumPosition) {
        throw new IOException(
            "Unexpected position, should be >= " + minimumPosition + "!");
      }
      moveTo(position);
      if (maxBaseEndPosition.containsKey(position)) {
        return maxBaseEndPosition.get(position).intValue() - position;
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  }

  /**
   * Gets the full list.
   *
   * @param docId the doc id
   * @param position the position
   * @return the full list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public HashSet<Integer> getFullList(int docId, int position)
      throws IOException {
    if (ignoreSpans != null && docId == currentDocId) {
      if (position < minimumPosition) {
        throw new IOException(
            "Unexpected startPosition, should be >= " + minimumPosition + "!");
      } else {
        computeFullList(position);
        return fullList.get(position);
      }
    } else {
      return null;
    }
  }

  /**
   * Compute full list.
   *
   * @param position the position
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void computeFullList(int position) throws IOException {
    if (ignoreSpans != null && !fullList.containsKey(position)) {
      // initial fill
      moveTo(position);
      HashSet<Integer> list = baseList.get(position);
      if (list != null && !list.isEmpty()) {
        int maxEndPosition = maxBaseEndPosition.get(position);
        HashSet<Integer> checkList = new HashSet<Integer>();
        HashSet<Integer> subCheckList = new HashSet<Integer>();
        checkList.addAll(list);
        int depth = 1;
        while (!checkList.isEmpty()) {
          if (depth > maximumIgnoreLength) {
            checkList.clear();
            subCheckList.clear();
            throw new IOException("too many successive ignores, maximum is "
                + maximumIgnoreLength);
          } else {
            for (Integer checkItem : checkList) {
              if (fullList.get(checkItem) != null) {
                list.addAll(fullList.get(checkItem));
                maxEndPosition = Math.max(maxEndPosition,
                    maxFullEndPosition.get(checkItem));
              } else {
                moveTo(checkItem);
                if (baseList.containsKey(checkItem)) {
                  list.addAll(baseList.get(checkItem));
                  maxEndPosition = Math.max(maxEndPosition,
                      maxBaseEndPosition.get(checkItem));
                  subCheckList.addAll(baseList.get(checkItem));
                } else {
                  // ready for checkItem
                }
              }
            }
            checkList.clear();
            checkList.addAll(subCheckList);
            subCheckList.clear();
            depth++;
          }
        }
        fullList.put(position, list);
        maxFullEndPosition.put(position, (maxEndPosition - position));
      } else {
        fullList.put(position, null);
        maxFullEndPosition.put(position, 0);
      }
    }
  }

  /**
   * Move to.
   *
   * @param position the position
   */
  private void moveTo(int position) {
    while (position >= currentPosition) {
      try {
        currentPosition = ignoreSpans.nextStartPosition();
        if (currentPosition != Spans.NO_MORE_POSITIONS
            && currentPosition >= minimumPosition) {
          if (!baseList.containsKey(currentPosition)) {
            baseList.put(currentPosition, new HashSet<Integer>());
            maxBaseEndPosition.put(currentPosition, currentPosition);
          }
          baseList.get(currentPosition).add(ignoreSpans.endPosition());
          maxBaseEndPosition.put(currentPosition,
              Math.max(maxBaseEndPosition.get(currentPosition),
                  ignoreSpans.endPosition()));
        }
      } catch (IOException e) {
        currentPosition = Spans.NO_MORE_POSITIONS;
        break;
      }
    }
  }

  /**
   * Removes the before.
   *
   * @param docId the doc id
   * @param position the position
   */
  public void removeBefore(int docId, int position) {
    if (ignoreSpans != null && docId == currentDocId) {
      baseList.entrySet().removeIf(entry -> entry.getKey() < position);
      fullList.entrySet().removeIf(entry -> entry.getKey() < position);
      maxBaseEndPosition.entrySet()
          .removeIf(entry -> entry.getKey() < position);
      maxFullEndPosition.entrySet()
          .removeIf(entry -> entry.getKey() < position);
      if (minimumPosition < position) {
        minimumPosition = position;
      }
      if (currentPosition < position) {
        currentPosition = position;
      }
    }
  }
}
