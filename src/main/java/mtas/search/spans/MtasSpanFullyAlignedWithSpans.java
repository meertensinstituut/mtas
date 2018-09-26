package mtas.search.spans;

import mtas.search.spans.MtasSpanFullyAlignedWithQuery.MtasSpanFullyAlignedWithQuerySpans;
import mtas.search.spans.util.MtasSpans;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.SpanCollector;

import java.io.IOException;
import java.util.HashSet;

public class MtasSpanFullyAlignedWithSpans extends MtasSpans {
  private MtasSpanFullyAlignedWithQuery query;
  private MtasSpanFullyAlignedWithQuerySpans spans1;
  private MtasSpanFullyAlignedWithQuerySpans spans2;
  private int lastSpans2StartPosition;
  private int lastSpans2EndPosition;
  private int previousSpans2StartPosition;
  private HashSet<Integer> previousSpans2EndPositions;
  private boolean calledNextStartPosition;
  private boolean noMorePositions;
  private boolean noMorePositionsSpan2;
  private int docId;

  public MtasSpanFullyAlignedWithSpans(MtasSpanFullyAlignedWithQuery query,
      MtasSpanFullyAlignedWithQuerySpans spans1,
      MtasSpanFullyAlignedWithQuerySpans spans2) {
    super();
    docId = -1;
    this.query = query;
    this.spans1 = spans1;
    this.spans2 = spans2;
    previousSpans2EndPositions = new HashSet<>();
  }

  @Override
  public int nextStartPosition() throws IOException {
    // no document
    if (docId == -1 || docId == NO_MORE_DOCS) {
      throw new IOException("no document");
      // finished
    } else if (noMorePositions) {
      return NO_MORE_POSITIONS;
      // littleSpans already at start match, because of check for matching
      // document
    } else if (!calledNextStartPosition) {
      calledNextStartPosition = true;
      return spans1.spans.startPosition();
      // compute next match
    } else {
      if (goToNextStartPosition()) {
        // match found
        return spans1.spans.startPosition();
      } else {
        // no more matches: document finished
        return NO_MORE_POSITIONS;
      }
    }
  }

  @Override
  public int startPosition() {
    if (calledNextStartPosition) {
      if (noMorePositions) {
        return NO_MORE_POSITIONS;
      } else {
        return spans1.spans.startPosition();
      }
    } else {
      return -1;
    }
  }

  @Override
  public int endPosition() {
    if (calledNextStartPosition) {
      if (noMorePositions) {
        return NO_MORE_POSITIONS;
      } else {
        return spans1.spans.endPosition();
      }
    } else {
      return -1;
    }
  }

  @Override
  public int width() {
    if (calledNextStartPosition) {
      if (noMorePositions) {
        return 0;
      } else {
        return spans1.spans.endPosition() - spans1.spans.startPosition();
      }
    } else {
      return 0;
    }
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    spans1.spans.collect(collector);
    spans2.spans.collect(collector);
  }

  @Override
  public float positionsCost() {
    return 0;
  }

  @Override
  public int docID() {
    return docId;
  }

  @Override
  public int nextDoc() throws IOException {
    reset();
    while (!goToNextDoc())
      ;
    return docId;
  }

  @Override
  public int advance(int target) throws IOException {
    reset();
    if (docId == NO_MORE_DOCS) {
      return docId;
    } else if (target < docId) {
      // should not happen
      docId = NO_MORE_DOCS;
      return docId;
    } else {
      // advance 1
      int spans1DocId = spans1.spans.docID();
      int newTarget = target;
      if (spans1DocId < newTarget) {
        spans1DocId = spans1.spans.advance(target);
        if (spans1DocId == NO_MORE_DOCS) {
          docId = NO_MORE_DOCS;
          return docId;
        }
        newTarget = Math.max(newTarget, spans1DocId);
      }
      int spans2DocId = spans2.spans.docID();
      // advance 2
      if (spans2DocId < newTarget) {
        spans2DocId = spans2.spans.advance(newTarget);
        if (spans2DocId == NO_MORE_DOCS) {
          docId = NO_MORE_DOCS;
          return docId;
        }
      }
      // check equal docId, otherwise next
      if (spans1DocId == spans2DocId) {
        docId = spans1DocId;
        // check match
        if (goToNextStartPosition()) {
          return docId;
        } else {
          return nextDoc();
        }
      } else {
        return nextDoc();
      }
    }
  }

  private boolean goToNextDoc() throws IOException {
    if (docId == NO_MORE_DOCS) {
      return true;
    } else {
      int spans1DocId = spans1.spans.nextDoc();
      int spans2DocId = spans2.spans.docID();
      docId = Math.max(spans1DocId, spans2DocId);
      while (spans1DocId != spans2DocId && docId != NO_MORE_DOCS) {
        if (spans1DocId < spans2DocId) {
          spans1DocId = spans1.spans.advance(spans2DocId);
          docId = spans1DocId;
        } else {
          spans2DocId = spans2.spans.advance(spans1DocId);
          docId = spans2DocId;
        }
      }
      if (docId != NO_MORE_DOCS && !goToNextStartPosition()) {
        reset();
        return false;
      }
      return true;
    }
  }

  private boolean goToNextStartPosition() throws IOException {
    int nextSpans1StartPosition;
    int nextSpans1EndPosition;
    int nextSpans2StartPosition;
    int nextSpans2EndPosition;
    // loop over span1
    while ((nextSpans1StartPosition = spans1.spans
        .nextStartPosition()) != NO_MORE_POSITIONS) {
      nextSpans1EndPosition = spans1.spans.endPosition();
      if (noMorePositionsSpan2
          && nextSpans1StartPosition > lastSpans2StartPosition) {
        noMorePositions = true;
        return false;
        // check if start/en span1 matches start/end span2 from last or previous
      } else if ((nextSpans1StartPosition == lastSpans2StartPosition
          && nextSpans1EndPosition == lastSpans2EndPosition)
          || (nextSpans1StartPosition == previousSpans2StartPosition
              && previousSpans2EndPositions.contains(nextSpans1EndPosition))) {
        return true;
      } else {
        // try to find matching span2
        while (!noMorePositionsSpan2
            && nextSpans1StartPosition >= lastSpans2StartPosition) {
          // get new span2
          nextSpans2StartPosition = spans2.spans.nextStartPosition();
          // check for finished span2
          if (nextSpans2StartPosition == NO_MORE_POSITIONS) {
            noMorePositionsSpan2 = true;
          } else {
            // get end for new span2
            nextSpans2EndPosition = spans2.spans.endPosition();
            // check for registering last span2 as previous
            if (nextSpans1StartPosition <= lastSpans2StartPosition) {
              if (previousSpans2StartPosition != lastSpans2StartPosition) {
                previousSpans2StartPosition = lastSpans2StartPosition;
                previousSpans2EndPositions.clear();
              }
              previousSpans2EndPositions.add(lastSpans2EndPosition);
            }
            // register span2 as last
            lastSpans2StartPosition = nextSpans2StartPosition;
            lastSpans2EndPosition = nextSpans2EndPosition;
            // check for match
            if (nextSpans1StartPosition == nextSpans2StartPosition
                && nextSpans1EndPosition == nextSpans2EndPosition) {
              return true;
            }
          }
        }

      }
    }
    noMorePositions = true;
    return false;
  }

  private void reset() {
    calledNextStartPosition = false;
    noMorePositions = false;
    noMorePositionsSpan2 = false;
    lastSpans2StartPosition = -1;
    lastSpans2EndPosition = -1;
    previousSpans2StartPosition = -1;
    previousSpans2EndPositions.clear();
  }

  @Override
  public long cost() {
    return 0;
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    if (spans1 == null || spans2 == null || !query.twoPhaseIteratorAllowed()) {
      return null;
    } else {
      // TODO
      return null;
    }
  }
}
