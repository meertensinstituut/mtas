package mtas.search.spans;

import mtas.search.spans.MtasSpanIntersectingQuery.MtasSpanIntersectingQuerySpans;
import mtas.search.spans.util.MtasSpans;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.SpanCollector;

import java.io.IOException;

public class MtasSpanIntersectingSpans extends MtasSpans {
  private MtasSpanIntersectingQuery query;
  private MtasSpanIntersectingQuerySpans spans1;
  private MtasSpanIntersectingQuerySpans spans2;
  private boolean calledNextStartPosition;
  private boolean noMorePositions;
  private int lastSpans2StartPosition;
  private int lastSpans2EndPosition;
  private int docId;

  public MtasSpanIntersectingSpans(MtasSpanIntersectingQuery query,
      MtasSpanIntersectingQuerySpans spans1,
      MtasSpanIntersectingQuerySpans spans2) {
    super();
    docId = -1;
    this.query = query;
    this.spans1 = spans1;
    this.spans2 = spans2;
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
    return calledNextStartPosition
        ? (noMorePositions ? NO_MORE_POSITIONS : spans1.spans.startPosition())
        : -1;
  }

  @Override
  public int endPosition() {
    return calledNextStartPosition
        ? (noMorePositions ? NO_MORE_POSITIONS : spans1.spans.endPosition())
        : -1;
  }

  @Override
  public int width() {
    return calledNextStartPosition ? (noMorePositions ? 0
        : spans1.spans.endPosition() - spans1.spans.startPosition()) : 0;
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
        spans1DocId = spans1.spans.advance(newTarget);
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
      if (docId != NO_MORE_DOCS) {
        if (!goToNextStartPosition()) {
          reset();
          return false;
        }
      }
      return true;
    }
  }

  private boolean goToNextStartPosition() throws IOException {
    int nextSpans1StartPosition;
    int nextSpans1EndPosition;
    int nextSpans2StartPosition;
    int nextSpans2EndPosition;
    while ((nextSpans1StartPosition = spans1.spans
        .nextStartPosition()) != NO_MORE_POSITIONS) {
      nextSpans1EndPosition = spans1.spans.endPosition();
      if (nextSpans1StartPosition <= lastSpans2EndPosition
          && nextSpans1EndPosition >= lastSpans2StartPosition) {
        return true;
      } else {
        while (lastSpans2StartPosition <= nextSpans1EndPosition) {
          nextSpans2StartPosition = spans2.spans.nextStartPosition();
          if (nextSpans2StartPosition == NO_MORE_POSITIONS) {
            noMorePositions = true;
            return false;
          } else {
            nextSpans2EndPosition = spans2.spans.endPosition();
            if (nextSpans2StartPosition > lastSpans2StartPosition
                || nextSpans2EndPosition > lastSpans2EndPosition) {
              if (nextSpans2EndPosition > lastSpans2EndPosition) {
                lastSpans2StartPosition = nextSpans2StartPosition;
                lastSpans2EndPosition = nextSpans2EndPosition;
                if (nextSpans1StartPosition <= lastSpans2EndPosition
                    && nextSpans1EndPosition >= lastSpans2StartPosition) {
                  return true;
                }
              }
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
    lastSpans2StartPosition = -1;
    lastSpans2EndPosition = -1;
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
