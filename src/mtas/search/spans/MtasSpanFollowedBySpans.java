package mtas.search.spans;

import java.io.IOException;
import java.util.HashSet;

import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.Spans;

import mtas.search.spans.MtasSpanFollowedByQuery.MtasSpanFollowedByQuerySpans;
import mtas.search.spans.util.MtasSpans;

public class MtasSpanFollowedBySpans extends Spans implements MtasSpans {

  /** The spans2. */
  private MtasSpanFollowedByQuerySpans spans1;
  private MtasSpanFollowedByQuerySpans spans2;

  private int lastSpans2StartPosition;
  private HashSet<Integer> previousSpans2StartPositions;


  /** The no more positions. */
  private boolean calledNextStartPosition;
  private boolean noMorePositions;

  /** The doc id. */
  private int docId;

  public MtasSpanFollowedBySpans(
      MtasSpanFollowedByQuery mtasSpanFollowedByQuery,
      MtasSpanFollowedByQuerySpans spans1,
      MtasSpanFollowedByQuerySpans spans2) {
    super();
    docId = -1;
    this.spans1 = spans1;
    this.spans2 = spans2;   
    previousSpans2StartPositions = new HashSet<>();
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    spans1.spans.collect(collector);
    spans2.spans.collect(collector);
  }

  @Override
  public int endPosition() {
    return calledNextStartPosition
        ? (noMorePositions ? NO_MORE_POSITIONS : spans1.spans.endPosition())
        : -1;
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
  public float positionsCost() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int startPosition() {
    return calledNextStartPosition
        ? (noMorePositions ? NO_MORE_POSITIONS : spans1.spans.startPosition())
        : -1;
  }

  @Override
  public int width() {
    return calledNextStartPosition ? (noMorePositions ? 0
        : spans1.spans.endPosition() - spans1.spans.startPosition()) : 0;
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
      if (spans1DocId < target) {
        spans1DocId = spans1.spans.advance(target);
        if (spans1DocId == NO_MORE_DOCS) {
          docId = NO_MORE_DOCS;
          return docId;
        }
        target = Math.max(target, spans1DocId);
      }
      int spans2DocId = spans2.spans.docID();
      // advance 2
      if (spans2DocId < target) {
        spans2DocId = spans2.spans.advance(target);
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

  @Override
  public long cost() {
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
    while ((nextSpans1StartPosition = spans1.spans
        .nextStartPosition()) != NO_MORE_POSITIONS) {
      nextSpans1EndPosition = spans1.spans.endPosition();
      if (nextSpans1EndPosition == lastSpans2StartPosition) {
        return true;
      } else {
        //clean up
        if(lastSpans2StartPosition<nextSpans1StartPosition) {
          previousSpans2StartPositions.clear();
        } else if(previousSpans2StartPositions.contains(nextSpans1EndPosition)) {
          return true;
        }
        //try to find match
        while (lastSpans2StartPosition < nextSpans1EndPosition) {
          if (lastSpans2StartPosition != NO_MORE_POSITIONS) {
            lastSpans2StartPosition = spans2.spans.nextStartPosition();
          }  
          if (lastSpans2StartPosition == NO_MORE_POSITIONS) {
            if(previousSpans2StartPositions.isEmpty()) {
              noMorePositions = true;
              return false;
            }            
          } else {
            if(lastSpans2StartPosition>=nextSpans1StartPosition) {
              previousSpans2StartPositions.add(lastSpans2StartPosition);
            } 
            if (nextSpans1EndPosition == lastSpans2StartPosition) {
              return true;
            } 
          }
        }
      }      
    }
    return false;
  }

  private void reset() {
    calledNextStartPosition = false;
    noMorePositions = false;
    lastSpans2StartPosition = -1;
    previousSpans2StartPositions.clear();
  }

}
