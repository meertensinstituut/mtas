package mtas.search.spans;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.Spans;

public class MtasSpanWithinSpans extends Spans {

  Spans littleSpans, bigSpans;

  private int docId;
  private boolean calledNextStartPosition, noMorePositions;
  private HashMap<Integer, Integer> minimumStartPositionForEndPosition;
  private int lastBigStartPosition, largestStoredEndPosition;
  private int lastBigSpansStartPosition, lastBigSpansEndPosition;

  public MtasSpanWithinSpans(Spans littleSpans, Spans bigSpans) {
    this.littleSpans = littleSpans;
    this.bigSpans = bigSpans;
    docId = -1;
    minimumStartPositionForEndPosition = new HashMap<Integer, Integer>();
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
      return littleSpans.startPosition();
      // compute next match
    } else {
      if (goToNextStartPosition()) {
        // match found
        return littleSpans.startPosition();
      } else {
        // no more matches: document finished
        noMorePositions = true;
        return NO_MORE_POSITIONS;
      }
    }
  }

  @Override
  public int startPosition() {
    return littleSpans.startPosition();
  }

  @Override
  public int endPosition() {
    return littleSpans.endPosition();
  }

  @Override
  public int width() {
    return littleSpans.width();
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    bigSpans.collect(collector);
    littleSpans.collect(collector);
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
      int littleDocId = littleSpans.docID();
      int bigDocId = littleSpans.docID();
      // advance little
      if (littleDocId < target) {
        littleDocId = littleSpans.advance(target);
      }
      // advance big
      if (bigDocId < target) {
        bigDocId = bigSpans.advance(target);
      }
      docId = Math.max(littleDocId, bigDocId);
      if (docId == NO_MORE_DOCS) {
        return docId;
      } else {
        if (!goToNextStartPosition()) {
          return nextDoc();
        } else {
          return docId;
        }
      }
    }
  }

  private boolean goToNextDoc() throws IOException {
    if (docId == NO_MORE_DOCS) {
      return true;
    } else {
      int littleDocId = littleSpans.nextDoc();
      int bigDocId = bigSpans.advance(littleDocId);
      docId = bigDocId;
      while (littleDocId != bigDocId && docId != NO_MORE_DOCS) {
        if (littleDocId < bigDocId) {
          littleDocId = littleSpans.advance(bigDocId);
          docId = littleDocId;
        } else {
          bigDocId = bigSpans.advance(littleDocId);
          docId = bigDocId;
        }
      }
      if (docId != NO_MORE_DOCS) {
        if(!goToNextStartPosition()) {
          reset();
          return false;
        }
      } 
      return true;      
    }
  }

  private boolean goToNextStartPosition() throws IOException {
    int nextLittleSpansStartPosition, nextLittleSpansEndPosition;
    while ((nextLittleSpansStartPosition = littleSpans
        .nextStartPosition()) != NO_MORE_POSITIONS) {
      nextLittleSpansEndPosition = littleSpans.endPosition();
      // check last
      if (nextLittleSpansStartPosition >= lastBigSpansStartPosition
          && nextLittleSpansEndPosition <= lastBigSpansEndPosition) {
        return true;
        // check stored values
      } else if (nextLittleSpansEndPosition <= largestStoredEndPosition) {
        if (nextLittleSpansStartPosition >= lastBigStartPosition) {
          return true;
        } else {
          Iterator<Integer> it = minimumStartPositionForEndPosition.keySet()
              .iterator();
          int bigEndPosition;
          while (it.hasNext()) {
            bigEndPosition = it.next();
            // remove
            if (bigEndPosition < nextLittleSpansStartPosition) {
              it.remove();
              // check for match
            } else if (nextLittleSpansEndPosition <= bigEndPosition
                && nextLittleSpansStartPosition >= minimumStartPositionForEndPosition
                    .get(bigEndPosition)) {
              return true;
            }
          }
        }
      }
      //check new bigSpans
      while(nextLittleSpansStartPosition>=lastBigStartPosition) {
        // store previous
        if (nextLittleSpansStartPosition <= lastBigSpansEndPosition) {
          minimumStartPositionForEndPosition.put(lastBigSpansEndPosition,
              lastBigSpansStartPosition);
          largestStoredEndPosition = Math.max(lastBigSpansEndPosition,
              largestStoredEndPosition);
        }
        lastBigSpansStartPosition = bigSpans.nextStartPosition();    
        lastBigSpansEndPosition = bigSpans.endPosition();
        lastBigStartPosition = lastBigSpansStartPosition;        
        if (lastBigSpansStartPosition == NO_MORE_POSITIONS) {
          noMorePositions = true;
          return false;
        } else if(nextLittleSpansStartPosition>=lastBigSpansStartPosition && nextLittleSpansEndPosition<=lastBigSpansEndPosition) {
          return true;
        }
      } 
    }
    return false;
  }

  private void reset() {
    noMorePositions = false;
    calledNextStartPosition = false;
    lastBigStartPosition = -1;
    largestStoredEndPosition = -1;
    lastBigSpansStartPosition = -1;
    lastBigSpansEndPosition = -1;
    minimumStartPositionForEndPosition.clear();
  }

  @Override
  public long cost() {
    return 0;
  }

}
