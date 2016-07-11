package mtas.search.spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.Spans;

public class MtasSpanRecurrence extends Spans {

  Spans spans;
  int minimumRecurrence;
  int maximumRecurrence;

  List<Match> queueSpans;
  List<Match> queueMatches;
  Match currentMatch;
  boolean noMorePositions;
  int lastStartPosition; // startPosition of last retrieved span
  boolean lastSpan; // last span for this document added to queue

  public MtasSpanRecurrence(MtasSpanRecurrenceQuery mtasSpanRecurrenceQuery,
      Spans spans, int minimumRecurrence, int maximumRecurrence) {
    assert minimumRecurrence <= maximumRecurrence : "minimumRecurrence > maximumRecurrence";
    assert minimumRecurrence > 0 : "minimumRecurrence < 1 not supported";
    this.spans = spans;
    this.minimumRecurrence = minimumRecurrence;
    this.maximumRecurrence = maximumRecurrence;
    queueSpans = new ArrayList<Match>();
    queueMatches = new ArrayList<Match>();
    resetQueue();
  }

  @Override
  public int nextStartPosition() throws IOException {
    if (findMatches()) {
      currentMatch = queueMatches.get(0);
      queueMatches.remove(0);
      noMorePositions = false;
      return currentMatch.startPosition();
    } else {
      currentMatch = null;
      noMorePositions = true;
      return NO_MORE_POSITIONS;
    }
  }

  @Override
  public int startPosition() {
    return (currentMatch == null) ? (noMorePositions ? NO_MORE_POSITIONS : -1)
        : currentMatch.startPosition();
  }

  @Override
  public int endPosition() {
    return (currentMatch == null) ? (noMorePositions ? NO_MORE_POSITIONS : -1)
        : currentMatch.endPosition();
  }

  @Override
  public int width() {
    return 1;
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    spans.collect(collector);
  }

  @Override
  public int docID() {
    return spans.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    resetQueue();
    return (spans.nextDoc() == NO_MORE_DOCS) ? NO_MORE_DOCS : toMatchDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    resetQueue();
    return (spans.advance(target) == NO_MORE_DOCS) ? NO_MORE_DOCS
        : toMatchDoc();
  }

  void resetQueue() {
    queueSpans.clear();
    queueMatches.clear();
    lastStartPosition = 0;
    lastSpan = false;
    currentMatch = null;
    noMorePositions = false;
  }

  int toMatchDoc() throws IOException {
    while (true) {
      if (findMatches()) {
        return docID();
      } 
      resetQueue();        
      if (spans.nextDoc() == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
    }
  }

  // try to get something in the queue of spans
  private boolean collectSpan() throws IOException {
    if (lastSpan) {
      return false;
    } else if (spans.nextStartPosition() == NO_MORE_POSITIONS) {
      lastSpan = true;
      return false;
    } else {
      queueSpans.add(new Match(spans.startPosition(), spans.endPosition()));
      lastStartPosition = spans.startPosition();
      return true;
    }
  }

  private boolean findMatches() throws IOException {
    // check for something in queue of matches
    if (!queueMatches.isEmpty()) {
      return true;
    } else {
      while (true) {
        // try to get something in queue of spans
        if (queueSpans.isEmpty() && !collectSpan()) {
          return false;
        }
        // try to get matches with first span in queue
        Match firstMatch = queueSpans.remove(0);
        // create a list of matches with same startPosition as firstMatch
        List<Match> matches = new ArrayList<Match>();
        matches.add(firstMatch);
        // try to collect spans until lastStartPosition not equal to
        // startPosition of firstMatch
        while (!lastSpan && (lastStartPosition == firstMatch.startPosition())) {
          collectSpan();
        }
        while (!queueSpans.isEmpty()
            && (queueSpans.get(0).startPosition() == firstMatch.startPosition())) {
          matches.add(queueSpans.remove(0));
        }
        // construct all matches for this startPosition
        for (Match match : matches) {
          for (int n = (minimumRecurrence - 1); n <= (maximumRecurrence - 1); n++) {
            findMatches(match, n);
          }
        }
        // check for something in queue of matches
        if (!queueMatches.isEmpty()) {
          return true;
        }
      }
    }
  }

  private void findMatches(Match match, int n) throws IOException {
    if (n > 0) {
      int largestMatchingEndPosition = match.endPosition();
      // try to find matches with existing queue
      if (!queueSpans.isEmpty()) {
        Match span;
        for (int i = 0; i < queueSpans.size(); i++) {
          span = queueSpans.get(i);
          if (match.endPosition() == span.startPosition()) {
            findMatches(new Match(match.startPosition(), span.endPosition()),
                (n - 1));
            largestMatchingEndPosition = Math.max(largestMatchingEndPosition,
                span.endPosition());
          }
        }
      }
      // extend queue if necessary and possible
      while (!lastSpan && (largestMatchingEndPosition >= lastStartPosition)) {
        if (spans.nextStartPosition() == NO_MORE_POSITIONS) {
          lastSpan = true;
        } else {
          Match span = new Match(spans.startPosition(), spans.endPosition());
          queueSpans.add(span);
          lastStartPosition = spans.startPosition();
          // check if this provides new match
          if (match.endPosition() == span.startPosition()) {
            findMatches(new Match(match.startPosition(), span.endPosition()),
                (n - 1));
            largestMatchingEndPosition = Math.max(largestMatchingEndPosition,
                span.endPosition());
          }
        }
      }
    } else {
      // only unique spans
      if (!queueMatches.contains(match)) {
        queueMatches.add(match);
      }
    }
  }

  private class Match {
    private int startPosition;
    private int endPosition;

    Match(int startPosition, int endPosition) {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
    }

    public int startPosition() {
      return startPosition;
    }

    public int endPosition() {
      return endPosition;
    }

    @Override
    public boolean equals(Object object) {
      if (this.getClass().equals(object.getClass())) {
        if ((((Match) object).startPosition == startPosition)
            && (((Match) object).endPosition == endPosition)) {
          return true;
        }
      }
      return false;
    }

  }

  @Override
  public long cost() {
    return (spans == null) ? 0 : spans.cost();
  }

  @Override
  public float positionsCost() {
    return 0;
  }

}
