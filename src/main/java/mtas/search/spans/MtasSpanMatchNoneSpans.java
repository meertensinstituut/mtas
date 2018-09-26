package mtas.search.spans;

import mtas.search.spans.util.MtasSpanQuery;
import mtas.search.spans.util.MtasSpans;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.SpanCollector;

import java.io.IOException;

public class MtasSpanMatchNoneSpans extends MtasSpans {
  private MtasSpanQuery query;
  private int currentStartPosition;
  private int currentEndPosition;
  private int docId;

  public MtasSpanMatchNoneSpans(MtasSpanQuery query) {
    currentStartPosition = NO_MORE_POSITIONS;
    currentEndPosition = NO_MORE_POSITIONS;
    docId = -1;
    this.query = query;
  }

  @Override
  public int nextStartPosition() throws IOException {
    return currentStartPosition;
  }

  @Override
  public int startPosition() {
    return currentStartPosition;
  }

  @Override
  public int endPosition() {
    return currentEndPosition;
  }

  @Override
  public int width() {
    return 0;
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    // do nothing
  }

  @Override
  public int docID() {
    return docId;
  }

  @Override
  public int nextDoc() throws IOException {
    docId = NO_MORE_DOCS;
    currentStartPosition = NO_MORE_POSITIONS;
    currentEndPosition = NO_MORE_POSITIONS;
    return docId;
  }

  @Override
  public int advance(int target) throws IOException {
    docId = NO_MORE_DOCS;
    currentStartPosition = NO_MORE_POSITIONS;
    currentEndPosition = NO_MORE_POSITIONS;
    return docId;
  }

  @Override
  public long cost() {
    return 0;
  }

  @Override
  public float positionsCost() {
    return 0;
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    if (!query.twoPhaseIteratorAllowed()) {
      return null;
    } else {
      // TODO
      return null;
    }
  }
}
