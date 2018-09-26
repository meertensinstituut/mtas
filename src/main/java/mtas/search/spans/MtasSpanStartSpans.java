package mtas.search.spans;

import mtas.search.spans.util.MtasSpans;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.Spans;

import java.io.IOException;

public class MtasSpanStartSpans extends MtasSpans {
  private MtasSpanStartQuery query;
  private Spans spans;

  public MtasSpanStartSpans(MtasSpanStartQuery query, Spans spans) {
    super();
    this.query = query;
    this.spans = spans;
  }

  @Override
  public int nextStartPosition() throws IOException {
    return (spans == null) ? NO_MORE_POSITIONS : spans.nextStartPosition();
  }

  @Override
  public int startPosition() {
    return (spans == null) ? -1 : spans.startPosition();
  }

  @Override
  public int endPosition() {
    return (spans == null) ? -1 : spans.startPosition();
  }

  @Override
  public int width() {
    return 0;
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    if (spans != null) {
      spans.collect(collector);
    }
  }

  @Override
  public int docID() {
    return (spans == null) ? NO_MORE_DOCS : spans.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return (spans == null) ? NO_MORE_DOCS : spans.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return (spans == null) ? NO_MORE_DOCS : spans.advance(target);
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    if (spans == null || !query.twoPhaseIteratorAllowed()) {
      return null;
    } else {
      return spans.asTwoPhaseIterator();
    }
  }

  @Override
  public long cost() {
    return (spans == null) ? 0 : spans.cost();
  }

  @Override
  public float positionsCost() {
    return (spans == null) ? 0 : spans.positionsCost();
  }
}
