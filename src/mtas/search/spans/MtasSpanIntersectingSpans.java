package mtas.search.spans;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.Spans;

import mtas.search.spans.MtasSpanIntersectingQuery.MtasSpanIntersectingQuerySpans;
import mtas.search.spans.util.MtasSpans;

public class MtasSpanIntersectingSpans extends Spans implements MtasSpans {

  private MtasSpanIntersectingQuerySpans spans1, spans2;
  
  private int docId;
  
  public MtasSpanIntersectingSpans(MtasSpanIntersectingQuery mtasSpanIntersectingQuery,
      MtasSpanIntersectingQuerySpans spans1, MtasSpanIntersectingQuerySpans spans2) {
    super();
    docId = -1;
    this.spans1 = spans1;
    this.spans2 = spans2;
  }
  
  @Override
  public void collect(SpanCollector collector) throws IOException {   
    spans1.spans.collect(collector);
    spans2.spans.collect(collector);
  }

  @Override
  public int endPosition() {
    return NO_MORE_POSITIONS;
  }

  @Override
  public int nextStartPosition() throws IOException {
    return NO_MORE_POSITIONS;
  }

  @Override
  public float positionsCost() {
    return 0;
  }

  @Override
  public int startPosition() {
    return NO_MORE_POSITIONS;
  }

  @Override
  public int width() {
    return 0;
  }

  @Override
  public int advance(int target) throws IOException {
    return NO_MORE_POSITIONS;
  }

  @Override
  public long cost() {
    return 0;
  }

  @Override
  public int docID() {
    return NO_MORE_DOCS;
  }

  @Override
  public int nextDoc() throws IOException {
    return NO_MORE_DOCS;
  }
  
}
