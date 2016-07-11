package mtas.search.spans;

import java.io.IOException;

import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.Spans;

public class MtasSpanMatchNone extends Spans {

  private int currentStartPosition, currentEndPosition, docId;
  
  public MtasSpanMatchNone(String field) {
    currentStartPosition = NO_MORE_POSITIONS;
    currentEndPosition = NO_MORE_POSITIONS;
    docId = -1;
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

}
