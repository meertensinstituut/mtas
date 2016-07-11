package mtas.search.spans;

import java.io.IOException;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.Spans;

/**
 * The Class MtasTermSpans.
 */
public class MtasEndSpans extends Spans {
  
Spans spans;
  
  public MtasEndSpans(Spans spans) {
    super();
    this.spans = spans;
  }

  @Override
  public int nextStartPosition() throws IOException {
    return (spans==null)?NO_MORE_POSITIONS:spans.nextStartPosition();
  }

  @Override
  public int startPosition() {
    return (spans==null)?-1:spans.endPosition();
  }

  @Override
  public int endPosition() {
    return (spans==null)?-1:spans.endPosition();
  }

  @Override
  public int width() {
    return 0;
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    if(spans!=null) {
      spans.collect(collector);    
    }
  }

  @Override
  public int docID() {
    return (spans==null)?NO_MORE_DOCS:spans.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return (spans==null)?NO_MORE_DOCS:spans.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return (spans==null)?NO_MORE_DOCS:spans.advance(target);
  }

  @Override
  public long cost() {
    return (spans==null)?0:spans.cost();
  }

  @Override
  public float positionsCost() {
    return (spans==null)?0:spans.positionsCost();
  }  

}
