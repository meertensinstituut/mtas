package mtas.search.spans;

import java.io.IOException;

import mtas.codec.util.CodecInfo;
import mtas.codec.util.CodecInfo.IndexDoc;

import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.Spans;

public class MtasSpanMatchAll extends Spans {

  private String field;
  private int minPosition, maxPosition, currentStartPosition, currentEndPosition, docId;
  private CodecInfo mtasCodecInfo;
  
  public MtasSpanMatchAll(CodecInfo mtasCodecInfo, String field) {
    super(); 
    this.mtasCodecInfo = mtasCodecInfo;
    this.field = field;
    minPosition = NO_MORE_POSITIONS;
    maxPosition = NO_MORE_POSITIONS;
    currentStartPosition = NO_MORE_POSITIONS;
    currentEndPosition = NO_MORE_POSITIONS;
    docId = -1;    
  }
  
  @Override
  public int nextStartPosition() throws IOException {
    if(currentStartPosition < minPosition) {
      currentStartPosition = minPosition;
      currentEndPosition = currentStartPosition + 1;
    } else {
      currentStartPosition++;
      currentEndPosition = currentStartPosition + 1;
      if(currentStartPosition > maxPosition) {
        currentStartPosition = NO_MORE_POSITIONS;
        currentEndPosition = NO_MORE_POSITIONS;
      }  
    }
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
    IndexDoc indexDoc = mtasCodecInfo.getNextDoc(field, docId);
    if(indexDoc!=null) {
      docId = indexDoc.docId;
      minPosition = indexDoc.minPosition;
      maxPosition = indexDoc.maxPosition;
      currentStartPosition = -1;
      currentEndPosition = -1;
    } else {
      docId = NO_MORE_DOCS;
      minPosition = NO_MORE_POSITIONS;
      maxPosition = NO_MORE_POSITIONS;
      currentStartPosition = NO_MORE_POSITIONS;
      currentEndPosition = NO_MORE_POSITIONS;
    }
    return docId;
  }

  @Override
  public int advance(int target) throws IOException {      
    IndexDoc indexDoc = mtasCodecInfo.getNextDoc(field, (target-1));
    if(indexDoc!=null) {
      docId = indexDoc.docId;
      minPosition = indexDoc.minPosition;
      maxPosition = indexDoc.maxPosition;
      currentStartPosition = -1;
      currentEndPosition = -1;
    } else {
      docId = NO_MORE_DOCS;
      minPosition = NO_MORE_POSITIONS;
      maxPosition = NO_MORE_POSITIONS;
      currentStartPosition = NO_MORE_POSITIONS;
      currentEndPosition = NO_MORE_POSITIONS;      
    }
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
