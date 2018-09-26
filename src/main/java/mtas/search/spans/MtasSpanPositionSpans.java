package mtas.search.spans;

import mtas.codec.util.CodecInfo;
import mtas.codec.util.CodecInfo.IndexDoc;
import mtas.search.spans.util.MtasSpans;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.SpanCollector;

import java.io.IOException;

public class MtasSpanPositionSpans extends MtasSpans {
  private MtasSpanPositionQuery query;
  private String field;
  private int start;
  private int end;
  private int minPosition;
  private int maxPosition;
  private int currentStartPosition;
  private int currentEndPosition;
  private int docId;
  private CodecInfo mtasCodecInfo;

  public MtasSpanPositionSpans(MtasSpanPositionQuery query,
      CodecInfo mtasCodecInfo, String field, int start, int end) {
    super();
    this.query = query;
    this.mtasCodecInfo = mtasCodecInfo;
    this.field = field;
    this.start = start;
    this.end = end;
    minPosition = NO_MORE_POSITIONS;
    maxPosition = NO_MORE_POSITIONS;
    currentStartPosition = NO_MORE_POSITIONS;
    currentEndPosition = NO_MORE_POSITIONS;
    docId = -1;
  }

  @Override
  public int nextStartPosition() throws IOException {
    if (currentStartPosition < minPosition) {
      currentStartPosition = minPosition;
      currentEndPosition = currentStartPosition + 1;
    } else {
      currentStartPosition++;
      currentEndPosition = currentStartPosition + 1;
      if (currentStartPosition > maxPosition) {
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
    return 1;
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    // don't do anything
  }

  @Override
  public int docID() {
    return docId;
  }

  @Override
  public int nextDoc() throws IOException {
    do {
      IndexDoc indexDoc = mtasCodecInfo.getNextDoc(field, docId);
      if (indexDoc != null) {
        docId = indexDoc.docId;
        minPosition = Math.max(start, indexDoc.minPosition);
        maxPosition = Math.min(end, indexDoc.maxPosition);
        currentStartPosition = -1;
        currentEndPosition = -1;
      } else {
        docId = NO_MORE_DOCS;
        minPosition = NO_MORE_POSITIONS;
        maxPosition = NO_MORE_POSITIONS;
        currentStartPosition = NO_MORE_POSITIONS;
        currentEndPosition = NO_MORE_POSITIONS;
      }
    } while (docId != NO_MORE_DOCS && (minPosition > maxPosition));
    return docId;
  }

  @Override
  public int advance(int target) throws IOException {
    int tmpTarget = target - 1;
    do {
      IndexDoc indexDoc = mtasCodecInfo.getNextDoc(field, tmpTarget);
      if (indexDoc != null) {
        docId = indexDoc.docId;
        minPosition = Math.max(start, indexDoc.minPosition);
        maxPosition = Math.min(end, indexDoc.maxPosition);
        currentStartPosition = -1;
        currentEndPosition = -1;
      } else {
        docId = NO_MORE_DOCS;
        minPosition = NO_MORE_POSITIONS;
        maxPosition = NO_MORE_POSITIONS;
        currentStartPosition = NO_MORE_POSITIONS;
        currentEndPosition = NO_MORE_POSITIONS;
      }
      tmpTarget = docId;
    } while (docId != NO_MORE_DOCS && minPosition > maxPosition);
    return docId;
  }

  @Override
  public long cost() {
    return mtasCodecInfo.getNumberOfDocs(field);
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
