package mtas.search.spans.util;

import java.io.IOException;

import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.Spans;

import mtas.codec.util.CodecInfo;
import mtas.codec.util.CodecInfo.IndexDoc;

public class MtasMaximumExpandSpans extends MtasSpans {

  /** The sub spans. */
  Spans subSpans;
  
  MtasMaximumExpandSpanQuery query;

  /** The min position. */
  int minPosition;

  /** The max position. */
  int maxPosition;

  /** The field. */
  String field;

  /** The mtas codec info. */
  CodecInfo mtasCodecInfo;

  /** The start position. */
  int startPosition;

  /** The end position. */
  int endPosition;

  /**
   * Instantiates a new mtas maximum expand spans.
   *
   * @param mtasCodecInfo the mtas codec info
   * @param field the field
   * @param subSpans the sub spans
   */
  public MtasMaximumExpandSpans(MtasMaximumExpandSpanQuery query, CodecInfo mtasCodecInfo, String field,
      Spans subSpans) {
    super();
    this.subSpans = subSpans;
    this.field = field;
    this.mtasCodecInfo = mtasCodecInfo;
    this.minPosition = 0;
    this.maxPosition = 0;
    this.startPosition = -1;
    this.endPosition = -1;
    this.query = query;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.Spans#nextStartPosition()
   */
  @Override
  public int nextStartPosition() throws IOException {
    int basicStartPosition;
    int basicEndPosition;
    while ((basicStartPosition = subSpans
        .nextStartPosition()) != NO_MORE_POSITIONS) {
      basicEndPosition = subSpans.endPosition();
      startPosition = Math.max(minPosition,
          (basicStartPosition - query.maximumLeft));
      endPosition = Math.min(maxPosition + 1,
          (basicEndPosition + query.maximumRight));
      if (startPosition <= (basicStartPosition - query.minimumLeft)
          && endPosition >= (basicEndPosition + query.minimumRight)) {
        return this.startPosition;
      }
    }
    startPosition = NO_MORE_POSITIONS;
    endPosition = NO_MORE_POSITIONS;
    return NO_MORE_POSITIONS;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.Spans#startPosition()
   */
  @Override
  public int startPosition() {
    return startPosition;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.Spans#endPosition()
   */
  @Override
  public int endPosition() {
    return endPosition;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.Spans#width()
   */
  @Override
  public int width() {
    return endPosition - startPosition;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.spans.Spans#collect(org.apache.lucene.search.
   * spans.SpanCollector)
   */
  @Override
  public void collect(SpanCollector collector) throws IOException {
    subSpans.collect(collector);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.Spans#positionsCost()
   */
  @Override
  public float positionsCost() {
    // return subSpans.positionsCost();
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.DocIdSetIterator#docID()
   */
  @Override
  public int docID() {
    return subSpans.docID();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.DocIdSetIterator#nextDoc()
   */
  @Override
  public int nextDoc() throws IOException {
    int docId = subSpans.nextDoc();
    startPosition = -1;
    endPosition = -1;
    if (docId != NO_MORE_DOCS) {
      IndexDoc doc = mtasCodecInfo.getDoc(field, docId);
      if (doc != null) {
        minPosition = doc.minPosition;
        maxPosition = doc.maxPosition;
      } else {
        minPosition = NO_MORE_POSITIONS;
        maxPosition = NO_MORE_POSITIONS;
      }
    } else {
      minPosition = NO_MORE_POSITIONS;
      maxPosition = NO_MORE_POSITIONS;
    }
    return docId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.DocIdSetIterator#advance(int)
   */
  @Override
  public int advance(int target) throws IOException {
    int docId = subSpans.advance(target);
    startPosition = -1;
    endPosition = -1;
    if (docId != NO_MORE_DOCS) {
      IndexDoc doc = mtasCodecInfo.getDoc(field, docId);
      if (doc != null) {
        minPosition = doc.minPosition;
        maxPosition = doc.maxPosition;
      } else {
        minPosition = NO_MORE_POSITIONS;
        maxPosition = NO_MORE_POSITIONS;
      }
    } else {
      minPosition = NO_MORE_POSITIONS;
      maxPosition = NO_MORE_POSITIONS;
    }
    return docId;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.spans.Spans#asTwoPhaseIterator()
   */
  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    return subSpans.asTwoPhaseIterator();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.DocIdSetIterator#cost()
   */
  @Override
  public long cost() {
    return subSpans != null ? subSpans.cost() : 0;
  }
}
