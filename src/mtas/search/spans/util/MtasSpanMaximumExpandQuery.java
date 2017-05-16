package mtas.search.spans.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import mtas.codec.util.CodecInfo;
import mtas.codec.util.CodecInfo.IndexDoc;
import mtas.search.spans.MtasSpanMatchNoneSpans;

/**
 * The Class MtasSpanMaximumExpandQuery.
 */
public class MtasSpanMaximumExpandQuery extends MtasSpanQuery {

  /** The query. */
  MtasSpanQuery query;
  
  /** The minimum left. */
  int minimumLeft;
  
  /** The maximum left. */
  int maximumLeft;
  
  /** The minimum right. */
  int minimumRight;
  
  /** The maximum right. */
  int maximumRight;

  /**
   * Instantiates a new mtas span maximum expand query.
   *
   * @param query the query
   * @param minimumLeft the minimum left
   * @param maximumLeft the maximum left
   * @param minimumRight the minimum right
   * @param maximumRight the maximum right
   */
  public MtasSpanMaximumExpandQuery(MtasSpanQuery query, int minimumLeft,
      int maximumLeft, int minimumRight, int maximumRight) {
    super(null, null);
    this.query = query;
    if (minimumLeft > maximumLeft || minimumRight > maximumRight
        || minimumLeft < 0 || minimumRight < 0) {
      throw new IllegalArgumentException();
    }
    this.minimumLeft = minimumLeft;
    this.maximumLeft = maximumLeft;
    this.minimumRight = minimumRight;
    this.maximumRight = maximumRight;
    Integer minimum = query.getMinimumWidth();
    Integer maximum = query.getMaximumWidth();
    if (minimum != null) {
      minimum += minimumLeft + minimumRight;
    }
    if (maximum != null) {
      maximum += maximumLeft + maximumRight;
    }
    setWidth(minimum, maximum);
  }

  /* (non-Javadoc)
   * @see mtas.search.spans.util.MtasSpanQuery#createWeight(org.apache.lucene.search.IndexSearcher, boolean)
   */
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    SpanWeight subWeight = query.createWeight(searcher, needsScores);
    if (maximumLeft == 0 && maximumRight == 0) {
      return subWeight;
    } else {
      return new MtasMaximumExpandWeight(subWeight, searcher, needsScores);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return query.getField();
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.Query#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    buffer.append(query.toString(field) + "]["+minimumLeft+","+maximumLeft+"]["+minimumRight+","+maximumRight+"])");
    return buffer.toString();
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.Query#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanMaximumExpandQuery that = (MtasSpanMaximumExpandQuery) obj;
    boolean isEqual;
    isEqual = query.equals(that.query);
    isEqual &= minimumLeft == that.minimumLeft;
    isEqual &= maximumLeft == that.maximumLeft;
    isEqual &= minimumRight == that.minimumRight;
    isEqual &= maximumRight == that.maximumRight;
    return isEqual;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.Query#hashCode()
   */
  @Override
  public int hashCode() {
    int h = Integer.rotateLeft(classHash(), 1);
    h ^= query.hashCode();
    h = Integer.rotateLeft(h, minimumLeft) + minimumLeft;
    h ^= 2;
    h = Integer.rotateLeft(h, maximumLeft) + maximumLeft;
    h ^= 3;
    h = Integer.rotateLeft(h, minimumRight) + minimumRight;
    h ^= 5;
    h = Integer.rotateLeft(h, maximumRight) + maximumRight;
    return h;
  }

  /* (non-Javadoc)
   * @see mtas.search.spans.util.MtasSpanQuery#rewrite(org.apache.lucene.index.IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newQuery = query.rewrite(reader);
    if (maximumLeft == 0 && maximumRight == 0) {
      return newQuery;
    } else if (!query.equals(newQuery)) {
      return new MtasSpanMaximumExpandQuery(newQuery, minimumLeft, maximumLeft,
          minimumRight, maximumRight);
    } else {
      return super.rewrite(reader);
    }
  }

  /**
   * The Class MtasMaximumExpandWeight.
   */
  private class MtasMaximumExpandWeight extends SpanWeight {
    
    /** The sub weight. */
    SpanWeight subWeight;

    /**
     * Instantiates a new mtas maximum expand weight.
     *
     * @param subWeight the sub weight
     * @param searcher the searcher
     * @param needsScores the needs scores
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasMaximumExpandWeight(SpanWeight subWeight, IndexSearcher searcher,
        boolean needsScores) throws IOException {
      super(MtasSpanMaximumExpandQuery.this, searcher,
          needsScores ? getTermContexts(subWeight) : null);
      this.subWeight = subWeight;
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.spans.SpanWeight#extractTermContexts(java.util.Map)
     */
    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      subWeight.extractTermContexts(contexts);
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.spans.SpanWeight#getSpans(org.apache.lucene.index.LeafReaderContext, org.apache.lucene.search.spans.SpanWeight.Postings)
     */
    @Override
    public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings)
        throws IOException {
      Spans spans = subWeight.getSpans(ctx, requiredPostings);
      if (maximumLeft == 0 && maximumRight == 0) {
        return spans;
      } else {
        try {
          // get leafreader
          LeafReader r = ctx.reader();
          // get delegate
          Boolean hasMethod = true;
          while (hasMethod) {
            hasMethod = false;
            Method[] methods = r.getClass().getMethods();
            for (Method m : methods) {
              if (m.getName().equals("getDelegate")) {
                hasMethod = true;
                r = (LeafReader) m.invoke(r, (Object[]) null);
                break;
              }
            }
          } // get fieldsproducer
          Method fpm = r.getClass().getMethod("getPostingsReader",
              (Class<?>[]) null);
          FieldsProducer fp = (FieldsProducer) fpm.invoke(r, (Object[]) null);
          // get MtasFieldsProducer using terms
          Terms t = fp.terms(field);
          if (t == null) {
            return new MtasSpanMatchNoneSpans(field);
          } else {
            CodecInfo mtasCodecInfo = CodecInfo.getCodecInfoFromTerms(t);
            return new MtasMaximumExpandSpans(mtasCodecInfo, query.getField(),
                spans);
          }
        } catch (Exception e) {
          throw new IOException("Can't get reader", e);
        }

      }
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.Weight#extractTerms(java.util.Set)
     */
    @Override
    public void extractTerms(Set<Term> terms) {
      subWeight.extractTerms(terms);
    }

  }

  /**
   * The Class MtasMaximumExpandSpans.
   */
  private class MtasMaximumExpandSpans extends Spans {

    /** The sub spans. */
    Spans subSpans;
    
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
    public MtasMaximumExpandSpans(CodecInfo mtasCodecInfo, String field,
        Spans subSpans) {
      super();
      this.subSpans = subSpans;
      this.field = field;
      this.mtasCodecInfo = mtasCodecInfo;
      this.minPosition = 0;
      this.maxPosition = 0;
      this.startPosition = -1;
      this.endPosition = -1;
    }

    /* (non-Javadoc)
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
            (basicStartPosition - maximumLeft));
        endPosition = Math.min(maxPosition + 1,
            (basicEndPosition + maximumRight));
        if (startPosition <= (basicStartPosition - minimumLeft)
            && endPosition >= (basicEndPosition + minimumRight)) {
          return this.startPosition;
        }
      }
      startPosition = NO_MORE_POSITIONS;
      endPosition = NO_MORE_POSITIONS;
      return NO_MORE_POSITIONS;
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.spans.Spans#startPosition()
     */
    @Override
    public int startPosition() {
      return startPosition;
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.spans.Spans#endPosition()
     */
    @Override
    public int endPosition() {
      return endPosition;
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.spans.Spans#width()
     */
    @Override
    public int width() {
      return endPosition-startPosition;
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.spans.Spans#collect(org.apache.lucene.search.spans.SpanCollector)
     */
    @Override
    public void collect(SpanCollector collector) throws IOException {
      subSpans.collect(collector);
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.spans.Spans#asTwoPhaseIterator()
     */
    @Override
    public final TwoPhaseIterator asTwoPhaseIterator() {
      // return subSpans.asTwoPhaseIterator();
      return null;
    }
    
    /* (non-Javadoc)
     * @see org.apache.lucene.search.spans.Spans#positionsCost()
     */
    @Override
    public float positionsCost() {
      //return subSpans.positionsCost();
      return 0;
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.DocIdSetIterator#docID()
     */
    @Override
    public int docID() {
      return subSpans.docID();
    }

    /* (non-Javadoc)
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

    /* (non-Javadoc)
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
     * @see org.apache.lucene.search.DocIdSetIterator#cost()
     */
    @Override
    public long cost() {
      return subSpans!=null?subSpans.cost():0;
    }
  }

}
