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
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import mtas.codec.util.CodecInfo;
import mtas.search.spans.MtasSpanMatchNoneSpans;

/**
 * The Class MtasExpandSpanQuery.
 */
public class MtasExpandSpanQuery extends MtasSpanQuery {

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
   * Instantiates a new mtas expand span query.
   *
   * @param query the query
   * @param minimumLeft the minimum left
   * @param maximumLeft the maximum left
   * @param minimumRight the minimum right
   * @param maximumRight the maximum right
   */
  public MtasExpandSpanQuery(MtasSpanQuery query, int minimumLeft,
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

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.search.spans.util.MtasSpanQuery#createWeight(org.apache.lucene.search.
   * IndexSearcher, boolean)
   */
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost)
      throws IOException {
    SpanWeight subWeight = query.createWeight(searcher, needsScores, boost);
    if (maximumLeft == 0 && maximumRight == 0) {
      return subWeight;
    } else {
      return new MtasExpandWeight(subWeight, searcher, needsScores, boost);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return query.getField();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    buffer.append(query.toString(field) + "][" + minimumLeft + "," + maximumLeft
        + "][" + minimumRight + "," + maximumRight + "])");
    return buffer.toString();
  }

  /*
   * (non-Javadoc)
   * 
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
    final MtasExpandSpanQuery that = (MtasExpandSpanQuery) obj;
    boolean isEqual;
    isEqual = query.equals(that.query);
    isEqual &= minimumLeft == that.minimumLeft;
    isEqual &= maximumLeft == that.maximumLeft;
    isEqual &= minimumRight == that.minimumRight;
    isEqual &= maximumRight == that.maximumRight;
    return isEqual;
  }

  /*
   * (non-Javadoc)
   * 
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

  /*
   * (non-Javadoc)
   * 
   * @see mtas.search.spans.util.MtasSpanQuery#rewrite(org.apache.lucene.index.
   * IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newQuery = query.rewrite(reader);
    if (maximumLeft == 0 && maximumRight == 0) {
      return newQuery;
    } else if (((maximumLeft == 0) || (maximumLeft == minimumLeft))
        && ((maximumRight == 0) || (maximumRight == minimumRight))) {
      MtasSpanQuery maximumExpandedQuery = new MtasMaximumExpandSpanQuery(
          newQuery, minimumLeft, maximumLeft, minimumRight, maximumRight);
      return maximumExpandedQuery.rewrite(reader);
    } else if (!query.equals(newQuery)) {
      return new MtasExpandSpanQuery(newQuery, minimumLeft, maximumLeft,
          minimumRight, maximumRight);
    } else {
      return super.rewrite(reader);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.search.spans.util.MtasSpanQuery#disableTwoPhaseIterator()
   */
  @Override
  public void disableTwoPhaseIterator() {
    super.disableTwoPhaseIterator();
    query.disableTwoPhaseIterator();
  }

  /**
   * The Class MtasExpandWeight.
   */
  private class MtasExpandWeight extends MtasSpanWeight {

    /** The Constant METHOD_GET_DELEGATE. */
    private static final String METHOD_GET_DELEGATE = "getDelegate";

    /** The Constant METHOD_GET_POSTINGS_READER. */
    private static final String METHOD_GET_POSTINGS_READER = "getPostingsReader";

    /** The sub weight. */
    SpanWeight subWeight;

    /**
     * Instantiates a new mtas expand weight.
     *
     * @param subWeight the sub weight
     * @param searcher the searcher
     * @param needsScores the needs scores
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasExpandWeight(SpanWeight subWeight, IndexSearcher searcher,
        boolean needsScores, float boost) throws IOException {
      super(MtasExpandSpanQuery.this, searcher,
          needsScores ? getTermContexts(subWeight) : null, boost);
      this.subWeight = subWeight;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.lucene.search.spans.SpanWeight#extractTermContexts(java.util.
     * Map)
     */
    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      subWeight.extractTermContexts(contexts);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.lucene.search.spans.SpanWeight#getSpans(org.apache.lucene.
     * index.LeafReaderContext,
     * org.apache.lucene.search.spans.SpanWeight.Postings)
     */
    @Override
    public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings)
        throws IOException {
      Spans spans = subWeight.getSpans(ctx, requiredPostings);
      if ((maximumLeft == 0 && maximumRight == 0) || spans == null) {
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
              if (m.getName().equals(METHOD_GET_DELEGATE)) {
                hasMethod = true;
                r = (LeafReader) m.invoke(r, (Object[]) null);
                break;
              }
            }
          } // get fieldsproducer
          Method fpm = r.getClass().getMethod(METHOD_GET_POSTINGS_READER,
              (Class<?>[]) null);
          FieldsProducer fp = (FieldsProducer) fpm.invoke(r, (Object[]) null);
          // get MtasFieldsProducer using terms
          Terms t = fp.terms(field);
          if (t == null) {
            return new MtasSpanMatchNoneSpans(MtasExpandSpanQuery.this);
          } else {
            CodecInfo mtasCodecInfo = CodecInfo.getCodecInfoFromTerms(t);
            return new MtasExpandSpans(MtasExpandSpanQuery.this, mtasCodecInfo,
                query.getField(), spans);
          }
        } catch (Exception e) {
          throw new IOException("Can't get reader", e);
        }

      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lucene.search.Weight#extractTerms(java.util.Set)
     */
    @Override
    public void extractTerms(Set<Term> terms) {
      subWeight.extractTerms(terms);
    }

  }

}
