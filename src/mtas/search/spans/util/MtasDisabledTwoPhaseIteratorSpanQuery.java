package mtas.search.spans.util;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;
import mtas.search.spans.MtasSpanMatchNoneQuery;

/**
 * The Class MtasDisabledTwoPhaseIteratorSpanQuery.
 */
public class MtasDisabledTwoPhaseIteratorSpanQuery extends MtasSpanQuery {

  /** The q. */
  private MtasSpanQuery subQuery;

  /**
   * Instantiates a new mtas disabled two phase iterator span query.
   *
   * @param q the q
   */
  public MtasDisabledTwoPhaseIteratorSpanQuery(MtasSpanQuery q) {
    super(q.getMinimumWidth(), q.getMaximumWidth());
    this.subQuery = q;
  }

  /* (non-Javadoc)
   * @see mtas.search.spans.util.MtasSpanQuery#createWeight(org.apache.lucene.search.IndexSearcher, boolean)
   */
  @Override
  public MtasSpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    SpanWeight subWeight = subQuery.createWeight(searcher, needsScores);
    return new MtasDisabledTwoPhaseIteratorWeight(subWeight, searcher,
        needsScores);
  }

  /* (non-Javadoc)
   * @see mtas.search.spans.util.MtasSpanQuery#rewrite(org.apache.lucene.index.IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newQ = subQuery.rewrite(reader);
    if (newQ == null) {
      newQ = new MtasSpanMatchNoneQuery(subQuery.getField());
      return new MtasDisabledTwoPhaseIteratorSpanQuery(newQ);
    } else if (!newQ.equals(subQuery)) {
      return new MtasDisabledTwoPhaseIteratorSpanQuery(newQ).rewrite(reader);
    } else {
      return super.rewrite(reader);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  public String getField() {
    return subQuery.getField();
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.Query#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    return subQuery.toString(field);
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
    final MtasDisabledTwoPhaseIteratorSpanQuery that = (MtasDisabledTwoPhaseIteratorSpanQuery) obj;
    return that.subQuery.equals(subQuery);
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.Query#hashCode()
   */
  @Override
  public int hashCode() {
    int h = Integer.rotateLeft(classHash(), 1);
    h ^= subQuery.hashCode();
    return h;
  }

  /**
   * The Class MtasDisabledTwoPhaseIteratorWeight.
   */
  private class MtasDisabledTwoPhaseIteratorWeight extends MtasSpanWeight {

    /** The sub weight. */
    SpanWeight subWeight;

    /**
     * Instantiates a new mtas disabled two phase iterator weight.
     *
     * @param subWeight the sub weight
     * @param searcher the searcher
     * @param needsScores the needs scores
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MtasDisabledTwoPhaseIteratorWeight(SpanWeight subWeight,
        IndexSearcher searcher, boolean needsScores) throws IOException {
      super(subQuery, searcher, needsScores ? getTermContexts(subWeight) : null);
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
    public MtasSpans getSpans(LeafReaderContext ctx, Postings requiredPostings)
        throws IOException {
      return new MtasDisabledTwoPhaseIteratorSpans(
          subWeight.getSpans(ctx, requiredPostings));
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.Weight#extractTerms(java.util.Set)
     */
    @Override
    public void extractTerms(Set<Term> terms) {
      subWeight.extractTerms(terms);
    }

  }

}
