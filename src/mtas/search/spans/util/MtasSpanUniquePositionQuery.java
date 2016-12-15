package mtas.search.spans.util;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import mtas.search.similarities.MtasSimScorer;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

/**
 * The Class MtasSpanUniquePositionQuery.
 */
public class MtasSpanUniquePositionQuery extends MtasSpanQuery {

  /** The clause. */
  private MtasSpanQuery clause;

  /** The field. */
  private String field;

  /**
   * Instantiates a new mtas span unique position query.
   *
   * @param clause
   *          the clause
   */
  public MtasSpanUniquePositionQuery(MtasSpanQuery clause) {
    field = clause.getField();
    this.clause = clause;
  }

  /**
   * Gets the clause.
   *
   * @return the clause
   */
  public MtasSpanQuery getClause() {
    return clause;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return field;
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
    final MtasSpanUniquePositionQuery that = (MtasSpanUniquePositionQuery) obj;
    return clause.equals(that.clause);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#hashCode()
   */
  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ clause.hashCode();
    return h;
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
    buffer.append(clause.toString(field));
    buffer.append("])");
    return buffer.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.spans.SpanQuery#createWeight(org.apache.lucene.
   * search.IndexSearcher, boolean)
   */
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    SpanWeight subWeight = clause.createWeight(searcher, false);
    return new SpanUniquePositionWeight(subWeight, searcher,
        needsScores ? getTermContexts(subWeight) : null);
  }

  /**
   * The Class SpanUniquePositionWeight.
   */
  public class SpanUniquePositionWeight extends SpanWeight {

    /** The sub weight. */
    final SpanWeight subWeight;

    /**
     * Instantiates a new span unique position weight.
     *
     * @param subWeight
     *          the sub weight
     * @param searcher
     *          the searcher
     * @param terms
     *          the terms
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public SpanUniquePositionWeight(SpanWeight subWeight,
        IndexSearcher searcher, Map<Term, TermContext> terms)
        throws IOException {
      super(MtasSpanUniquePositionQuery.this, searcher, terms);
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
    public Spans getSpans(LeafReaderContext context, Postings requiredPostings)
        throws IOException {
      Terms terms = context.reader().terms(field);
      if (terms == null) {
        return null; // field does not exist
      }

      Spans subSpan = subWeight.getSpans(context, requiredPostings);
      if (subSpan == null) {
        return null;
      } else {
        SimScorer scorer = getSimScorer(context);
        if (scorer == null) {
          scorer = new MtasSimScorer();
        }
        return new MtasSpanUniquePosition(MtasSpanUniquePositionQuery.this,
            subSpan);
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
