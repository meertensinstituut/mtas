package mtas.search.spans;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSpanStartQuery.
 */
public class MtasSpanStartQuery extends MtasSpanQuery {

  /** The query. */
  private MtasSpanQuery clause;

  /**
   * Instantiates a new mtas span start query.
   *
   * @param query
   *          the query
   */
  public MtasSpanStartQuery(MtasSpanQuery query) {
    super(0, 0);
    clause = query;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.Query#rewrite(org.apache.lucene.index.IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newClause = clause.rewrite(reader);
    if (newClause != clause) {
      return new MtasSpanStartQuery(newClause).rewrite(reader);
    } else if(newClause.getMaximumWidth()!=null && newClause.getMaximumWidth()==0) {
      return newClause;
    } else {
      return super.rewrite(reader);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.spans.SpanTermQuery#toString(java.lang.String)
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
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return clause.getField();
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
    SpanWeight spanWeight = ((SpanQuery) searcher.rewrite(clause))
        .createWeight(searcher, needsScores);
    return new SpanTermWeight(spanWeight, searcher);
  }

  /**
   * The Class SpanTermWeight.
   */
  protected class SpanTermWeight extends SpanWeight {

    /** The span weight. */
    SpanWeight spanWeight;

    /**
     * Instantiates a new span term weight.
     *
     * @param spanWeight
     *          the span weight
     * @param searcher
     *          the searcher
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public SpanTermWeight(SpanWeight spanWeight, IndexSearcher searcher)
        throws IOException {
      super(MtasSpanStartQuery.this, searcher, null);
      this.spanWeight = spanWeight;
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
      spanWeight.extractTermContexts(contexts);
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
      return new MtasSpanStartSpans(spanWeight.getSpans(ctx, requiredPostings));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lucene.search.Weight#extractTerms(java.util.Set)
     */
    @Override
    public void extractTerms(Set<Term> terms) {
      spanWeight.extractTerms(terms);
    }

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
    final MtasSpanStartQuery that = (MtasSpanStartQuery) obj;
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

}
