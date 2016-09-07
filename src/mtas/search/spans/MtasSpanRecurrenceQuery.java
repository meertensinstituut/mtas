package mtas.search.spans;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

/**
 * The Class MtasSpanRecurrenceQuery.
 */
public class MtasSpanRecurrenceQuery extends SpanQuery implements Cloneable {

  /** The clause. */
  private SpanQuery clause;

  /** The minimum recurrence. */
  private int minimumRecurrence;

  /** The maximum recurrence. */
  private int maximumRecurrence;

  /** The field. */
  private String field;

  /** The query name. */
  private static String QUERY_NAME = "mtasSpanRecurrenceQuery";

  /**
   * Instantiates a new mtas span recurrence query.
   *
   * @param clause
   *          the clause
   * @param minimumRecurrence
   *          the minimum recurrence
   * @param maximumRecurrence
   *          the maximum recurrence
   */
  public MtasSpanRecurrenceQuery(SpanQuery clause, int minimumRecurrence,
      int maximumRecurrence) {
    if (minimumRecurrence > maximumRecurrence) {
      throw new IllegalArgumentException(
          "minimumRecurrence > maximumRecurrence");
    } else if (minimumRecurrence < 1) {
      throw new IllegalArgumentException("minimumRecurrence < 1 not supported");
    } else if (clause == null) {
      throw new IllegalArgumentException("no clause");
    }
    this.minimumRecurrence = minimumRecurrence;
    this.maximumRecurrence = maximumRecurrence;
    field = clause.getField();
    this.clause = clause;
  }

  /**
   * Gets the clause.
   *
   * @return the clause
   */
  public SpanQuery getClause() {
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

  // @Override
  // public MtasSpanRecurrenceQuery clone() {
  // MtasSpanRecurrenceQuery soq = new
  // MtasSpanRecurrenceQuery((SpanQuery)clause.clone(), minimumRecurrence,
  // maximumRecurrence);
  // return soq;
  // }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.Query#rewrite(org.apache.lucene.index.IndexReader)
   */
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    SpanQuery query = (SpanQuery) clause.rewrite(reader);
    if (query != clause) { // clause rewrote: must clone
      return new MtasSpanRecurrenceQuery(query, minimumRecurrence,
          maximumRecurrence);
    } else {
      return this; // no rewrote
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("mtasSpanRecurrence([");
    buffer.append(clause.toString(clause.getField()));
    buffer.append("," + minimumRecurrence + "," + maximumRecurrence);
    buffer.append("])");
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
    final MtasSpanRecurrenceQuery that = (MtasSpanRecurrenceQuery) obj;
    return clause.equals(that.clause)
        && minimumRecurrence == that.minimumRecurrence
        && maximumRecurrence == that.maximumRecurrence;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#hashCode()
   */
  @Override
  public int hashCode() {
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ clause.hashCode();
    h = (h * 11) ^ minimumRecurrence;
    h = (h * 13) ^ maximumRecurrence;
    return h;
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
    return new SpanRecurrenceWeight(subWeight, searcher,
        needsScores ? getTermContexts(subWeight) : null);
  }

  /**
   * The Class SpanRecurrenceWeight.
   */
  public class SpanRecurrenceWeight extends SpanWeight {

    /** The sub weight. */
    final SpanWeight subWeight;

    /**
     * Instantiates a new span recurrence weight.
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
    public SpanRecurrenceWeight(SpanWeight subWeight, IndexSearcher searcher,
        Map<Term, TermContext> terms) throws IOException {
      super(MtasSpanRecurrenceQuery.this, searcher, terms);
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
      if (field == null) {
        return null;
      } else {
        Terms terms = context.reader().terms(field);
        if (terms == null) {
          return null; // field does not exist
        }
        Spans subSpan = subWeight.getSpans(context, requiredPostings);
        if (subSpan == null) {
          return null;
        } else {
          return new MtasSpanRecurrence(MtasSpanRecurrenceQuery.this, subSpan,
              minimumRecurrence, maximumRecurrence);
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
