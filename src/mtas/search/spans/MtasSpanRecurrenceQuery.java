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
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSpanRecurrenceQuery.
 */
public class MtasSpanRecurrenceQuery extends MtasSpanQuery
    implements Cloneable {

  /** The clause. */
  private MtasSpanQuery clause;

  /** The minimum recurrence. */
  private int minimumRecurrence;

  /** The maximum recurrence. */
  private int maximumRecurrence;

  /** The ignore clause. */
  private MtasSpanQuery ignoreClause;

  /** The maximum ignore length. */
  private Integer maximumIgnoreLength;

  /** The field. */
  private String field;

  /**
   * Instantiates a new mtas span recurrence query.
   *
   * @param clause
   *          the clause
   * @param minimumRecurrence
   *          the minimum recurrence
   * @param maximumRecurrence
   *          the maximum recurrence
   * @param ignore
   *          the ignore
   * @param maximumIgnoreLength
   *          the maximum ignore length
   */
  public MtasSpanRecurrenceQuery(MtasSpanQuery clause, int minimumRecurrence,
      int maximumRecurrence, MtasSpanQuery ignore,
      Integer maximumIgnoreLength) {
    super(null, null);
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
    if (field != null && ignore != null) {
      if (ignore.getField() == null || field.equals(ignore.getField())) {
        this.ignoreClause = ignore;
        this.maximumIgnoreLength = maximumIgnoreLength;
      } else {
        throw new IllegalArgumentException(
            "ignore must have same field as clauses");
      }
    } else {
      this.ignoreClause = null;
      this.maximumIgnoreLength = null;
    }
    // set minimum/maximum
    Integer minimum = null, maximum = null;
    if (clause.getMinimumWidth() != null) {
      minimum = minimumRecurrence * clause.getMinimumWidth();
    }
    if (clause.getMaximumWidth() != null) {
      maximum = maximumRecurrence * clause.getMaximumWidth();
      if (ignore != null && maximumIgnoreLength != null) {
        if (ignore.getMaximumWidth() != null) {
          maximum += (maximumRecurrence - 1) * maximumIgnoreLength
              * ignore.getMaximumWidth();
        } else {
          maximum = null;
        }
      }
    }
    setWidth(minimum, maximum);
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
   * @see
   * org.apache.lucene.search.Query#rewrite(org.apache.lucene.index.IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newClause = clause.rewrite(reader);
    MtasSpanQuery newIgnoreClause = (ignoreClause != null)
        ? ignoreClause.rewrite(reader) : null;
    if(newClause instanceof MtasSpanRecurrenceQuery) {
      //for now too difficult, possibly merge later
    }
    if (newClause != clause
        || (newIgnoreClause != null && newIgnoreClause != ignoreClause)) { 
      return new MtasSpanRecurrenceQuery(newClause, minimumRecurrence,
          maximumRecurrence, newIgnoreClause, maximumIgnoreLength).rewrite(reader);
    } else {
      return super.rewrite(reader);
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
    buffer.append(this.getClass().getSimpleName() + "([");
    buffer.append(clause.toString(clause.getField()));
    buffer.append("," + minimumRecurrence + "," + maximumRecurrence);
    buffer.append(", ");
    buffer.append(ignoreClause);
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
    final MtasSpanRecurrenceQuery other = (MtasSpanRecurrenceQuery) obj;
    return clause.equals(other.clause)
        && minimumRecurrence == other.minimumRecurrence
        && maximumRecurrence == other.maximumRecurrence
        && ((ignoreClause == null && other.ignoreClause == null)
            || ignoreClause != null && other.ignoreClause != null
                && ignoreClause.equals(other.ignoreClause));
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
    SpanWeight ignoreWeight = null;
    if (ignoreClause != null) {
      ignoreWeight = ignoreClause.createWeight(searcher, false);
    }
    return new SpanRecurrenceWeight(subWeight, ignoreWeight,
        maximumIgnoreLength, searcher,
        needsScores ? getTermContexts(subWeight) : null);
  }

  /**
   * The Class SpanRecurrenceWeight.
   */
  public class SpanRecurrenceWeight extends SpanWeight {

    /** The sub weight. */
    final SpanWeight subWeight;

    /** The ignore weight. */
    final SpanWeight ignoreWeight;

    /** The maximum ignore length. */
    final Integer maximumIgnoreLength;

    /**
     * Instantiates a new span recurrence weight.
     *
     * @param subWeight
     *          the sub weight
     * @param ignoreWeight
     *          the ignore weight
     * @param maximumIgnoreLength
     *          the maximum ignore length
     * @param searcher
     *          the searcher
     * @param terms
     *          the terms
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public SpanRecurrenceWeight(SpanWeight subWeight, SpanWeight ignoreWeight,
        Integer maximumIgnoreLength, IndexSearcher searcher,
        Map<Term, TermContext> terms) throws IOException {
      super(MtasSpanRecurrenceQuery.this, searcher, terms);
      this.subWeight = subWeight;
      this.ignoreWeight = ignoreWeight;
      this.maximumIgnoreLength = maximumIgnoreLength;
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
        Spans subSpans = subWeight.getSpans(context, requiredPostings);
        if (subSpans == null) {
          return null;
        } else {
          Spans ignoreSpans = null;
          if (ignoreWeight != null) {
            ignoreSpans = ignoreWeight.getSpans(context, requiredPostings);
          }
          return new MtasSpanRecurrenceSpans(MtasSpanRecurrenceQuery.this,
              subSpans, minimumRecurrence, maximumRecurrence, ignoreSpans,
              maximumIgnoreLength);
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
