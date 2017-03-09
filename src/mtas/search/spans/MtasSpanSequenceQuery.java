package mtas.search.spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
 * The Class MtasSpanSequenceQuery.
 */
public class MtasSpanSequenceQuery extends MtasSpanQuery implements Cloneable {

  /** The items. */
  private List<MtasSpanSequenceItem> items;

  /** The ignore clause. */
  private MtasSpanQuery ignoreClause;

  /** The maximum ignore length. */
  private Integer maximumIgnoreLength;

  /** The field. */
  private String field;

  /**
   * Instantiates a new mtas span sequence query.
   *
   * @param items the items
   * @param ignore the ignore
   * @param maximumIgnoreLength the maximum ignore length
   */
  public MtasSpanSequenceQuery(List<MtasSpanSequenceItem> items,
      MtasSpanQuery ignore, Integer maximumIgnoreLength) {    
    this.items = items;
    // get field and do checks
    for (MtasSpanSequenceItem item : items) {
      if (field == null) {
        field = item.getQuery().getField();
      } else if (item.getQuery().getField() != null
          && !item.getQuery().getField().equals(field)) {
        throw new IllegalArgumentException("Clauses must have same field.");
      }
    }
    //check ignore
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
   * @see java.lang.Object#clone()
   */
  @Override
  public MtasSpanSequenceQuery clone() {
    int sz = items.size();
    List<MtasSpanSequenceItem> newItems = new ArrayList<MtasSpanSequenceItem>();
    for (int i = 0; i < sz; i++) {
      newItems.add(items.get(i).clone());
    }
    MtasSpanSequenceQuery soq = new MtasSpanSequenceQuery(newItems,
        ignoreClause, maximumIgnoreLength);
    return soq;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.Query#rewrite(org.apache.lucene.index.IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanSequenceQuery clone = null;
    for (int i = 0; i < items.size(); i++) {
      MtasSpanQuery c = items.get(i).getQuery();
      MtasSpanQuery query = (MtasSpanQuery) c.rewrite(reader);
      if (query != c) { // clause rewrote: must clone
        if (clone == null) {
          clone = this.clone();
        }
        clone.items.get(i).setQuery(query);
      }
    }
    if (ignoreClause != null) {
      MtasSpanQuery query = (MtasSpanQuery) ignoreClause.rewrite(reader);
      if (query != ignoreClause) {
        clone = this.clone();
        clone.ignoreClause = query;
      }
    }
    if (clone != null) {
      return clone; // some clauses rewrote
    } else {
      return this; // no clauses rewrote
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
    Iterator<MtasSpanSequenceItem> i = items.iterator();
    while (i.hasNext()) {
      MtasSpanSequenceItem item = i.next();
      MtasSpanQuery clause = item.getQuery();
      buffer.append(clause.toString(field));
      if (item.isOptional()) {
        buffer.append("{OPTIONAL}");
      }
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("]");
    buffer.append(", ");
    buffer.append(ignoreClause);
    buffer.append(")");
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
    MtasSpanSequenceQuery other = (MtasSpanSequenceQuery) obj;
    return field.equals(other.field) && items.equals(other.items)
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
    h = (h * 3) ^ field.hashCode();
    h = (h * 5) ^ items.hashCode();
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
    List<MtasSpanSequenceQueryWeight> subWeights = new ArrayList<MtasSpanSequenceQueryWeight>();
    SpanWeight ignoreWeight = null;
    for (MtasSpanSequenceItem item : items) {
      subWeights.add(new MtasSpanSequenceQueryWeight(
          item.getQuery().createWeight(searcher, false), item.isOptional()));
    }
    if (ignoreClause != null) {
      ignoreWeight = ignoreClause.createWeight(searcher, false);
    }
    return new SpanSequenceWeight(subWeights, ignoreWeight, maximumIgnoreLength,
        searcher, needsScores ? getTermContexts(subWeights) : null);
  }

  /**
   * Gets the term contexts.
   *
   * @param items the items
   * @return the term contexts
   */
  protected Map<Term, TermContext> getTermContexts(
      List<MtasSpanSequenceQueryWeight> items) {
    List<SpanWeight> weights = new ArrayList<SpanWeight>();
    for (MtasSpanSequenceQueryWeight item : items) {
      weights.add(item.spanWeight);
    }
    return getTermContexts(weights);
  }

  /**
   * The Class SpanSequenceWeight.
   */
  public class SpanSequenceWeight extends SpanWeight {

    /** The sub weights. */
    final List<MtasSpanSequenceQueryWeight> subWeights;

    /** The ignore weight. */
    final SpanWeight ignoreWeight;

    /** The maximum ignore length. */
    final Integer maximumIgnoreLength;

    /**
     * Instantiates a new span sequence weight.
     *
     * @param subWeights the sub weights
     * @param ignoreWeight the ignore weight
     * @param maximumIgnoreLength the maximum ignore length
     * @param searcher the searcher
     * @param terms the terms
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public SpanSequenceWeight(List<MtasSpanSequenceQueryWeight> subWeights,
        SpanWeight ignoreWeight, Integer maximumIgnoreLength,
        IndexSearcher searcher, Map<Term, TermContext> terms)
        throws IOException {
      super(MtasSpanSequenceQuery.this, searcher, terms);
      this.subWeights = subWeights;
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
      for (MtasSpanSequenceQueryWeight w : subWeights) {
        w.spanWeight.extractTermContexts(contexts);
      }
      if (ignoreWeight != null) {
        ignoreWeight.extractTermContexts(contexts);
      }
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
      List<MtasSpanSequenceQuerySpans> setSequenceSpans = new ArrayList<>(
          items.size());
      Spans ignoreSpans = null;
      boolean allSpansEmpty = true;
      for (MtasSpanSequenceQueryWeight w : subWeights) {
        Spans sequenceSpans = w.spanWeight.getSpans(context, requiredPostings);
        if (sequenceSpans != null) {
          setSequenceSpans
              .add(new MtasSpanSequenceQuerySpans(sequenceSpans, w.optional));
          allSpansEmpty = false;
        } else {
          if (w.optional) {
            setSequenceSpans
                .add(new MtasSpanSequenceQuerySpans(null, w.optional));
          } else {
            return null;
          }
        }
      }
      if (allSpansEmpty) {
        return null; // at least one required
      } else if (ignoreWeight != null) {
        ignoreSpans = ignoreWeight.getSpans(context, requiredPostings);
      }
      return new MtasSpanSequenceSpans(MtasSpanSequenceQuery.this,
          setSequenceSpans, ignoreSpans, maximumIgnoreLength);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lucene.search.Weight#extractTerms(java.util.Set)
     */
    @Override
    public void extractTerms(Set<Term> terms) {
      for (MtasSpanSequenceQueryWeight w : subWeights) {
        w.spanWeight.extractTerms(terms);
      }
      if (ignoreWeight != null) {
        ignoreWeight.extractTerms(terms);
      }
    }

  }

  /**
   * The Class MtasSpanSequenceQuerySpans.
   */
  public class MtasSpanSequenceQuerySpans {

    /** The spans. */
    public Spans spans;

    /** The optional. */
    public boolean optional;

    /**
     * Instantiates a new mtas span sequence query spans.
     *
     * @param spans the spans
     * @param optional the optional
     */
    public MtasSpanSequenceQuerySpans(Spans spans, boolean optional) {
      this.spans = spans;
      this.optional = optional;
    }
  }

  /**
   * The Class MtasSpanSequenceQueryWeight.
   */
  public class MtasSpanSequenceQueryWeight {

    /** The span weight. */
    public SpanWeight spanWeight;

    /** The optional. */
    public boolean optional;

    /**
     * Instantiates a new mtas span sequence query weight.
     *
     * @param spanWeight the span weight
     * @param optional the optional
     */
    public MtasSpanSequenceQueryWeight(SpanWeight spanWeight,
        boolean optional) {
      this.spanWeight = spanWeight;
      this.optional = optional;
    }
  }

}
