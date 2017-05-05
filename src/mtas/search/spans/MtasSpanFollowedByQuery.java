package mtas.search.spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import mtas.search.spans.util.MtasSpanQuery;

public class MtasSpanFollowedByQuery extends MtasSpanQuery {
  
  /** The field. */
  private String field;

  /** The q 2. */
  private SpanQuery q1;
  private SpanQuery q2;

  public MtasSpanFollowedByQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super(q1 != null ? q1.getMinimumWidth() : null,
        q1 != null ? q1.getMaximumWidth() : null);
    if (q1 != null && (field = q1.getField()) != null) {
      if (q2 != null && ((field == null && q2.getField() != null)
          || !q2.getField().equals(field))) {
        throw new IllegalArgumentException("Clauses must have same field.");
      }
    } else if (q2 != null) {
      field = q2.getField();
    } else {
      field = null;
    }
    this.q1 = q1;
    this.q2 = q2;
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
  
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    if (q1 == null || q2 == null) {
      return null;
    } else {
      MtasSpanFollowedByQueryWeight w1 = new MtasSpanFollowedByQueryWeight(
          q1.createWeight(searcher, needsScores));
      MtasSpanFollowedByQueryWeight w2 = new MtasSpanFollowedByQueryWeight(
          q2.createWeight(searcher, needsScores));
      // subWeights
      List<MtasSpanFollowedByQueryWeight> subWeights = new ArrayList<MtasSpanFollowedByQueryWeight>();
      subWeights.add(w1);
      subWeights.add(w2);
      // return
      return new SpanFollowedByWeight(w1, w2, searcher,
          needsScores ? getTermContexts(subWeights) : null);
    }
  }
  
  protected Map<Term, TermContext> getTermContexts(
      List<MtasSpanFollowedByQueryWeight> items) {
    List<SpanWeight> weights = new ArrayList<SpanWeight>();
    for (MtasSpanFollowedByQueryWeight item : items) {
      weights.add(item.spanWeight);
    }
    return getTermContexts(weights);
  }
  
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    if (q1 != null) {
      buffer.append(q1.toString(q1.getField()));
    } else {
      buffer.append("null");
    }
    buffer.append(",");
    if (q2 != null) {
      buffer.append(q2.toString(q2.getField()));
    } else {
      buffer.append("null");
    }
    buffer.append("])");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanFollowedByQuery other = (MtasSpanFollowedByQuery) obj;
    return q1.equals(other.q1) && q2.equals(other.q2);
  }

  @Override
  public int hashCode() {
    int h = Integer.rotateLeft(classHash(), 1);
    h ^= q1.hashCode();
    h = Integer.rotateLeft(h, 1);
    h ^= q2.hashCode();
    return h;
  }
  
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newQ1 = (MtasSpanQuery) q1.rewrite(reader);
    MtasSpanQuery newQ2 = (MtasSpanQuery) q2.rewrite(reader);
    if(newQ1==null || newQ1 instanceof MtasSpanMatchNoneQuery || newQ2==null || newQ2 instanceof MtasSpanMatchNoneQuery) {
      return new MtasSpanMatchNoneQuery(field);      
    } else if (newQ1 != q1 || newQ2 != q2) {
      return new MtasSpanFollowedByQuery(newQ1, newQ2).rewrite(reader);
    } else if (newQ1 == null || newQ2 == null) {
      return new MtasSpanMatchNoneQuery(this.getField());
    } else {
      return super.rewrite(reader);
    }
  }
  
  public class SpanFollowedByWeight extends SpanWeight {

    /** The w 2. */
    MtasSpanFollowedByQueryWeight w1, w2;

    /**
     * Instantiates a new span intersecting weight.
     *
     * @param w1
     *          the w 1
     * @param w2
     *          the w 2
     * @param searcher
     *          the searcher
     * @param terms
     *          the terms
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public SpanFollowedByWeight(MtasSpanFollowedByQueryWeight w1,
        MtasSpanFollowedByQueryWeight w2, IndexSearcher searcher,
        Map<Term, TermContext> terms) throws IOException {
      super(MtasSpanFollowedByQuery.this, searcher, terms);
      this.w1 = w1;
      this.w2 = w2;
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
      w1.spanWeight.extractTermContexts(contexts);
      w2.spanWeight.extractTermContexts(contexts);
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
      MtasSpanFollowedByQuerySpans s1 = new MtasSpanFollowedByQuerySpans(
          w1.spanWeight.getSpans(context, requiredPostings));
      MtasSpanFollowedByQuerySpans s2 = new MtasSpanFollowedByQuerySpans(
          w2.spanWeight.getSpans(context, requiredPostings));
      return new MtasSpanFollowedBySpans(
          MtasSpanFollowedByQuery.this, s1, s2);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lucene.search.Weight#extractTerms(java.util.Set)
     */
    @Override
    public void extractTerms(Set<Term> terms) {
      w1.spanWeight.extractTerms(terms);
      w2.spanWeight.extractTerms(terms);
    }

  }

  /**
   * The Class MtasSpanIntersectingQuerySpans.
   */
  public class MtasSpanFollowedByQuerySpans {

    /** The spans. */
    public Spans spans;

    /**
     * Instantiates a new mtas span intersecting query spans.
     *
     * @param spans
     *          the spans
     */
    public MtasSpanFollowedByQuerySpans(Spans spans) {
      this.spans = spans!=null?spans:new MtasSpanMatchNoneSpans(field);
    }

  }

  /**
   * The Class MtasSpanIntersectingQueryWeight.
   */
  public class MtasSpanFollowedByQueryWeight {

    /** The span weight. */
    public SpanWeight spanWeight;

    /**
     * Instantiates a new mtas span intersecting query weight.
     *
     * @param spanWeight
     *          the span weight
     */
    public MtasSpanFollowedByQueryWeight(SpanWeight spanWeight) {
      this.spanWeight = spanWeight;
    }
  }

}


