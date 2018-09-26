package mtas.search.spans;

import mtas.search.spans.util.MtasSpanQuery;
import mtas.search.spans.util.MtasSpanWeight;
import mtas.search.spans.util.MtasSpans;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MtasSpanNotQuery extends MtasSpanQuery {
  private String field;
  private SpanNotQuery baseQuery;
  private MtasSpanQuery q1;
  private MtasSpanQuery q2;

  public MtasSpanNotQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super(q1 != null ? q1.getMinimumWidth() : null,
        q2 != null ? q2.getMaximumWidth() : null);
    if (q1 != null && (field = q1.getField()) != null) {
      if (q2 != null && q2.getField() != null && !q2.getField().equals(field)) {
        throw new IllegalArgumentException("Clauses must have same field.");
      }
    } else if (q2 != null) {
      field = q2.getField();
    } else {
      field = null;
    }
    this.q1 = q1;
    this.q2 = q2;
    baseQuery = new SpanNotQuery(q1, q2);
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public MtasSpanWeight createWeight(IndexSearcher searcher,
      boolean needsScores, float boost) throws IOException {
    // return baseQuery.createWeight(searcher, needsScores);
    if (q1 == null || q2 == null) {
      return null;
    } else {
      MtasSpanNotQueryWeight w1 = new MtasSpanNotQueryWeight(
          q1.createWeight(searcher, needsScores, boost));
      MtasSpanNotQueryWeight w2 = new MtasSpanNotQueryWeight(
          q2.createWeight(searcher, needsScores, boost));
      // subWeights
      List<MtasSpanNotQueryWeight> subWeights = new ArrayList<>();
      subWeights.add(w1);
      subWeights.add(w2);
      // return
      return new SpanNotWeight(w1, w2, searcher,
          needsScores ? getTermContexts(subWeights) : null, boost);
    }
  }

  protected Map<Term, TermContext> getTermContexts(
      List<MtasSpanNotQueryWeight> items) {
    List<SpanWeight> weights = new ArrayList<>();
    for (MtasSpanNotQueryWeight item : items) {
      weights.add(item.spanWeight);
    }
    return getTermContexts(weights);
  }

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newQ1 = q1.rewrite(reader);
    MtasSpanQuery newQ2 = q2.rewrite(reader);
    if (!newQ1.equals(q1) || !newQ2.equals(q2)) {
      return new MtasSpanNotQuery(newQ1, newQ2).rewrite(reader);
    } else {
      baseQuery = (SpanNotQuery) baseQuery.rewrite(reader);
      return super.rewrite(reader);
    }
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
    final MtasSpanNotQuery that = (MtasSpanNotQuery) obj;
    return baseQuery.equals(that.baseQuery);
  }

  @Override
  public int hashCode() {
    return baseQuery.hashCode();
  }

  @Override
  public void disableTwoPhaseIterator() {
    super.disableTwoPhaseIterator();
    q1.disableTwoPhaseIterator();
    q2.disableTwoPhaseIterator();
  }

  protected class SpanNotWeight extends MtasSpanWeight {
    MtasSpanNotQueryWeight w1;
    MtasSpanNotQueryWeight w2;

    public SpanNotWeight(MtasSpanNotQueryWeight w1, MtasSpanNotQueryWeight w2,
        IndexSearcher searcher, Map<Term, TermContext> termContexts, float boost)
        throws IOException {
      super(MtasSpanNotQuery.this, searcher, termContexts, boost);
      this.w1 = w1;
      this.w2 = w2;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      w1.spanWeight.extractTermContexts(contexts);
      w2.spanWeight.extractTermContexts(contexts);
    }

    @Override
    public MtasSpans getSpans(LeafReaderContext context,
        Postings requiredPostings) throws IOException {
      Terms terms = context.reader().terms(field);
      if (terms == null) {
        return null; // field does not exist
      }
      MtasSpanNotQuerySpans s1 = new MtasSpanNotQuerySpans(
          MtasSpanNotQuery.this,
          w1.spanWeight.getSpans(context, requiredPostings));
      MtasSpanNotQuerySpans s2 = new MtasSpanNotQuerySpans(
          MtasSpanNotQuery.this,
          w2.spanWeight.getSpans(context, requiredPostings));
      return new MtasSpanNotSpans(MtasSpanNotQuery.this, s1, s2);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      w1.spanWeight.extractTerms(terms);
      w2.spanWeight.extractTerms(terms);
    }
    
//    @Override
//    public boolean isCacheable(LeafReaderContext arg0) {
//      return w1.spanWeight.isCacheable(arg0) && w2.spanWeight.isCacheable(arg0);
//    }

  }

  protected static class MtasSpanNotQuerySpans {
    public Spans spans;

    public MtasSpanNotQuerySpans(MtasSpanNotQuery query, Spans spans) {
      this.spans = spans != null ? spans : new MtasSpanMatchNoneSpans(query);
    }
  }

  private static class MtasSpanNotQueryWeight {
    public SpanWeight spanWeight;

    public MtasSpanNotQueryWeight(SpanWeight spanWeight) {
      this.spanWeight = spanWeight;
    }
  }
  
  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }
}
