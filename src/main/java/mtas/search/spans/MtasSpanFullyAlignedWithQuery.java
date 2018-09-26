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
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MtasSpanFullyAlignedWithQuery extends MtasSpanQuery {
  private String field;
  private MtasSpanQuery q1;
  private MtasSpanQuery q2;

  public MtasSpanFullyAlignedWithQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super(q1 != null ? q1.getMinimumWidth() : null,
        q1 != null ? q1.getMaximumWidth() : null);
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
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public MtasSpanWeight createWeight(IndexSearcher searcher,
      boolean needsScores, float boost) throws IOException {
    if (q1 == null || q2 == null) {
      return null;
    } else {
      MtasSpanFullyAlignedWithQueryWeight w1 = new MtasSpanFullyAlignedWithQueryWeight(
          q1.createWeight(searcher, needsScores, boost));
      MtasSpanFullyAlignedWithQueryWeight w2 = new MtasSpanFullyAlignedWithQueryWeight(
          q2.createWeight(searcher, needsScores, boost));
      // subWeights
      List<MtasSpanFullyAlignedWithQueryWeight> subWeights = new ArrayList<>();
      subWeights.add(w1);
      subWeights.add(w2);
      // return
      return new SpanFullyAlignedWithWeight(w1, w2, searcher,
          needsScores ? getTermContexts(subWeights) : null, boost);
    }
  }

  protected Map<Term, TermContext> getTermContexts(
      List<MtasSpanFullyAlignedWithQueryWeight> items) {
    List<SpanWeight> weights = new ArrayList<>();
    for (MtasSpanFullyAlignedWithQueryWeight item : items) {
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
    final MtasSpanFullyAlignedWithQuery other = (MtasSpanFullyAlignedWithQuery) obj;
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
    MtasSpanQuery newQ1 = q1.rewrite(reader);
    MtasSpanQuery newQ2 = q2.rewrite(reader);
    if (newQ1 == null || newQ1 instanceof MtasSpanMatchNoneQuery
        || newQ2 == null || newQ2 instanceof MtasSpanMatchNoneQuery) {
      return new MtasSpanMatchNoneQuery(field);
    } else if (!newQ1.equals(q1) || !newQ2.equals(q2)) {
      return new MtasSpanFullyAlignedWithQuery(newQ1, newQ2).rewrite(reader);
    } else if (newQ1.equals(newQ2)) {
      return newQ1;
    } else {
      boolean returnNone;
      returnNone = newQ1.getMaximumWidth() != null
          && newQ1.getMaximumWidth() == 0;
      returnNone |= newQ2.getMaximumWidth() != null
          && newQ2.getMaximumWidth() == 0;
      returnNone |= newQ1.getMinimumWidth() != null
          && newQ2.getMaximumWidth() != null
          && newQ1.getMinimumWidth() > newQ2.getMaximumWidth();
      returnNone |= newQ2.getMinimumWidth() != null
          && newQ1.getMaximumWidth() != null
          && newQ2.getMinimumWidth() > newQ1.getMaximumWidth();
      returnNone |= newQ2.getMinimumWidth() != null
          && newQ1.getMaximumWidth() != null
          && newQ2.getMinimumWidth() > newQ1.getMaximumWidth();
      if (returnNone) {
        return new MtasSpanMatchNoneQuery(this.getField());
      } else {
        return super.rewrite(reader);
      }
    }
  }

  @Override
  public void disableTwoPhaseIterator() {
    super.disableTwoPhaseIterator();
    q1.disableTwoPhaseIterator();
    q2.disableTwoPhaseIterator();
  }

  protected class SpanFullyAlignedWithWeight extends MtasSpanWeight {
    MtasSpanFullyAlignedWithQueryWeight w1;
    MtasSpanFullyAlignedWithQueryWeight w2;

    public SpanFullyAlignedWithWeight(MtasSpanFullyAlignedWithQueryWeight w1,
        MtasSpanFullyAlignedWithQueryWeight w2, IndexSearcher searcher,
        Map<Term, TermContext> terms, float boost) throws IOException {
      super(MtasSpanFullyAlignedWithQuery.this, searcher, terms, boost);
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
      MtasSpanFullyAlignedWithQuerySpans s1 = new MtasSpanFullyAlignedWithQuerySpans(
          MtasSpanFullyAlignedWithQuery.this,
          w1.spanWeight.getSpans(context, requiredPostings));
      MtasSpanFullyAlignedWithQuerySpans s2 = new MtasSpanFullyAlignedWithQuerySpans(
          MtasSpanFullyAlignedWithQuery.this,
          w2.spanWeight.getSpans(context, requiredPostings));
      return new MtasSpanFullyAlignedWithSpans(
          MtasSpanFullyAlignedWithQuery.this, s1, s2);
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

  protected static class MtasSpanFullyAlignedWithQuerySpans {
    public Spans spans;

    public MtasSpanFullyAlignedWithQuerySpans(
        MtasSpanFullyAlignedWithQuery query, Spans spans) {
      this.spans = spans != null ? spans : new MtasSpanMatchNoneSpans(query);
    }
  }

  private static class MtasSpanFullyAlignedWithQueryWeight {
    public SpanWeight spanWeight;

    public MtasSpanFullyAlignedWithQueryWeight(SpanWeight spanWeight) {
      this.spanWeight = spanWeight;
    }
  }
  
  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }
}
