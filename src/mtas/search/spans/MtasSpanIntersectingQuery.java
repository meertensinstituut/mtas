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

public class MtasSpanIntersectingQuery extends MtasSpanQuery {

  private String field;

  private SpanQuery q1, q2;

  public MtasSpanIntersectingQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    if (q1 != null) {
      field = q1.getField();
      if (q2 != null && !q2.getField().equals(field)) {
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
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    if (q1 == null || q2 == null) {
      return null;
    } else {
      MtasSpanIntersectingQueryWeight w1 = new MtasSpanIntersectingQueryWeight(
          q1.createWeight(searcher, needsScores));
      MtasSpanIntersectingQueryWeight w2 = new MtasSpanIntersectingQueryWeight(
          q2.createWeight(searcher, needsScores));
      //subWeights
      List<MtasSpanIntersectingQueryWeight> subWeights = new ArrayList<MtasSpanIntersectingQueryWeight>();      
      subWeights.add(w1);
      subWeights.add(w2);
      //return
      return new SpanIntersectingWeight(w1, w2, searcher, needsScores ? getTermContexts(subWeights) : null);
    }
  }
  
  protected Map<Term, TermContext> getTermContexts(
      List<MtasSpanIntersectingQueryWeight> items) {
    List<SpanWeight> weights = new ArrayList<SpanWeight>();
    for (MtasSpanIntersectingQueryWeight item : items) {
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
    final MtasSpanIntersectingQuery other = (MtasSpanIntersectingQuery) obj;
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
    if(newQ1!=q1 || newQ2!=q2) {
      return new MtasSpanIntersectingQuery(newQ1, newQ2);      
    } else {
      return this;
    }  
}

  public class SpanIntersectingWeight extends SpanWeight {
    
    MtasSpanIntersectingQueryWeight w1,w2;

    public SpanIntersectingWeight(MtasSpanIntersectingQueryWeight w1, MtasSpanIntersectingQueryWeight w2, IndexSearcher searcher,
        Map<Term, TermContext> terms) throws IOException {
      super(MtasSpanIntersectingQuery.this, searcher, terms);
      this.w1=w1;
      this.w2=w2;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      w1.spanWeight.extractTermContexts(contexts);
      w2.spanWeight.extractTermContexts(contexts);
    }

    @Override
    public Spans getSpans(LeafReaderContext context, Postings requiredPostings)
        throws IOException {
      Terms terms = context.reader().terms(field);
      if (terms == null) {
        return null; // field does not exist
      }
      MtasSpanIntersectingQuerySpans s1 = new MtasSpanIntersectingQuerySpans(w1.spanWeight.getSpans(context, requiredPostings));
      MtasSpanIntersectingQuerySpans s2 = new MtasSpanIntersectingQuerySpans(w2.spanWeight.getSpans(context, requiredPostings));
      return new MtasSpanIntersectingSpans(MtasSpanIntersectingQuery.this,
          s1, s2);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      w1.spanWeight.extractTerms(terms);
      w2.spanWeight.extractTerms(terms);
    }

  }

  public class MtasSpanIntersectingQuerySpans {
    public Spans spans;
    
    public MtasSpanIntersectingQuerySpans(Spans spans) {
      this.spans = spans;
    }
    
  }
  
  public class MtasSpanIntersectingQueryWeight {

    /** The span weight. */
    public SpanWeight spanWeight;

    public MtasSpanIntersectingQueryWeight(SpanWeight spanWeight) {
      this.spanWeight = spanWeight;
    }
  }

}
