package mtas.search.spans;

import mtas.search.spans.util.MtasSpanQuery;
import mtas.search.spans.util.MtasSpanWeight;
import mtas.search.spans.util.MtasSpans;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class MtasSpanEndQuery extends MtasSpanQuery {
  private MtasSpanQuery clause;

  public MtasSpanEndQuery(MtasSpanQuery query) {
    super(0, 0);
    clause = query;
  }

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newClause = clause.rewrite(reader);
    if (!newClause.equals(clause)) {
      return new MtasSpanEndQuery(newClause).rewrite(reader);
    } else if (newClause.getMaximumWidth() != null
        && newClause.getMaximumWidth() == 0) {
      return newClause;
    } else {
      return super.rewrite(reader);
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    buffer.append(clause.toString(field));
    buffer.append("])");
    return buffer.toString();
  }

  @Override
  public String getField() {
    return clause.getField();
  }

  @Override
  public MtasSpanWeight createWeight(IndexSearcher searcher,
      boolean needsScores, float boost) throws IOException {
    SpanWeight spanWeight = ((SpanQuery) searcher.rewrite(clause))
        .createWeight(searcher, needsScores, boost);
    return new SpanTermWeight(spanWeight, searcher, boost);
  }

  public class SpanTermWeight extends MtasSpanWeight {

    SpanWeight spanWeight;

    public SpanTermWeight(SpanWeight spanWeight, IndexSearcher searcher, float boost)
        throws IOException {
      super(MtasSpanEndQuery.this, searcher, null, boost);
      this.spanWeight = spanWeight;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      spanWeight.extractTermContexts(contexts);
    }

    @Override
    public MtasSpans getSpans(LeafReaderContext context,
        Postings requiredPostings) throws IOException {
      return new MtasSpanEndSpans(MtasSpanEndQuery.this,
          spanWeight.getSpans(context, requiredPostings));
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      spanWeight.extractTerms(terms);
    }

//    @Override
//    public boolean isCacheable(LeafReaderContext arg0) {
//      return spanWeight.isCacheable(arg0);
//    }

  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanEndQuery that = (MtasSpanEndQuery) obj;
    return clause.equals(that.clause);
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ clause.hashCode();
    return h;
  }

  @Override
  public void disableTwoPhaseIterator() {
    super.disableTwoPhaseIterator();
    clause.disableTwoPhaseIterator();
  }
  
  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }

}
