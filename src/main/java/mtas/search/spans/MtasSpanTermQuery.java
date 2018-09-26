package mtas.search.spans;

import mtas.search.spans.util.MtasExtendedSpanTermQuery;
import mtas.search.spans.util.MtasSpanQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;

import java.io.IOException;

public class MtasSpanTermQuery extends MtasSpanQuery {
  private MtasExtendedSpanTermQuery baseQuery;

  public MtasSpanTermQuery(Term term) {
    this(term, true);
  }

  public MtasSpanTermQuery(Term term, boolean singlePosition) {
    this(new SpanTermQuery(term), true);
  }

  public MtasSpanTermQuery(SpanTermQuery query, boolean singlePosition) {
    super(singlePosition ? 1 : null, singlePosition ? 1 : null);
    baseQuery = new MtasExtendedSpanTermQuery(query, singlePosition);
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost)
      throws IOException {
    return baseQuery.createWeight(searcher, needsScores, boost);
  }

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    baseQuery = (MtasExtendedSpanTermQuery) baseQuery.rewrite(reader);
    return super.rewrite(reader);
  }

  @Override
  public String toString(String field) {
    return baseQuery.toString(field);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MtasSpanTermQuery other = (MtasSpanTermQuery) obj;
    return baseQuery.equals(other.baseQuery);
  }

  @Override
  public String getField() {
    return baseQuery.getField();
  }

  @Override
  public int hashCode() {
    return baseQuery.hashCode();
  }

  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }
}
