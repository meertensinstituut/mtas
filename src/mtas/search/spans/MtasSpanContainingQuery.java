package mtas.search.spans;

import java.io.IOException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import mtas.search.spans.util.MtasSpanQuery;

public class MtasSpanContainingQuery extends MtasSpanQuery {
  
  /** The base query. */
  private SpanContainingQuery baseQuery;

  public MtasSpanContainingQuery(SpanQuery q1, SpanQuery q2) {
    super();
    baseQuery = new SpanContainingQuery(q1, q2);
  }

  @Override
  public String getField() {
    return baseQuery.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return baseQuery.createWeight(searcher, needsScores);
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
    final MtasSpanContainingQuery that = (MtasSpanContainingQuery) obj;
    return baseQuery.equals(that.baseQuery);
  }

  @Override
  public int hashCode() {
    return baseQuery.hashCode();
  }

}
