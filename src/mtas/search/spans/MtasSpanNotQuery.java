package mtas.search.spans;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.util.MtasSpanQuery;

public class MtasSpanNotQuery extends MtasSpanQuery {
  /** The base query. */
  private SpanNotQuery baseQuery;

  public MtasSpanNotQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super();
    baseQuery = new SpanNotQuery(q1, q2);
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
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    baseQuery = (SpanNotQuery) baseQuery.rewrite(reader);
    return this;
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
    final MtasSpanNotQuery that = (MtasSpanNotQuery) obj;
    return baseQuery.equals(that.baseQuery);
  }

  @Override
  public int hashCode() {
    return baseQuery.hashCode();
  }
  
}
