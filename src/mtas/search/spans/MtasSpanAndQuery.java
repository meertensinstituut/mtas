package mtas.search.spans;

import java.io.IOException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.util.MtasExtendedSpanAndQuery;
import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSpanAndQuery.
 */
public class MtasSpanAndQuery extends MtasSpanQuery {

  /** The base query. */
  SpanNearQuery baseQuery;

  /**
   * Instantiates a new mtas span and query.
   *
   * @param clauses the clauses
   */
  public MtasSpanAndQuery(MtasSpanQuery... clauses) {
    super();
    baseQuery = new MtasExtendedSpanAndQuery(clauses);
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return baseQuery.getField();
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.spans.SpanQuery#createWeight(org.apache.lucene.search.IndexSearcher, boolean)
   */
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return baseQuery.createWeight(searcher, needsScores);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.spans.SpanNearQuery#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    return baseQuery.toString(field);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanNearQuery#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanAndQuery that = (MtasSpanAndQuery) obj;
    return baseQuery.equals(that.baseQuery);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanNearQuery#hashCode()
   */
  @Override
  public int hashCode() {
    return baseQuery.hashCode();
  }

}
