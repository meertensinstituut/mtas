package mtas.search.spans;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.SpanWithinQuery;

import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSpanWithinQuery.
 */
public class MtasSpanWithinQuery extends MtasSpanQuery {

  /** The base query. */
  private SpanWithinQuery baseQuery;

  /**
   * Instantiates a new mtas span within query.
   *
   * @param q1 the q1
   * @param q2 the q2
   */
  public MtasSpanWithinQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super();
    baseQuery = new SpanWithinQuery(q1, q2);
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.search.spans.util.MtasSpanQuery#rewrite(org.apache.lucene.index.
   * IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    baseQuery = (SpanWithinQuery) baseQuery.rewrite(reader);
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return baseQuery.getField();
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
    SpanWeight sw = baseQuery.createWeight(searcher, needsScores);
    return sw;
    // return baseQuery.createWeight(searcher, needsScores);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    return baseQuery.toString(field);
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
    final MtasSpanWithinQuery that = (MtasSpanWithinQuery) obj;
    return baseQuery.equals(that.baseQuery);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#hashCode()
   */
  @Override
  public int hashCode() {
    return baseQuery.hashCode();
  }

}
