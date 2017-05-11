package mtas.search.spans;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSpanContainingQuery.
 */
public class MtasSpanContainingQuery extends MtasSpanQuery {

  /** The base query. */
  private SpanContainingQuery baseQuery;
  
  /** The big query. */
  private MtasSpanQuery bigQuery;
  
  /** The small query. */
  private MtasSpanQuery smallQuery;
  
  /** The field. */
  private String field;

  /**
   * Instantiates a new mtas span containing query.
   *
   * @param q1 the q 1
   * @param q2 the q 2
   */
  public MtasSpanContainingQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super(q1 != null ? q1.getMinimumWidth() : null,
        q1 != null ? q1.getMaximumWidth() : null);
    if (q2 != null && q2.getMinimumWidth() != null
        && (this.getMinimumWidth() == null
            || this.getMinimumWidth() < q2.getMinimumWidth())) {
      this.setWidth(q2.getMinimumWidth(), this.getMaximumWidth());
    }
    bigQuery = q1;
    smallQuery = q2;
    if (bigQuery != null && bigQuery.getField() != null) {
      field = bigQuery.getField();
    } else if (smallQuery != null && smallQuery.getField() != null) {
      field = smallQuery.getField();
    } else {
      field = null;
    }
    if (field != null) {
      baseQuery = new SpanContainingQuery(bigQuery, smallQuery);
    } else {
      baseQuery = null;
    }
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
    return baseQuery.createWeight(searcher, needsScores);
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
   * @see mtas.search.spans.util.MtasSpanQuery#rewrite(org.apache.lucene.index.
   * IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newBigQuery = bigQuery.rewrite(reader);
    MtasSpanQuery newSmallQuery = smallQuery.rewrite(reader);
    if (newBigQuery == null || newBigQuery instanceof MtasSpanMatchNoneQuery
        || newSmallQuery == null
        || newSmallQuery instanceof MtasSpanMatchNoneQuery) {
      return new MtasSpanMatchNoneQuery(field);
    }

    if (!newBigQuery.equals(bigQuery) || !newSmallQuery.equals(smallQuery)) {
      return new MtasSpanContainingQuery(newBigQuery, newSmallQuery)
          .rewrite(reader);
    } else if (newBigQuery.equals(newSmallQuery)) {
      return newBigQuery;
    } else {
      baseQuery = (SpanContainingQuery) baseQuery.rewrite(reader);
      return super.rewrite(reader);
    }
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
    final MtasSpanContainingQuery that = (MtasSpanContainingQuery) obj;
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
