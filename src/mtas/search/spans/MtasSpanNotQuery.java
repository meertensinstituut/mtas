package mtas.search.spans;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSpanNotQuery.
 */
public class MtasSpanNotQuery extends MtasSpanQuery {
  
  /** The field. */
  private String field;
  
  /** The base query. */
  private SpanNotQuery baseQuery;
  
  /** The q 1. */
  private SpanQuery q1;
  
  /** The q 2. */
  private SpanQuery q2;

  /**
   * Instantiates a new mtas span not query.
   *
   * @param q1 the q 1
   * @param q2 the q 2
   */
  public MtasSpanNotQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super(q1!=null?q1.getMinimumWidth():null, q2!=null?q2.getMaximumWidth():null);
    if (q1 != null && (field = q1.getField()) != null) {
      if (q2 != null && q2.getField()!=null && !q2.getField().equals(field)) {
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

  /* (non-Javadoc)
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return field;
  }

  /* (non-Javadoc)
   * @see mtas.search.spans.util.MtasSpanQuery#createWeight(org.apache.lucene.search.IndexSearcher, boolean)
   */
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return baseQuery.createWeight(searcher, needsScores);
  }

  /* (non-Javadoc)
   * @see mtas.search.spans.util.MtasSpanQuery#rewrite(org.apache.lucene.index.IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newQ1 = (MtasSpanQuery) q1.rewrite(reader);
    MtasSpanQuery newQ2 = (MtasSpanQuery) q2.rewrite(reader);
    if (newQ1 != q1 || newQ2 != q2) {
      return new MtasSpanNotQuery(newQ1, newQ2).rewrite(reader);
    } else {
      return super.rewrite(reader);
    }    
  }
  
  /* (non-Javadoc)
   * @see org.apache.lucene.search.Query#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    return baseQuery.toString(field);
  }

  /* (non-Javadoc)
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
    final MtasSpanNotQuery that = (MtasSpanNotQuery) obj;
    return baseQuery.equals(that.baseQuery);
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.Query#hashCode()
   */
  @Override
  public int hashCode() {
    return baseQuery.hashCode();
  }
  
}
