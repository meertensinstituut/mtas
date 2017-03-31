package mtas.search.spans;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.util.MtasSpanQuery;

public class MtasSpanNotQuery extends MtasSpanQuery {
  private String field;
  
  /** The base query. */
  private SpanNotQuery baseQuery;
  
  private SpanQuery q1, q2;

  public MtasSpanNotQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super(q1!=null?q1.getMinimumWidth():null, q2!=null?q2.getMaximumWidth():null);
    if (q1 != null && (field = q1.getField()) != null) {
      if (q2 != null && q2.getField()!=null && ((field == null && q2.getField() != null)
          || !q2.getField().equals(field))) {
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

  @Override
  public String getField() {
    return field;
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return baseQuery.createWeight(searcher, needsScores);
  }

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
