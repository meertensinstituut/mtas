package mtas.search.spans;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.SpanWithinQuery;

import mtas.search.spans.util.MtasSpanQuery;

public class MtasSpanWithinQuery extends MtasSpanQuery {
  
  /** The base query. */
  private SpanWithinQuery baseQuery;

  public MtasSpanWithinQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super();
    baseQuery = new SpanWithinQuery(q1, q2);
  }
  
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    SpanWithinQuery newBaseQuery = (SpanWithinQuery) baseQuery.rewrite(reader);
    if(newBaseQuery!=baseQuery) {
      try {
        MtasSpanWithinQuery clone = (MtasSpanWithinQuery) this.clone();      
        clone.baseQuery = newBaseQuery;
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new AssertionError(e);
      }
    } else {
      return this;
    }  
  } 

  @Override
  public String getField() {
    return baseQuery.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    SpanWeight sw = baseQuery.createWeight(searcher, needsScores);
    return sw;
    //return baseQuery.createWeight(searcher, needsScores);
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
    final MtasSpanWithinQuery that = (MtasSpanWithinQuery) obj;
    return baseQuery.equals(that.baseQuery);
  }

  @Override
  public int hashCode() {
    return baseQuery.hashCode();
  }

}
