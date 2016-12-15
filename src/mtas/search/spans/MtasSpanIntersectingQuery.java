package mtas.search.spans;

import java.io.IOException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.util.MtasSpanQuery;

public class MtasSpanIntersectingQuery extends MtasSpanQuery {

  public MtasSpanIntersectingQuery(SpanQuery q1, SpanQuery q2) {
    super();
  }
  
  @Override
  public String getField() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String toString(String field) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int hashCode() {
    // TODO Auto-generated method stub
    return 0;
  }

}
