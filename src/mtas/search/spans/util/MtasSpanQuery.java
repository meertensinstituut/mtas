package mtas.search.spans.util;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

public abstract class MtasSpanQuery extends SpanQuery {

  public MtasSpanQuery() {
    super();
  }
  
  @Override
  public abstract SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException;

  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    return this;
  }

}
