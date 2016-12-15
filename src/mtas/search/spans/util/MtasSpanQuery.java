package mtas.search.spans.util;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spans.SpanQuery;

public abstract class MtasSpanQuery extends SpanQuery {
 

  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    return this;
  }
  
}
