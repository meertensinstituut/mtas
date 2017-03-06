package mtas.search.spans.util;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

public abstract class MtasSpanWeight extends SpanWeight {

  public MtasSpanWeight(SpanQuery query, IndexSearcher searcher,
      Map<Term, TermContext> termContexts) throws IOException {
    super(query, searcher, termContexts);
    //TODO
  }
   
  
}
