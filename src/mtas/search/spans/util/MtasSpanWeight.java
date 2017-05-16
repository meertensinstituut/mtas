package mtas.search.spans.util;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

/**
 * The Class MtasSpanWeight.
 */
public abstract class MtasSpanWeight extends SpanWeight {

  /**
   * Instantiates a new mtas span weight.
   *
   * @param query the query
   * @param searcher the searcher
   * @param termContexts the term contexts
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasSpanWeight(SpanQuery query, IndexSearcher searcher,
      Map<Term, TermContext> termContexts) throws IOException {
    super(query, searcher, termContexts);
    //TODO
  }
   
  
}
