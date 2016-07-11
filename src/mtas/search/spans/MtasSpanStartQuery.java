package mtas.search.spans;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

/**
 * The Class MtasSpanTermQuery.
 */
public class MtasSpanStartQuery extends SpanQuery {

  private SpanQuery query;
  private static String QUERY_NAME = "mtasSpanStartQuery";

  public MtasSpanStartQuery(SpanQuery query) {
    super();
    this.query = query;    
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    query.rewrite(reader);
    return this;  
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.spans.SpanTermQuery#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(QUERY_NAME+"([");
    buffer.append(this.query.toString(field));    
    buffer.append("])");
    return buffer.toString();
  }

  @Override
  public String getField() {
    return query.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    SpanWeight spanWeight = ((SpanQuery) searcher.rewrite(query)).createWeight(searcher,
        needsScores);    
    return new SpanTermWeight(spanWeight, searcher);
  }
  
  public class SpanTermWeight extends SpanWeight {

    SpanWeight spanWeight;
    
    public SpanTermWeight(SpanWeight spanWeight, IndexSearcher searcher) throws IOException {
      super(MtasSpanStartQuery.this, searcher, null);
      this.spanWeight = spanWeight;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      spanWeight.extractTermContexts(contexts); 
    }

    @Override
    public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings)
        throws IOException {
      return new MtasStartSpans(spanWeight.getSpans(ctx, requiredPostings));      
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      spanWeight.extractTerms(terms);
    }
    
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj== null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanStartQuery that = (MtasSpanStartQuery) obj;
    return query.equals(that.query);    
  }

  @Override
  public int hashCode() {
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ query.hashCode();
    return h;
  }

}
