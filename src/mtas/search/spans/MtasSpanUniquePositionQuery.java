package mtas.search.spans;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import mtas.search.similarities.MtasSimScorer;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

abstract public class MtasSpanUniquePositionQuery extends SpanQuery {
  
  private SpanQuery clause;
  private String field;
  
  private static String QUERY_NAME = "mtasSpanUniquePositionQuery";
  
  public MtasSpanUniquePositionQuery(SpanQuery clause) {
    field = clause.getField();
    this.clause = clause;
  }

  public SpanQuery getClause() {
    return clause;
  }
  
  @Override
  public String getField() { return field; }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj== null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanUniquePositionQuery that = (MtasSpanUniquePositionQuery) obj;
    return clause.equals(that.clause);    
  }  

  @Override
  public int hashCode() {
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ clause.hashCode();
    return h;
  }
  
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(QUERY_NAME+"([");
    buffer.append(clause.toString(field));
    buffer.append("])");
    return buffer.toString();
  }
  
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    SpanWeight subWeight = clause.createWeight(searcher, false);    
    return new SpanUniquePositionWeight(subWeight, searcher, needsScores ? getTermContexts(subWeight) : null);
  }

  
  public class SpanUniquePositionWeight extends SpanWeight {

    final SpanWeight subWeight;

    public SpanUniquePositionWeight(SpanWeight subWeight, IndexSearcher searcher, Map<Term, TermContext> terms) throws IOException {
      super(MtasSpanUniquePositionQuery.this, searcher, terms);
      this.subWeight = subWeight;
    }
    
    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      subWeight.extractTermContexts(contexts);
    }

    @Override
    public Spans getSpans(LeafReaderContext context, Postings requiredPostings)
        throws IOException {
      Terms terms = context.reader().terms(field);
      if (terms == null) {
        return null; // field does not exist
      }

      Spans subSpan = subWeight.getSpans(context, requiredPostings);
      if (subSpan == null) {
        return null;
      } else {
        SimScorer scorer = getSimScorer(context);
        if(scorer==null) {
          scorer = new MtasSimScorer();
        }                
        return new MtasSpanUniquePosition(MtasSpanUniquePositionQuery.this, subSpan);      
      }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      subWeight.extractTerms(terms);
    }
    
  }
  
  

}
