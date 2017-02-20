package mtas.search.spans;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import mtas.search.spans.util.MtasSpanQuery;

public class MtasSpanWithinQuery extends MtasSpanQuery {
  
  private SpanQuery little, big;

  public MtasSpanWithinQuery(SpanQuery little, SpanQuery big) {
    super();
    if(little==null || big==null) {
      throw new IllegalArgumentException("queries shouldn't be null");
    } else if (little.getField()!=null && big.getField()!=null && !little.getField().equals(big.getField())) {    
      throw new IllegalArgumentException("big ("+big.getField()+") and little ("+little.getField()+") not same field");
    } else {
      this.little=little;
      this.big=big;
    }  
  }
  
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    SpanQuery newLittle = (SpanQuery) little.rewrite(reader); 
    SpanQuery newBig = (SpanQuery) big.rewrite(reader); 
    if(newLittle!=little || newBig!=big) {
      return new MtasSpanWithinQuery(newLittle, newBig);      
    } else {
      return this;
    }  
  } 

  @Override
  public String getField() {
    return little.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    SpanWeight littleWeight = little.createWeight(searcher, false);
    SpanWeight bigWeight = big.createWeight(searcher, false);
    return new MtasSpanWithinWeight(searcher, needsScores ? getTermContexts(littleWeight, bigWeight) : null,
                                      bigWeight, littleWeight); 
    
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName());
    buffer.append("(");
    buffer.append(little.toString(field));
    buffer.append(", ");
    buffer.append(big.toString(field));
    buffer.append(")");
    return buffer.toString();        
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
    return little.equals(that.little) && big.equals(that.big);
  }

  @Override
  public int hashCode() {
    int h = Integer.rotateLeft(classHash(), 1);
    h ^= little.hashCode();
    h = Integer.rotateLeft(h, 1);
    h ^= big.hashCode();
    return h;
  }
  
  public class MtasSpanWithinWeight extends SpanWeight {

    final SpanWeight bigWeight;
    final SpanWeight littleWeight;

    public MtasSpanWithinWeight(IndexSearcher searcher, Map<Term, TermContext> terms
                             , SpanWeight littleWeight, SpanWeight bigWeight) throws IOException {
      super(MtasSpanWithinQuery.this, searcher, terms);
      this.littleWeight = littleWeight;
      this.bigWeight = bigWeight;      
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      bigWeight.extractTermContexts(contexts);
      littleWeight.extractTermContexts(contexts);
    }

    @Override
    public Spans getSpans(LeafReaderContext context, Postings postings)
        throws IOException {
      Spans bigSpans = bigWeight.getSpans(context, postings);
      if(bigSpans==null) {
        return null;
      }
      Spans littleSpans = littleWeight.getSpans(context, postings);
      if(littleSpans==null) {
        return null;
      }      
      return new MtasSpanWithinSpans(littleSpans, bigSpans);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      bigWeight.extractTerms(terms);
      littleWeight.extractTerms(terms);
    }
  }  

}
