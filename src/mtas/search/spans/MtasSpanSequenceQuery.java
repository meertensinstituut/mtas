package mtas.search.spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

public class MtasSpanSequenceQuery extends SpanQuery implements Cloneable {

  private List<MtasSpanSequenceItem> items;
  private String field;
  
  private static String QUERY_NAME = "mtasSpanSequenceQuery";  
  
  public MtasSpanSequenceQuery(List<MtasSpanSequenceItem> items) {
    this.items = items;     
    //get field and do checks
    for (MtasSpanSequenceItem item : items) {
      if (field == null) {
        field = item.getQuery().getField();
      } else if (item.getQuery().getField() != null && !item.getQuery().getField().equals(field)) {
        throw new IllegalArgumentException("Clauses must have same field.");
      }
    }    
  }
  
  @Override
  public String getField() { return field; }
  
  @Override
  public MtasSpanSequenceQuery clone() {
    int sz = items.size();
    List<MtasSpanSequenceItem> newItems = new ArrayList<MtasSpanSequenceItem>();
    for (int i = 0; i < sz; i++) {
      newItems.add(items.get(i).clone());
    }
    MtasSpanSequenceQuery soq = new MtasSpanSequenceQuery(newItems);
    return soq;
  }
  
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    MtasSpanSequenceQuery clone = null;
    for (int i = 0 ; i < items.size(); i++) {
      SpanQuery c = items.get(i).getQuery();
      SpanQuery query = (SpanQuery) c.rewrite(reader);
      if (query != c) {                     // clause rewrote: must clone
        if (clone == null)
          clone = this.clone();
        clone.items.get(i).setQuery(query);
      }
    }
    if (clone != null) {
      return clone;                        // some clauses rewrote
    } else {
      return this;                         // no clauses rewrote
    }
  }
  
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("mtasSpanSequence([");
    Iterator<MtasSpanSequenceItem> i = items.iterator();
    while (i.hasNext()) {
      MtasSpanSequenceItem item = i.next();
      SpanQuery clause = item.getQuery();
      buffer.append(clause.toString(field));
      if(item.isOptional()) {
        buffer.append("{OPTIONAL}");
      }
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("])");
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
    MtasSpanSequenceQuery other = (MtasSpanSequenceQuery) obj;
    return field.equals(other.field) && items.equals(other.items);    
  }

  @Override
  public int hashCode() {
    int h = field.hashCode();
    h = (h * 7) ^ items.hashCode();
    return h;
  }

  

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {    
    List<MtasSpanSequenceWeight> subWeights = new ArrayList<MtasSpanSequenceWeight>();
    for(MtasSpanSequenceItem item : items) {
      subWeights.add(new MtasSpanSequenceWeight(item.getQuery().createWeight(searcher, false), item.isOptional()) );    
    }
    return new SpanSequenceWeight(subWeights, searcher, needsScores ? getTermContexts(subWeights) : null);
  }
  
  protected Map<Term, TermContext> getTermContexts(List<MtasSpanSequenceWeight> items) {
    List<SpanWeight> weights = new ArrayList<SpanWeight>();
    for(MtasSpanSequenceWeight item : items) {
      weights.add(item.spanWeight);
    }
    return getTermContexts(weights);
  }
  
  public class SpanSequenceWeight extends SpanWeight {

    final List<MtasSpanSequenceWeight> subWeights;

    public SpanSequenceWeight(List<MtasSpanSequenceWeight> subWeights, IndexSearcher searcher, Map<Term, TermContext> terms) throws IOException {
      super(MtasSpanSequenceQuery.this, searcher, terms);
      this.subWeights = subWeights;   
    }
    
    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      for (MtasSpanSequenceWeight w : subWeights) {
        w.spanWeight.extractTermContexts(contexts);
      }
    }

    @Override
    public Spans getSpans(LeafReaderContext context, Postings requiredPostings)
        throws IOException {
      Terms terms = context.reader().terms(field);
      if (terms == null) {
        return null; // field does not exist
      }
      List<MtasSpanSequenceSpans> setSequenceSpans = new ArrayList<>(items.size());
      boolean allSpansEmpty = true;
      for (MtasSpanSequenceWeight w : subWeights) {        
        Spans sequenceSpans = w.spanWeight.getSpans(context, requiredPostings);        
        if (sequenceSpans != null) {
          setSequenceSpans.add(new MtasSpanSequenceSpans(sequenceSpans, w.optional));
          allSpansEmpty = false;
        } else {
          if(w.optional) {
            setSequenceSpans.add(new MtasSpanSequenceSpans(null, w.optional));
          } else {
            return null;
          }
        }
      }
      if(allSpansEmpty) {
        return null; // at least one required
      }
      return new MtasSpanSequence(MtasSpanSequenceQuery.this, setSequenceSpans);   
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (MtasSpanSequenceWeight w : subWeights) {
        w.spanWeight.extractTerms(terms);
      }
    }
    
  }
  
  public class MtasSpanSequenceSpans {
    public Spans spans;
    public boolean optional;
    public MtasSpanSequenceSpans(Spans spans, boolean optional) {
      this.spans = spans;
      this.optional = optional;
    }
  }
  
  public class MtasSpanSequenceWeight {    
    public SpanWeight spanWeight;
    public boolean optional;    
    public MtasSpanSequenceWeight(SpanWeight spanWeight, boolean optional) {
      this.spanWeight = spanWeight;
      this.optional = optional;
    }
  }

}
