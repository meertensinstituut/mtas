package mtas.search.spans.util;

import mtas.search.spans.MtasSpanMatchNoneQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class MtasDisabledTwoPhaseIteratorSpanQuery extends MtasSpanQuery {
  private MtasSpanQuery subQuery;

  public MtasDisabledTwoPhaseIteratorSpanQuery(MtasSpanQuery q) {
    super(q.getMinimumWidth(), q.getMaximumWidth());
    this.subQuery = q;
  }

  @Override
  public MtasSpanWeight createWeight(IndexSearcher searcher,
      boolean needsScores, float boost) throws IOException {
    SpanWeight subWeight = subQuery.createWeight(searcher, needsScores, boost);
    return new MtasDisabledTwoPhaseIteratorWeight(subWeight, searcher,
        needsScores, boost);
  }

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newQ = subQuery.rewrite(reader);
    if (newQ == null) {
      newQ = new MtasSpanMatchNoneQuery(subQuery.getField());
      return new MtasDisabledTwoPhaseIteratorSpanQuery(newQ);
    } else {
      newQ.disableTwoPhaseIterator();
      if (!newQ.equals(subQuery)) {
        return new MtasDisabledTwoPhaseIteratorSpanQuery(newQ).rewrite(reader);
      } else {
        return super.rewrite(reader);
      }
    }
  }

  public String getField() {
    return subQuery.getField();
  }

  @Override
  public String toString(String field) {
    return subQuery.toString(field);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasDisabledTwoPhaseIteratorSpanQuery that = (MtasDisabledTwoPhaseIteratorSpanQuery) obj;
    return that.subQuery.equals(subQuery);
  }

  @Override
  public int hashCode() {
    int h = Integer.rotateLeft(classHash(), 1);
    h ^= subQuery.hashCode();
    return h;
  }

  @Override
  public void disableTwoPhaseIterator() {
    super.disableTwoPhaseIterator();
    subQuery.disableTwoPhaseIterator();
  }
  
  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }

  private class MtasDisabledTwoPhaseIteratorWeight extends MtasSpanWeight {

    SpanWeight subWeight;

    public MtasDisabledTwoPhaseIteratorWeight(SpanWeight subWeight,
        IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
      super(subQuery, searcher,
          needsScores ? getTermContexts(subWeight) : null, boost);
      this.subWeight = subWeight;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      subWeight.extractTermContexts(contexts);
    }

    @Override
    public MtasSpans getSpans(LeafReaderContext ctx, Postings requiredPostings)
        throws IOException {
      return new MtasDisabledTwoPhaseIteratorSpans(
          subWeight.getSpans(ctx, requiredPostings));
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      subWeight.extractTerms(terms);
    }
    
//    @Override
//    public boolean isCacheable(LeafReaderContext arg0) {
//      return subWeight.isCacheable(arg0);
//    }
  }
}
