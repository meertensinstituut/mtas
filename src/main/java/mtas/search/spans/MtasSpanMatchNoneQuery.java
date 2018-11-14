package mtas.search.spans;

import mtas.search.similarities.MtasSimScorer;
import mtas.search.spans.util.MtasSpanQuery;
import mtas.search.spans.util.MtasSpanWeight;
import mtas.search.spans.util.MtasSpans;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

public class MtasSpanMatchNoneQuery extends MtasSpanQuery {
  private String field;

  public MtasSpanMatchNoneQuery(String field) {
    super(null, null);
    this.field = field;
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public MtasSpanWeight createWeight(IndexSearcher searcher,
      boolean needsScores, float boost) throws IOException {
    return new SpanNoneWeight(searcher, null, boost);
  }

  protected class SpanNoneWeight extends MtasSpanWeight {
    private static final String METHOD_GET_DELEGATE = "getDelegate";

    public SpanNoneWeight(IndexSearcher searcher,
        Map<Term, TermContext> termContexts, float boost) throws IOException {
      super(MtasSpanMatchNoneQuery.this, searcher, termContexts, boost);
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      // don't do anything
    }

    @Override
    public MtasSpans getSpans(LeafReaderContext context,
        Postings requiredPostings) throws IOException {
      try {
        // get leafreader
        LeafReader r = context.reader();
        // get delegate
        Boolean hasMethod = true;
        while (hasMethod) {
          hasMethod = false;
          Method[] methods = r.getClass().getMethods();
          for (Method m : methods) {
            if (m.getName().equals(METHOD_GET_DELEGATE)) {
              hasMethod = true;
              r = (LeafReader) m.invoke(r, (Object[]) null);
              break;
            }
          }
        }
        // get MtasFieldsProducer using terms
        return new MtasSpanMatchNoneSpans(MtasSpanMatchNoneQuery.this);
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new IOException("Can't get reader", e);
      }

    }

    @Override
    public void extractTerms(Set<Term> terms) {
      // don't do anything
    }

    @Override
    public SimScorer getSimScorer(LeafReaderContext context) {
      return new MtasSimScorer();
    }

//    @Override
//    public boolean isCacheable(LeafReaderContext arg0) {
//      return true;
//    }

  }

  @Override
  public String toString(String field) {
    return this.getClass().getSimpleName() + "([])";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanMatchNoneQuery that = (MtasSpanMatchNoneQuery) obj;
    if (field == null) {
      return that.field == null;
    } else {
      return field.equals(that.field);
    }
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    if (field != null) {
      h = (h * 7) ^ field.hashCode();
    }
    return h;
  }
  
  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }
}
