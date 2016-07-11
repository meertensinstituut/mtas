package mtas.search.spans;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import mtas.codec.util.CodecInfo;
import mtas.search.similarities.MtasSimScorer;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.store.IndexInput;

/**
 * The Class MtasSpanTermQuery.
 */
public class MtasSpanPositionQuery extends SpanQuery {

  private String field;
  private int start, end;
  private static String QUERY_NAME = "mtasSpanPositionQuery";

  public MtasSpanPositionQuery(String field, int position) {
    this.field = field;
    this.start = position;
    this.end = position;
  }

  public MtasSpanPositionQuery(String field, int start, int end) {
    this.field = field;
    this.start = start;
    this.end = end;
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return new SpanAllWeight(searcher, null);
  }

  /**
   * The Class SpanTermWeight.
   */
  public class SpanAllWeight extends SpanWeight {

    public SpanAllWeight(IndexSearcher searcher,
        Map<Term, TermContext> termContexts) throws IOException {
      super(MtasSpanPositionQuery.this, searcher, termContexts);
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public Spans getSpans(LeafReaderContext context, Postings requiredPostings)
        throws IOException {
      try {
        // get leafreader
        LeafReader r = context.reader();
        // get delegate
        Boolean hasMethod = true;
        while (hasMethod) {
          hasMethod = false;
          Method[] methods = r.getClass().getMethods();
          for (Method m : methods) {
            if (m.getName().equals("getDelegate")) {
              hasMethod = true;
              r = (LeafReader) m.invoke(r, (Object[]) null);
              break;
            }
          }
        }
        // get fieldsproducer
        Method fpm = r.getClass().getMethod("getPostingsReader",
            (Class<?>[]) null);
        FieldsProducer fp = (FieldsProducer) fpm.invoke(r, (Object[]) null);
        // get MtasFieldsProducer using terms
        Terms t = fp.terms(field);
        if (t == null) {
          return new MtasSpanMatchNone(field);
        } else {
          CodecInfo mtasCodecInfo = CodecInfo.getCodecInfoFromTerms(t);
          return new MtasSpanPosition(mtasCodecInfo, field, start, end);
        }
      } catch (Exception e) {
        throw new IOException("Can't get reader");
      }

    }

    @Override
    public void extractTerms(Set<Term> terms) {
    }

    @Override
    public SimScorer getSimScorer(LeafReaderContext context) {
      return new MtasSimScorer();
    }

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
    buffer.append(
        QUERY_NAME + "([" + start + (start != end ? "," + end : "") + "])");
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
    final MtasSpanPositionQuery that = (MtasSpanPositionQuery) obj;
    return field.equals(that.field) && start == that.start && end == that.end;
  }

  @Override
  public int hashCode() {
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ field.hashCode();
    h = (h * 13) ^ start;
    h = (h * 17) ^ end;
    return h;
  }

}
