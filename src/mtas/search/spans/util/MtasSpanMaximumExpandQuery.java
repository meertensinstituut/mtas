package mtas.search.spans.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import mtas.codec.util.CodecInfo;
import mtas.codec.util.CodecInfo.IndexDoc;
import mtas.search.spans.MtasSpanMatchNoneSpans;

public class MtasSpanMaximumExpandQuery extends MtasSpanQuery {

  MtasSpanQuery query;
  int minimumLeft;
  int maximumLeft;
  int minimumRight;
  int maximumRight;

  public MtasSpanMaximumExpandQuery(MtasSpanQuery query, int minimumLeft,
      int maximumLeft, int minimumRight, int maximumRight) {
    super(null, null);
    this.query = query;
    if (minimumLeft > maximumLeft || minimumRight > maximumRight
        || minimumLeft < 0 || minimumRight < 0) {
      throw new IllegalArgumentException();
    }
    this.minimumLeft = minimumLeft;
    this.maximumLeft = maximumLeft;
    this.minimumRight = minimumRight;
    this.maximumRight = maximumRight;
    Integer minimum = query.getMinimumWidth();
    Integer maximum = query.getMaximumWidth();
    if (minimum != null) {
      minimum += minimumLeft + minimumRight;
    }
    if (maximum != null) {
      maximum += maximumLeft + maximumRight;
    }
    setWidth(minimum, maximum);
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    SpanWeight subWeight = query.createWeight(searcher, needsScores);
    if (maximumLeft == 0 && maximumRight == 0) {
      return subWeight;
    } else {
      return new MtasMaximumExpandWeight(subWeight, searcher, needsScores);
    }
  }

  @Override
  public String getField() {
    return query.getField();
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    buffer.append(query.toString(field) + "]["+minimumLeft+","+maximumLeft+"]["+minimumRight+","+maximumRight+"])");
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
    final MtasSpanMaximumExpandQuery that = (MtasSpanMaximumExpandQuery) obj;
    boolean isEqual;
    isEqual = query.equals(that.query);
    isEqual &= minimumLeft == that.minimumLeft;
    isEqual &= maximumLeft == that.maximumLeft;
    isEqual &= minimumRight == that.minimumRight;
    isEqual &= maximumRight == that.maximumRight;
    return isEqual;
  }

  @Override
  public int hashCode() {
    int h = Integer.rotateLeft(classHash(), 1);
    h ^= query.hashCode();
    h = Integer.rotateLeft(h, minimumLeft) + minimumLeft;
    h ^= 2;
    h = Integer.rotateLeft(h, maximumLeft) + maximumLeft;
    h ^= 3;
    h = Integer.rotateLeft(h, minimumRight) + minimumRight;
    h ^= 5;
    h = Integer.rotateLeft(h, maximumRight) + maximumRight;
    return h;
  }

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newQuery = query.rewrite(reader);
    if (maximumLeft == 0 && maximumRight == 0) {
      return newQuery;
    } else if (!query.equals(newQuery)) {
      return new MtasSpanMaximumExpandQuery(newQuery, minimumLeft, maximumLeft,
          minimumRight, maximumRight);
    } else {
      return super.rewrite(reader);
    }
  }

  private class MtasMaximumExpandWeight extends SpanWeight {
    SpanWeight subWeight;

    public MtasMaximumExpandWeight(SpanWeight subWeight, IndexSearcher searcher,
        boolean needsScores) throws IOException {
      super(MtasSpanMaximumExpandQuery.this, searcher,
          needsScores ? getTermContexts(subWeight) : null);
      this.subWeight = subWeight;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      subWeight.extractTermContexts(contexts);
    }

    @Override
    public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings)
        throws IOException {
      Spans spans = subWeight.getSpans(ctx, requiredPostings);
      if (maximumLeft == 0 && maximumRight == 0) {
        return spans;
      } else {
        try {
          // get leafreader
          LeafReader r = ctx.reader();
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
          } // get fieldsproducer
          Method fpm = r.getClass().getMethod("getPostingsReader",
              (Class<?>[]) null);
          FieldsProducer fp = (FieldsProducer) fpm.invoke(r, (Object[]) null);
          // get MtasFieldsProducer using terms
          Terms t = fp.terms(field);
          if (t == null) {
            return new MtasSpanMatchNoneSpans(field);
          } else {
            CodecInfo mtasCodecInfo = CodecInfo.getCodecInfoFromTerms(t);
            return new MtasMaximumExpandSpans(mtasCodecInfo, query.getField(),
                spans);
          }
        } catch (Exception e) {
          throw new IOException("Can't get reader", e);
        }

      }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      subWeight.extractTerms(terms);
    }

  }

  private class MtasMaximumExpandSpans extends Spans {

    Spans subSpans;
    int minPosition;
    int maxPosition;
    String field;
    CodecInfo mtasCodecInfo;
    int startPosition;
    int endPosition;

    public MtasMaximumExpandSpans(CodecInfo mtasCodecInfo, String field,
        Spans subSpans) {
      super();
      this.subSpans = subSpans;
      this.field = field;
      this.mtasCodecInfo = mtasCodecInfo;
      this.minPosition = 0;
      this.maxPosition = 0;
      this.startPosition = -1;
      this.endPosition = -1;
    }

    @Override
    public int nextStartPosition() throws IOException {
      int basicStartPosition;
      int basicEndPosition;
      while ((basicStartPosition = subSpans
          .nextStartPosition()) != NO_MORE_POSITIONS) {
        basicEndPosition = subSpans.endPosition();
        startPosition = Math.max(minPosition,
            (basicStartPosition - maximumLeft));
        endPosition = Math.min(maxPosition + 1,
            (basicEndPosition + maximumRight));
        if (startPosition <= (basicStartPosition - minimumLeft)
            && endPosition >= (basicEndPosition + minimumRight)) {
          return this.startPosition;
        }
      }
      startPosition = NO_MORE_POSITIONS;
      endPosition = NO_MORE_POSITIONS;
      return NO_MORE_POSITIONS;
    }

    @Override
    public int startPosition() {
      return startPosition;
    }

    @Override
    public int endPosition() {
      return endPosition;
    }

    @Override
    public int width() {
      return endPosition-startPosition;
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {
      subSpans.collect(collector);
    }

    @Override
    public final TwoPhaseIterator asTwoPhaseIterator() {
      // return subSpans.asTwoPhaseIterator();
      return null;
    }
    
    @Override
    public float positionsCost() {
      //return subSpans.positionsCost();
      return 0;
    }

    @Override
    public int docID() {
      return subSpans.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docId = subSpans.nextDoc();
      startPosition = -1;
      endPosition = -1;
      if (docId != NO_MORE_DOCS) {
        IndexDoc doc = mtasCodecInfo.getDoc(field, docId);
        if (doc != null) {
          minPosition = doc.minPosition;
          maxPosition = doc.maxPosition;
        } else {
          minPosition = NO_MORE_POSITIONS;
          maxPosition = NO_MORE_POSITIONS;
        }
      } else {
        minPosition = NO_MORE_POSITIONS;
        maxPosition = NO_MORE_POSITIONS;
      }
      return docId;
    }

    @Override
    public int advance(int target) throws IOException {
      int docId = subSpans.advance(target);
      startPosition = -1;
      endPosition = -1;
      if (docId != NO_MORE_DOCS) {
        IndexDoc doc = mtasCodecInfo.getDoc(field, docId);
        if (doc != null) {
          minPosition = doc.minPosition;
          maxPosition = doc.maxPosition;
        } else {
          minPosition = NO_MORE_POSITIONS;
          maxPosition = NO_MORE_POSITIONS;
        }
      } else {
        minPosition = NO_MORE_POSITIONS;
        maxPosition = NO_MORE_POSITIONS;
      }
      return docId;
    }

    @Override
    public long cost() {
      return subSpans!=null?subSpans.cost():0;
    }
  }

}
