package mtas.search.spans.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import mtas.analysis.token.MtasToken;
import mtas.codec.util.CodecUtil;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.FilterSpans;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

/**
 * The Class MtasExtendedSpanTermQuery.
 */
public class MtasExtendedSpanTermQuery extends SpanTermQuery {

  /** The prefix. */
  private String prefix;

  /** The value. */
  private String value;

  /** The single position. */
  private boolean singlePosition;

  /** The term. */
  private Term term;

  /**
   * Instantiates a new mtas extended span term query.
   *
   * @param term the term
   */
  public MtasExtendedSpanTermQuery(Term term) {
    this(term, true);
  }

  /**
   * Instantiates a new mtas extended span term query.
   *
   * @param term the term
   * @param singlePosition the single position
   */
  public MtasExtendedSpanTermQuery(Term term, boolean singlePosition) {
    this(new SpanTermQuery(term), true);
  }

  /**
   * Instantiates a new mtas extended span term query.
   *
   * @param query the query
   * @param singlePosition the single position
   */
  public MtasExtendedSpanTermQuery(SpanTermQuery query,
      boolean singlePosition) {
    super(query.getTerm());
    term = query.getTerm();
    this.singlePosition = singlePosition;
    int i = term.text().indexOf(MtasToken.DELIMITER);
    if (i >= 0) {
      prefix = term.text().substring(0, i);
      value = term.text().substring((i + MtasToken.DELIMITER.length()));
      value = (value.length() > 0) ? value : null;
    } else {
      prefix = term.text();
      value = null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.spans.SpanTermQuery#createWeight(org.apache.lucene
   * .search.IndexSearcher, boolean)
   */
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    final TermContext context;
    final IndexReaderContext topContext = searcher.getTopReaderContext();
    if (termContext == null) {
      context = TermContext.build(topContext, term);
    } else {
      context = termContext;
    }
    return new SpanTermWeight(context, searcher,
        needsScores ? Collections.singletonMap(term, context) : null);
  }

  /**
   * The Class SpanTermWeight.
   */
  public class SpanTermWeight extends SpanWeight {

    /** The term context. */
    final TermContext termContext;

    /**
     * Instantiates a new span term weight.
     *
     * @param termContext the term context
     * @param searcher the searcher
     * @param terms the terms
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public SpanTermWeight(TermContext termContext, IndexSearcher searcher,
        Map<Term, TermContext> terms) throws IOException {
      super(MtasExtendedSpanTermQuery.this, searcher, terms);
      this.termContext = termContext;
      assert termContext != null : "TermContext must not be null";
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lucene.search.Weight#extractTerms(java.util.Set)
     */
    @Override
    public void extractTerms(Set<Term> terms) {
      terms.add(term);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.lucene.search.spans.SpanWeight#extractTermContexts(java.util
     * .Map)
     */
    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      contexts.put(term, termContext);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.lucene.search.spans.SpanWeight#getSpans(org.apache.lucene.
     * index.LeafReaderContext,
     * org.apache.lucene.search.spans.SpanWeight.Postings)
     */
    @Override
    public Spans getSpans(final LeafReaderContext context,
        Postings requiredPostings) throws IOException {
      final TermState state = termContext.get(context.ord);
      if (state == null) { // term is not present in that reader
        assert context.reader().docFreq(
            term) == 0 : "no termstate found but term exists in reader term="
                + term;
        return null;
      }

      final Terms terms = context.reader().terms(term.field());
      if (terms == null)
        return null;
      if (terms.hasPositions() == false)
        throw new IllegalStateException("field \"" + term.field()
            + "\" was indexed without position data; cannot run SpanTermQuery (term="
            + term.text() + ")");

      final TermsEnum termsEnum = terms.iterator();
      termsEnum.seekExact(term.bytes(), state);

      final PostingsEnum postings;
      Spans matchSpans;

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

        FieldInfo fieldInfo = r.getFieldInfos().fieldInfo(field);

        if (CodecUtil.isSinglePositionPrefix(fieldInfo, prefix)) {
          postings = termsEnum.postings(null,
              requiredPostings.getRequiredPostings());
          matchSpans = new MtasExtendedTermSpans(postings, term, true);
        } else {
          postings = termsEnum.postings(null, requiredPostings
              .atLeast(Postings.PAYLOADS).getRequiredPostings());
          matchSpans = new MtasExtendedTermSpans(postings, term, false);
        }
        return (matchSpans == null) ? null
            : singlePosition ? new FilterSpans(matchSpans) {
              @Override
              protected AcceptStatus accept(Spans candidate)
                  throws IOException {
                assert candidate.startPosition() != candidate.endPosition();
                AcceptStatus res = ((candidate.endPosition()
                    - candidate.startPosition()) == 1) ? AcceptStatus.YES
                        : AcceptStatus.NO;
                return res;
              }
            } : matchSpans;

      } catch (Exception e) {
        // e.printStackTrace();
        throw new IOException("Can't get reader: " + e.getMessage());
      }

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
    buffer.append(this.getClass().getSimpleName() + "([");
    if (value == null) {
      buffer.append(field + ":" + prefix);
    } else {
      buffer.append(field + ":" + prefix + "=" + value);
    }
    buffer.append("])");
    return buffer.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanTermQuery#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MtasExtendedSpanTermQuery other = (MtasExtendedSpanTermQuery) obj;
    return other.term.equals(term) && (other.singlePosition != singlePosition);
  }

}
