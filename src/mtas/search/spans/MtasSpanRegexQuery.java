package mtas.search.spans;

import java.io.IOException;

import mtas.codec.util.CodecUtil;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;

/**
 * The Class MtasSpanTermQuery.
 */
public class MtasSpanRegexQuery extends SpanQuery {

  private static final int MTAS_REGEX_EXPAND_BOUNDARY = 1000;

  /** The prefix. */
  private String prefix;

  /** The value. */
  private String value;

  /** The single position. */
  private boolean singlePosition;

  private Term term;

  private SpanMultiTermQueryWrapper<RegexpQuery> query;

  private static String QUERY_NAME = "mtasSpanRegexQuery";
  
  

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query q = query.rewrite(reader);
    if (q instanceof SpanOrQuery) {
      SpanQuery[] clauses = ((SpanOrQuery) q).getClauses();
      if (clauses.length > MTAS_REGEX_EXPAND_BOUNDARY) {
        // TODO : forward index solution
        throw new IOException("JAN-ODIJK-EXCEPTION: Regex \""
            + CodecUtil.termValue(term.text()) + "\" expands to "
            + clauses.length + " terms, too many (boundary "
            + MTAS_REGEX_EXPAND_BOUNDARY + ")!");
      }
      SpanQuery[] newClauses = new SpanQuery[clauses.length];
      for (int i = 0; i < clauses.length; i++) {
        if (clauses[i] instanceof SpanTermQuery) {
          newClauses[i] = new MtasSpanTermQuery((SpanTermQuery) clauses[i],
              singlePosition);
        } else {
          throw new IOException("no SpanTermQuery after rewrite");
        }
      }
      return new SpanOrQuery(newClauses);
    } else {
      throw new IOException("no SpanOrQuery after rewrite");
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
    buffer.append(QUERY_NAME+"([");
    if (value == null) {
      buffer.append(this.query.getField() + ":" + prefix);
    } else {
      buffer.append(this.query.getField() + ":" + prefix + "=" + value);
    }
    buffer.append("])");
    return buffer.toString();
  }

  @Override
  public String getField() {
    return term.field();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return ((SpanQuery) searcher.rewrite(query)).createWeight(searcher,
        needsScores);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MtasSpanRegexQuery that = (MtasSpanRegexQuery) obj;
    return term.equals(that.term) && singlePosition==that.singlePosition;    
  }

  @Override
  public int hashCode() {
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ term.hashCode();
    h += (singlePosition?1:0);
    return h;
  }


}
