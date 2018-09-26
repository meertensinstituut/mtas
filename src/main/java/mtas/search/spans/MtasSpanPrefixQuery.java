package mtas.search.spans;

import mtas.analysis.token.MtasToken;
import mtas.search.spans.util.MtasSpanQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;

import java.io.IOException;

public class MtasSpanPrefixQuery extends MtasSpanQuery {
  private String prefix;
  private String value;
  private boolean singlePosition;
  private Term term;
  private SpanMultiTermQueryWrapper<PrefixQuery> query;

  public MtasSpanPrefixQuery(Term term) {
    this(term, true);
  }

  public MtasSpanPrefixQuery(Term term, boolean singlePosition) {
    super(singlePosition ? 1 : null, singlePosition ? 1 : null);
    PrefixQuery pfq = new PrefixQuery(term);
    query = new SpanMultiTermQueryWrapper<>(pfq);
    this.term = term;
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

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    Query q = query.rewrite(reader);
    if (q instanceof SpanOrQuery) {
      SpanQuery[] clauses = ((SpanOrQuery) q).getClauses();
      MtasSpanQuery[] newClauses = new MtasSpanQuery[clauses.length];
      for (int i = 0; i < clauses.length; i++) {
        if (clauses[i] instanceof SpanTermQuery) {
          newClauses[i] = new MtasSpanTermQuery((SpanTermQuery) clauses[i],
              singlePosition).rewrite(reader);
        } else {
          throw new IOException("no SpanTermQuery after rewrite");
        }
      }
      return new MtasSpanOrQuery(newClauses).rewrite(reader);
    } else {
      throw new IOException("no SpanOrQuery after rewrite");
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    if (value == null) {
      buffer.append(this.query.getField() + ":" + prefix);
    } else {
      buffer.append(this.query.getField() + ":" + prefix + "=" + value);
    }
    buffer.append("," + singlePosition + "])");
    return buffer.toString();
  }

  @Override
  public String getField() {
    return term.field();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost)
      throws IOException {
    return ((SpanQuery) searcher.rewrite(query)).createWeight(searcher,
        needsScores, boost);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MtasSpanPrefixQuery other = (MtasSpanPrefixQuery) obj;
    return other.term.equals(term) && (other.singlePosition == singlePosition);
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ term.hashCode();
    h += (singlePosition ? 1 : 0);
    return h;
  }
  
  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }
}
