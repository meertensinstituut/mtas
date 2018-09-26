package mtas.parser.cql.util;

import mtas.analysis.token.MtasToken;
import mtas.search.spans.MtasSpanPrefixQuery;
import mtas.search.spans.MtasSpanRegexpQuery;
import mtas.search.spans.MtasSpanTermQuery;
import mtas.search.spans.MtasSpanWildcardQuery;
import mtas.search.spans.util.MtasSpanQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;

import java.io.IOException;

public class MtasCQLParserGroupQuery extends MtasSpanQuery {
  MtasSpanQuery query;
  Term term;

  public static final String MTAS_CQL_TERM_QUERY = "term";
  public static final String MTAS_CQL_REGEXP_QUERY = "regexp";
  public static final String MTAS_CQL_WILDCARD_QUERY = "wildcard";

  public MtasCQLParserGroupQuery(String field, String prefix) {
    super(null, null);
    term = new Term(field, prefix + MtasToken.DELIMITER);
    query = new MtasSpanPrefixQuery(term, false);
  }

  public MtasCQLParserGroupQuery(String field, String prefix, String value) {
    this(field, prefix, value, MTAS_CQL_REGEXP_QUERY);
  }

  public MtasCQLParserGroupQuery(String field, String prefix, String value,
                                 String type) {
    super(null, null);
    if (value == null || value.trim().isEmpty()) {
      term = new Term(field, prefix + MtasToken.DELIMITER);
      query = new MtasSpanPrefixQuery(term, false);
    } else if (type == null) {
      term = new Term(field, prefix + MtasToken.DELIMITER + value);
      query = new MtasSpanTermQuery(term, false);
    } else {
      switch (type) {
        case MTAS_CQL_REGEXP_QUERY:
          term = new Term(field,
            prefix + MtasToken.DELIMITER + value + "\u0000*");
          query = new MtasSpanRegexpQuery(term, false);
          break;
        case MTAS_CQL_WILDCARD_QUERY:
          term = new Term(field, prefix + MtasToken.DELIMITER + value);
          query = new MtasSpanWildcardQuery(term, false);
          break;
        case MTAS_CQL_TERM_QUERY:
        default:
          term = new Term(field, prefix + MtasToken.DELIMITER + value);
          query = new MtasSpanTermQuery(term, false);
          break;
      }
    }
  }

  @Override
  public String getField() {
    return term.field();
  }

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    return query.rewrite(reader);
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost)
    throws IOException {
    return query.createWeight(searcher, needsScores, boost);
  }

  @Override
  public String toString(String field) {
    return query.toString(term.field());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasCQLParserGroupQuery that = (MtasCQLParserGroupQuery) obj;
    return query.equals(that.query);
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 5) ^ term.hashCode();
    h = (h * 7) ^ query.hashCode();
    return h;
  }

  @Override
  public void disableTwoPhaseIterator() {
    super.disableTwoPhaseIterator();
    query.disableTwoPhaseIterator();
  }

  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }
}
