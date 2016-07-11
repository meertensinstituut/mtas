package mtas.parser.cql.util;

import java.io.IOException;

import mtas.analysis.token.MtasToken;
import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanPositionQuery;
import mtas.search.spans.MtasSpanPrefixQuery;
import mtas.search.spans.MtasSpanRegexpQuery;
import mtas.search.spans.MtasSpanTermQuery;
import mtas.search.spans.MtasSpanWildcardQuery;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

public class MtasCQLParserWordPositionQuery extends SpanQuery {

  SpanQuery query;
  Term term;
  
  private static String QUERY_NAME = "mtasCQLParserWordPositionQuery";
  
  public MtasCQLParserWordPositionQuery(String field, int position) {
    term = new Term(field);
    query = new MtasSpanPositionQuery(field, position);    
  }
  
  public MtasCQLParserWordPositionQuery(String field, int start, int end) {
    term = new Term(field);
    query = new MtasSpanPositionQuery(field, start, end);
  }
  
  @Override
  public String getField() {
    return term.field();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return query.rewrite(reader);
  }
  
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return query.createWeight(searcher, needsScores);
  }

  @Override
  public String toString(String field) {
    return query.toString(term.field());
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj== null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasCQLParserWordPositionQuery that = (MtasCQLParserWordPositionQuery) obj;
    return query.equals(that.query);    
  }

  @Override
  public int hashCode() {
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ query.hashCode();
    return h;
  }
  
}
