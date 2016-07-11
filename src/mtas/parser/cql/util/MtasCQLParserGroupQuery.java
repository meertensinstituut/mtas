package mtas.parser.cql.util;

import java.io.IOException;

import mtas.analysis.token.MtasToken;
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

public class MtasCQLParserGroupQuery extends SpanQuery {

  SpanQuery query;
  Term term;
  
  private static String QUERY_NAME = "mtasCQLParserGroupQuery";
  
  public static final String MTAS_CQL_TERM_QUERY = "term";
  public static final String MTAS_CQL_REGEXP_QUERY = "regexp";
  public static final String MTAS_CQL_WILDCARD_QUERY = "wildcard";
  
  public MtasCQLParserGroupQuery(String field, String prefix) {  
    term = new Term(field,prefix+MtasToken.DELIMITER);
    query = new MtasSpanPrefixQuery(term, false);     
  }
  
  public MtasCQLParserGroupQuery(String field, String prefix, String value) {
    this(field, prefix, value, MTAS_CQL_REGEXP_QUERY);    
  }
  
  public MtasCQLParserGroupQuery(String field, String prefix, String value, String type) {
    if(value==null || value.trim().equals("")) {
      term = new Term(field,prefix+MtasToken.DELIMITER);
      query = new MtasSpanPrefixQuery(term, false);     
    } else {      
      if(type==null) {
        term = new Term(field,prefix+MtasToken.DELIMITER+value);
        query = new MtasSpanTermQuery(term, false);
      } else if(type.equals(MTAS_CQL_REGEXP_QUERY)) {
        term = new Term(field,prefix+MtasToken.DELIMITER+value+"\u0000*");
        query = new MtasSpanRegexpQuery(term, false);
      } else if(type.equals(MTAS_CQL_WILDCARD_QUERY)) {
        term = new Term(field,prefix+MtasToken.DELIMITER+value);
        query = new MtasSpanWildcardQuery(term, false);
      } else if(type.equals(MTAS_CQL_TERM_QUERY)) {
        term = new Term(field,prefix+MtasToken.DELIMITER+value);
        query = new MtasSpanTermQuery(term, false);
      } else {
        term = new Term(field,prefix+MtasToken.DELIMITER+value);
        query = new MtasSpanTermQuery(term, false);
      }  
    }  
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
    final MtasCQLParserGroupQuery that = (MtasCQLParserGroupQuery) obj;
    return query.equals(that.query);    
  }

  @Override
  public int hashCode() {
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ query.hashCode();
    return h;
  }
  
}
