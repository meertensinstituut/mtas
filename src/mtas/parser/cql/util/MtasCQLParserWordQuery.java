package mtas.parser.cql.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import mtas.analysis.token.MtasToken;
import mtas.parser.cql.ParseException;
import mtas.search.spans.MtasSpanMatchNoneQuery;
import mtas.search.spans.MtasSpanOrQuery;
import mtas.search.spans.MtasSpanPrefixQuery;
import mtas.search.spans.MtasSpanRegexpQuery;
import mtas.search.spans.MtasSpanTermQuery;
import mtas.search.spans.MtasSpanWildcardQuery;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;

/**
 * The Class MtasCQLParserWordQuery.
 */
public class MtasCQLParserWordQuery extends SpanQuery {

  /** The query. */
  SpanQuery query;

  /** The term. */
  Term term;

  /** The query name. */
  private static String QUERY_NAME = "mtasCQLParserWordQuery";

  /** The Constant MTAS_CQL_TERM_QUERY. */
  public static final String MTAS_CQL_TERM_QUERY = "term";

  /** The Constant MTAS_CQL_REGEXP_QUERY. */
  public static final String MTAS_CQL_REGEXP_QUERY = "regexp";

  /** The Constant MTAS_CQL_WILDCARD_QUERY. */
  public static final String MTAS_CQL_WILDCARD_QUERY = "wildcard";
  
  public static final String MTAS_CQL_VARIABLE_QUERY = "variable";

  /**
   * Instantiates a new mtas cql parser word query.
   *
   * @param field
   *          the field
   * @param prefix
   *          the prefix
   */
  public MtasCQLParserWordQuery(String field, String prefix, HashMap<String, String[] > variables) {
    term = new Term(field, prefix + MtasToken.DELIMITER);
    query = new MtasSpanPrefixQuery(term, true);
  }

  /**
   * Instantiates a new mtas cql parser word query.
   *
   * @param field
   *          the field
   * @param prefix
   *          the prefix
   * @param value
   *          the value
   * @throws IOException 
   */
  public MtasCQLParserWordQuery(String field, String prefix, String value, HashMap<String, String[] > variables, HashSet<String> usedVariables) throws ParseException {
    this(field, prefix, value, MTAS_CQL_REGEXP_QUERY, variables, usedVariables);
  }

  /**
   * Instantiates a new mtas cql parser word query.
   *
   * @param field
   *          the field
   * @param prefix
   *          the prefix
   * @param value
   *          the value
   * @param type
   *          the type
   * @throws IOException 
   */
  public MtasCQLParserWordQuery(String field, String prefix, String value, 
      String type, HashMap<String, String[] > variables, HashSet<String> usedVariables) throws ParseException {
    String termBase = prefix + MtasToken.DELIMITER + value;
    if (type.equals(MTAS_CQL_REGEXP_QUERY)) {
      term = new Term(field, termBase + "\u0000*");
      query = new MtasSpanRegexpQuery(term, true);
    } else if (type.equals(MTAS_CQL_WILDCARD_QUERY)) {
      term = new Term(field, termBase );
      query = new MtasSpanWildcardQuery(term, true);
    } else if (type.equals(MTAS_CQL_TERM_QUERY)) {      
      term = new Term(field, "\""+termBase+"\"\u0000*");
      query = new MtasSpanRegexpQuery(term, true);
    } else if (type.equals(MTAS_CQL_VARIABLE_QUERY)) {
      if(value!=null && variables!=null && variables.containsKey(value) && variables.get(value)!=null) {
        if(usedVariables.contains(value)) {
          throw new ParseException("variable $"+value+" should be used exactly one time");
        } else {
          usedVariables.add(value);
        }
        String[] list = variables.get(value);
        SpanQuery[] queries = new SpanQuery[list.length];
        term = new Term(field, prefix + MtasToken.DELIMITER);
        for(int i=0; i<list.length; i++) {
          termBase = prefix + MtasToken.DELIMITER + list[i];
          term = new Term(field, "\""+termBase+"\"\u0000*");
          queries[i] = new MtasSpanRegexpQuery(term, true);
        }  
        if(queries.length==0) {
          query = new MtasSpanMatchNoneQuery(field);
        } else if(queries.length>1) {
          query = new MtasSpanOrQuery(queries);
        } else {
          query = queries[0];
        }
      } else {        
        throw new ParseException("variable $"+value+" not defined");
      }      
    } else {
      term = new Term(field, prefix + MtasToken.DELIMITER + value);
      query = new MtasSpanTermQuery(term, true);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return term.field();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.Query#rewrite(org.apache.lucene.index.IndexReader)
   */
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return query.rewrite(reader);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.spans.SpanQuery#createWeight(org.apache.lucene.
   * search.IndexSearcher, boolean)
   */
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return query.createWeight(searcher, needsScores);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    return query.toString(term.field());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasCQLParserWordQuery that = (MtasCQLParserWordQuery) obj;
    return query.equals(that.query);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#hashCode()
   */
  @Override
  public int hashCode() {
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ query.hashCode();
    return h;
  }

}