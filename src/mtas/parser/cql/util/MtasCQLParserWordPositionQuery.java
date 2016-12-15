package mtas.parser.cql.util;

import java.io.IOException;

import mtas.search.spans.MtasSpanPositionQuery;
import mtas.search.spans.util.MtasSpanQuery;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanWeight;

/**
 * The Class MtasCQLParserWordPositionQuery.
 */
public class MtasCQLParserWordPositionQuery extends MtasSpanQuery {

  /** The query. */
  MtasSpanQuery query;

  /** The term. */
  Term term;

  /**
   * Instantiates a new mtas cql parser word position query.
   *
   * @param field the field
   * @param position the position
   */
  public MtasCQLParserWordPositionQuery(String field, int position) {
    term = new Term(field);
    query = new MtasSpanPositionQuery(field, position);
  }

  /**
   * Instantiates a new mtas cql parser word position query.
   *
   * @param field the field
   * @param start the start
   * @param end the end
   */
  public MtasCQLParserWordPositionQuery(String field, int start, int end) {
    term = new Term(field);
    query = new MtasSpanPositionQuery(field, start, end);
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
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
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
    final MtasCQLParserWordPositionQuery that = (MtasCQLParserWordPositionQuery) obj;
    return query.equals(that.query);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#hashCode()
   */
  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ query.hashCode();
    return h;
  }

}
