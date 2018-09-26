package mtas.parser.cql.util;

import mtas.search.spans.MtasSpanPositionQuery;
import mtas.search.spans.util.MtasSpanQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;

import java.io.IOException;

public class MtasCQLParserWordPositionQuery extends MtasSpanQuery {
  MtasSpanQuery query;
  Term term;

  public MtasCQLParserWordPositionQuery(String field, int position) {
    super(1, 1);
    term = new Term(field);
    query = new MtasSpanPositionQuery(field, position);
  }

  public MtasCQLParserWordPositionQuery(String field, int start, int end) {
    super(1, 1);
    term = new Term(field);
    query = new MtasSpanPositionQuery(field, start, end);
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
    final MtasCQLParserWordPositionQuery that = (MtasCQLParserWordPositionQuery) obj;
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
