package mtas.search.spans;

import mtas.search.spans.util.MtasSpanQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanWeight;

import java.io.IOException;

public class MtasSpanContainingQuery extends MtasSpanQuery {
  private SpanContainingQuery baseQuery;
  private MtasSpanQuery bigQuery;
  private MtasSpanQuery smallQuery;
  private String field;

  public MtasSpanContainingQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    super(q1 != null ? q1.getMinimumWidth() : null,
        q1 != null ? q1.getMaximumWidth() : null);
    if (q2 != null && q2.getMinimumWidth() != null
        && (this.getMinimumWidth() == null
            || this.getMinimumWidth() < q2.getMinimumWidth())) {
      this.setWidth(q2.getMinimumWidth(), this.getMaximumWidth());
    }
    bigQuery = q1;
    smallQuery = q2;
    if (bigQuery != null && bigQuery.getField() != null) {
      field = bigQuery.getField();
    } else if (smallQuery != null && smallQuery.getField() != null) {
      field = smallQuery.getField();
    } else {
      field = null;
    }
    if (field != null && bigQuery != null && smallQuery != null) {
      if (bigQuery.getField() != null && smallQuery.getField() != null) {
        baseQuery = new SpanContainingQuery(bigQuery, smallQuery);
      } else {
        baseQuery = null;
      }
    } else {
      baseQuery = null;
    }
  }

  @Override
  public String getField() {
    return baseQuery.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost)
      throws IOException {
    return baseQuery.createWeight(searcher, needsScores, boost);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName());
    buffer.append("(");
    buffer.append(bigQuery != null ? bigQuery.toString(field) : "null");
    buffer.append(", ");
    buffer.append(smallQuery != null ? smallQuery.toString(field) : "null");
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newBigQuery = bigQuery.rewrite(reader);
    MtasSpanQuery newSmallQuery = smallQuery.rewrite(reader);
    if (newBigQuery == null || newBigQuery instanceof MtasSpanMatchNoneQuery
        || newSmallQuery == null
        || newSmallQuery instanceof MtasSpanMatchNoneQuery) {
      return new MtasSpanMatchNoneQuery(field);
    }
    if (!newBigQuery.equals(bigQuery) || !newSmallQuery.equals(smallQuery)) {
      return new MtasSpanContainingQuery(newBigQuery, newSmallQuery)
          .rewrite(reader);
    } else if (newBigQuery.equals(newSmallQuery)) {
      return newBigQuery;
    } else if (baseQuery == null) {
      return new MtasSpanMatchNoneQuery(field);
    } else {
      baseQuery = (SpanContainingQuery) baseQuery.rewrite(reader);
      return super.rewrite(reader);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanContainingQuery that = (MtasSpanContainingQuery) obj;
    return baseQuery.equals(that.baseQuery);
  }

  @Override
  public int hashCode() {
    return baseQuery.hashCode();
  }

  @Override
  public void disableTwoPhaseIterator() {
    super.disableTwoPhaseIterator();
    bigQuery.disableTwoPhaseIterator();
    smallQuery.disableTwoPhaseIterator();
  }
  
  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }
}
