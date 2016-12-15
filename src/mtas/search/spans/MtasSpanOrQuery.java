package mtas.search.spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.util.MtasSpanQuery;
import mtas.search.spans.util.MtasSpanUniquePositionQuery;

/**
 * The Class MtasSpanOrQuery.
 */
public class MtasSpanOrQuery extends MtasSpanQuery {

  /** The clauses. */
  private List<MtasSpanQuery> clauses;
  
  private SpanQuery baseQuery;

  /**
   * Instantiates a new mtas span or query.
   *
   * @param clauses
   *          the clauses
   */
  public MtasSpanOrQuery(MtasSpanQuery... clauses) {
    super();
    baseQuery = new MtasSpanUniquePositionQuery(new SpanOrQuery(clauses));
    this.clauses = new ArrayList<>(clauses.length);
    for (MtasSpanQuery clause : clauses) {
      this.clauses.add(clause);
    }
  }
  
  @Override
  public String getField() {
    return baseQuery.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return baseQuery.createWeight(searcher, needsScores);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.search.spans.MtasSpanUniquePositionQuery#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    Iterator<MtasSpanQuery> i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("])");
    return buffer.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.search.spans.MtasSpanUniquePositionQuery#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanOrQuery that = (MtasSpanOrQuery) obj;
    return clauses.equals(that.clauses);
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.search.spans.MtasSpanUniquePositionQuery#hashCode()
   */
  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ baseQuery.hashCode();
    return h;
  }  

}
