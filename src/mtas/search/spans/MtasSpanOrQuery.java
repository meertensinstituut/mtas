package mtas.search.spans;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;

/**
 * The Class MtasSpanOrQuery.
 */
public class MtasSpanOrQuery extends MtasSpanUniquePositionQuery {

  /** The clauses. */
  private List<SpanQuery> clauses;

  /** The query name. */
  private static String QUERY_NAME = "mtasSpanOrQuery";

  /**
   * Instantiates a new mtas span or query.
   *
   * @param clauses
   *          the clauses
   */
  public MtasSpanOrQuery(SpanQuery... clauses) {
    super(new SpanOrQuery(clauses));
    this.clauses = new ArrayList<>(clauses.length);
    for (SpanQuery clause : clauses) {
      this.clauses.add(clause);
    }
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
    buffer.append(QUERY_NAME + "([");
    Iterator<SpanQuery> i = clauses.iterator();
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
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ super.hashCode();
    return h;
  }

}
