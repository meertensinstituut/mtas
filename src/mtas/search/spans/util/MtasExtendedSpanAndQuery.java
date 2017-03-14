package mtas.search.spans.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;

/**
 * The Class MtasExtendedSpanAndQuery.
 */
public class MtasExtendedSpanAndQuery extends SpanNearQuery {

  /** The clauses. */
  private HashSet<SpanQuery> clauses;

  /**
   * Instantiates a new mtas extended span and query.
   *
   * @param clauses the clauses
   */
  public MtasExtendedSpanAndQuery(SpanQuery... clauses) {
    super(clauses, -1 * (clauses.length - 1), false);
    this.clauses = new HashSet<SpanQuery>();
    for (SpanQuery clause : clauses) {
      this.clauses.add(clause);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.spans.SpanNearQuery#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
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
   * @see org.apache.lucene.search.spans.SpanNearQuery#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasExtendedSpanAndQuery that = (MtasExtendedSpanAndQuery) obj;    
    return clauses.equals(that.clauses);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanNearQuery#hashCode()
   */
  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ super.hashCode();
    return h;
  }
  
  
}
