package mtas.search.spans;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;

public class MtasSpanAndQuery extends SpanNearQuery {

  private List<SpanQuery> clauses;
  private static String QUERY_NAME = "mtasSpanAndQuery";

  public MtasSpanAndQuery(SpanQuery... clauses) {
    super(clauses, -1 * (clauses.length - 1), false);
    this.clauses = new ArrayList<>(clauses.length);
    for (SpanQuery clause : clauses) {
      this.clauses.add(clause);
    }    
  }

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

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj== null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanAndQuery that = (MtasSpanAndQuery) obj;
    return clauses.equals(that.clauses);    
  }
  
  @Override
  public int hashCode() {
    int h = QUERY_NAME.hashCode();
    h = (h * 7) ^ super.hashCode();
    return h;
  }



}
