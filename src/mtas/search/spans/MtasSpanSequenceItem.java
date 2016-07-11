package mtas.search.spans;

import org.apache.lucene.search.spans.SpanQuery;

public class MtasSpanSequenceItem {
  private SpanQuery spanQuery;
  private boolean optional;
  
  public MtasSpanSequenceItem(SpanQuery spanQuery, boolean optional) {
    this.spanQuery = spanQuery;
    this.optional = optional;
  }
  
  public SpanQuery getQuery() {
    return spanQuery;
  }
  
  public void setQuery(SpanQuery spanQuery) {
    this.spanQuery = spanQuery;
  }
  
  public boolean isOptional() {
    return optional;
  }
  
  @Override
  public MtasSpanSequenceItem clone() {
    MtasSpanSequenceItem item = new MtasSpanSequenceItem(spanQuery, optional);
    return item;
  }
  
  @Override
  public boolean equals(Object o) {
    if(o instanceof MtasSpanSequenceItem) {
      final MtasSpanSequenceItem that = (MtasSpanSequenceItem) o;
      return spanQuery.equals(that.getQuery()) && (optional==that.isOptional());
    } else {
      return false;
    }
  }
  
}
