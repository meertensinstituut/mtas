package mtas.search.spans.util;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.MtasSpanMatchAllQuery;
import mtas.search.spans.MtasSpanMatchNoneQuery;

public abstract class MtasSpanQuery extends SpanQuery {

  private Integer minimumSpanWidth, maximumSpanWidth, spanWidth;
  private boolean singlePositionQuery;
  
  public MtasSpanQuery(Integer minimum, Integer maximum) {
    super();
    initialize(minimum, maximum);
  }

  public void setWidth(Integer minimum, Integer maximum) {
    initialize(minimum, maximum);
  }

  private void initialize(Integer minimum, Integer maximum) {
    minimumSpanWidth = minimum;
    maximumSpanWidth = maximum;
    spanWidth = (minimum != null && maximum != null && minimum.equals(maximum))
        ? minimum : null;
    singlePositionQuery = spanWidth != null && spanWidth.equals(1);   
  }

  @Override
  public abstract SpanWeight createWeight(IndexSearcher searcher,
      boolean needsScores) throws IOException;

  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newClause = this;
    if(minimumSpanWidth!=null && maximumSpanWidth!=null && minimumSpanWidth>maximumSpanWidth) {
      newClause = new MtasSpanMatchNoneQuery(this.getField());
    } 
    return (newClause!=this)?newClause:this;
  }

  public Integer getWidth() {
    return spanWidth;
  }

  public Integer getMinimumWidth() {
    return minimumSpanWidth;
  }

  public Integer getMaximumWidth() {
    return maximumSpanWidth;
  }
  
  public boolean isSinglePositionQuery() {
    return singlePositionQuery;
  }

}
