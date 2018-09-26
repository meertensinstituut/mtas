package mtas.search.spans.util;

import mtas.search.spans.MtasSpanMatchNoneQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spans.SpanQuery;

import java.io.IOException;

public abstract class MtasSpanQuery extends SpanQuery {
  private Integer minimumSpanWidth;
  private Integer maximumSpanWidth;
  private Integer spanWidth;
  private boolean singlePositionQuery;
  private boolean allowTwoPhaseIterator;

  public MtasSpanQuery(Integer minimum, Integer maximum) {
    super();
    initialize(minimum, maximum);
    allowTwoPhaseIterator = true;
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

  // public abstract MtasSpanWeight createWeight(IndexSearcher searcher,
  // boolean needsScores) throws IOException;

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    if (minimumSpanWidth != null && maximumSpanWidth != null
        && minimumSpanWidth > maximumSpanWidth) {
      return new MtasSpanMatchNoneQuery(this.getField());
    } else {
      return this;
    }
  }

  public final Integer getWidth() {
    return spanWidth;
  }

  public final Integer getMinimumWidth() {
    return minimumSpanWidth;
  }

  public final Integer getMaximumWidth() {
    return maximumSpanWidth;
  }

  public void disableTwoPhaseIterator() {
    allowTwoPhaseIterator = false;
  }

  public final boolean twoPhaseIteratorAllowed() {
    return allowTwoPhaseIterator;
  }

  public final boolean isSinglePositionQuery() {
    return singlePositionQuery;
  }
  
  public abstract boolean isMatchAllPositionsQuery();
  
  @Override
  public abstract boolean equals(Object object);

  @Override
  public abstract int hashCode();
}
