package mtas.search.spans.util;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import mtas.search.spans.MtasSpanMatchNoneQuery;

/**
 * The Class MtasSpanQuery.
 */
public abstract class MtasSpanQuery extends SpanQuery {

  /** The span width. */
  private Integer minimumSpanWidth, maximumSpanWidth, spanWidth;
  
  /** The single position query. */
  private boolean singlePositionQuery;
  
  /**
   * Instantiates a new mtas span query.
   *
   * @param minimum the minimum
   * @param maximum the maximum
   */
  public MtasSpanQuery(Integer minimum, Integer maximum) {
    super();
    initialize(minimum, maximum);
  }

  /**
   * Sets the width.
   *
   * @param minimum the minimum
   * @param maximum the maximum
   */
  public void setWidth(Integer minimum, Integer maximum) {
    initialize(minimum, maximum);
  }

  /**
   * Initialize.
   *
   * @param minimum the minimum
   * @param maximum the maximum
   */
  private void initialize(Integer minimum, Integer maximum) {
    minimumSpanWidth = minimum;
    maximumSpanWidth = maximum;
    spanWidth = (minimum != null && maximum != null && minimum.equals(maximum))
        ? minimum : null;
    singlePositionQuery = spanWidth != null && spanWidth.equals(1);   
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.spans.SpanQuery#createWeight(org.apache.lucene.search.IndexSearcher, boolean)
   */
  @Override
  public abstract SpanWeight createWeight(IndexSearcher searcher,
      boolean needsScores) throws IOException;

  /* (non-Javadoc)
   * @see org.apache.lucene.search.Query#rewrite(org.apache.lucene.index.IndexReader)
   */
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    if(minimumSpanWidth!=null && maximumSpanWidth!=null && minimumSpanWidth>maximumSpanWidth) {
      return new MtasSpanMatchNoneQuery(this.getField());
    } else {
      return this;
    }
  }

  /**
   * Gets the width.
   *
   * @return the width
   */
  public Integer getWidth() {
    return spanWidth;
  }

  /**
   * Gets the minimum width.
   *
   * @return the minimum width
   */
  public Integer getMinimumWidth() {
    return minimumSpanWidth;
  }

  /**
   * Gets the maximum width.
   *
   * @return the maximum width
   */
  public Integer getMaximumWidth() {
    return maximumSpanWidth;
  }
  
  /**
   * Checks if is single position query.
   *
   * @return true, if is single position query
   */
  public boolean isSinglePositionQuery() {
    return singlePositionQuery;
  }

}
