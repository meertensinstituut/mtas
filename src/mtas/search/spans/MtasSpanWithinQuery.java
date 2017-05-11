package mtas.search.spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.SpanWithinQuery;
import mtas.search.spans.util.MtasSpanMaximumExpandQuery;
import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSpanWithinQuery.
 */
public class MtasSpanWithinQuery extends MtasSpanQuery {

  /** The base query. */
  private SpanWithinQuery baseQuery;
  private MtasSpanQuery smallQuery, bigQuery;
  private int leftBoundaryMinimum, leftBoundaryMaximum, rightBoundaryMaximum,
      rightBoundaryMinimum;
  private boolean autoAdjustBigQuery;
  String field;

  /**
   * Instantiates a new mtas span within query.
   *
   * @param q1
   *          the q1
   * @param q2
   *          the q2
   */

  public MtasSpanWithinQuery(MtasSpanQuery q1, MtasSpanQuery q2) {
    this(q1, q2, 0, 0, 0, 0, true);
  }

  public MtasSpanWithinQuery(MtasSpanQuery q1, MtasSpanQuery q2,
      int leftMinimum, int leftMaximum, int rightMinimum, int rightMaximum,
      boolean adjustBigQuery) {
    super(q1 != null ? q1.getMinimumWidth() : null,
        q1 != null ? q1.getMaximumWidth() : null);
    if (q2 != null && q2.getMinimumWidth() != null) {
      if (this.getMinimumWidth() == null
          || this.getMinimumWidth() < q2.getMinimumWidth()) {
        this.setWidth(q2.getMinimumWidth(), this.getMaximumWidth());
      }
    }
    bigQuery = q1;
    smallQuery = q2;
    leftBoundaryMinimum = leftMinimum;
    leftBoundaryMaximum = leftMaximum;
    rightBoundaryMinimum = rightMinimum;
    rightBoundaryMaximum = rightMaximum;
    autoAdjustBigQuery = adjustBigQuery;
    if (bigQuery!=null && bigQuery.getField() != null) {
      field = bigQuery.getField();
    } else if (smallQuery!=null && smallQuery.getField() != null) {
      field = smallQuery.getField();
    } else {
      field = null;
    }
    if (field != null) {
      baseQuery = new SpanWithinQuery(
          new MtasSpanMaximumExpandQuery(bigQuery, leftBoundaryMinimum,
              leftBoundaryMaximum, rightBoundaryMinimum, rightBoundaryMaximum),
          smallQuery);
    } else {
      baseQuery = null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.search.spans.util.MtasSpanQuery#rewrite(org.apache.lucene.index.
   * IndexReader)
   */
  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newBigQuery = bigQuery.rewrite(reader);
    MtasSpanQuery newSmallQuery = smallQuery.rewrite(reader);

    if (newBigQuery == null || newBigQuery instanceof MtasSpanMatchNoneQuery
        || newSmallQuery == null
        || newSmallQuery instanceof MtasSpanMatchNoneQuery) {
      return new MtasSpanMatchNoneQuery(field);
    }

    if (autoAdjustBigQuery) {
      if (newBigQuery instanceof MtasSpanRecurrenceQuery) {
        MtasSpanRecurrenceQuery recurrenceQuery = (MtasSpanRecurrenceQuery) newBigQuery;
        if (recurrenceQuery.getIgnoreQuery() == null
            && recurrenceQuery.getQuery() instanceof MtasSpanMatchAllQuery) {
          rightBoundaryMaximum += leftBoundaryMaximum
              + recurrenceQuery.getMaximumRecurrence();
          rightBoundaryMinimum += leftBoundaryMinimum
              + recurrenceQuery.getMinimumRecurrence();
          leftBoundaryMaximum = 0;
          leftBoundaryMinimum = 0;
          newBigQuery = new MtasSpanMatchAllQuery(field);
          // System.out.println("REPLACE WITH " + newBigQuery + " (["
          // + leftBoundaryMinimum + "," + leftBoundaryMaximum + "],["
          // + rightBoundaryMinimum + "," + rightBoundaryMaximum + "])");
          return new MtasSpanWithinQuery(newBigQuery, newSmallQuery,
              leftBoundaryMinimum, leftBoundaryMaximum, rightBoundaryMinimum,
              rightBoundaryMaximum, autoAdjustBigQuery).rewrite(reader);
        }
      } else if (newBigQuery instanceof MtasSpanMatchAllQuery) {
        if (leftBoundaryMaximum > 0) {
          rightBoundaryMaximum += leftBoundaryMaximum;
          rightBoundaryMinimum += leftBoundaryMinimum;
          leftBoundaryMaximum = 0;
          leftBoundaryMinimum = 0;
          // System.out.println("REPLACE WITH " + newBigQuery + " (["
          // + leftBoundaryMinimum + "," + leftBoundaryMaximum + "],["
          // + rightBoundaryMinimum + "," + rightBoundaryMaximum + "])");
          return new MtasSpanWithinQuery(newBigQuery, newSmallQuery,
              leftBoundaryMinimum, leftBoundaryMaximum, rightBoundaryMinimum,
              rightBoundaryMaximum, autoAdjustBigQuery).rewrite(reader);
        }
      } else if (newBigQuery instanceof MtasSpanSequenceQuery) {
        MtasSpanSequenceQuery sequenceQuery = (MtasSpanSequenceQuery) newBigQuery;
        if (sequenceQuery.getIgnoreQuery() == null) {
          List<MtasSpanSequenceItem> items = sequenceQuery.getItems();
          List<MtasSpanSequenceItem> newItems = new ArrayList<MtasSpanSequenceItem>();
          int newLeftBoundaryMinimum = 0, newLeftBoundaryMaximum = 0,
              newRightBoundaryMinimum = 0, newRightBoundaryMaximum = 0;
          for (int i = 0; i < items.size(); i++) {
            // first item
            if (i == 0) {
              if (items.get(i).getQuery() instanceof MtasSpanMatchAllQuery) {
                newLeftBoundaryMaximum++;
                if (!items.get(i).isOptional()) {
                  newLeftBoundaryMinimum++;
                }
              } else if (items.get(i)
                  .getQuery() instanceof MtasSpanRecurrenceQuery) {
                MtasSpanRecurrenceQuery msrq = (MtasSpanRecurrenceQuery) items
                    .get(i).getQuery();
                if (msrq.getQuery() instanceof MtasSpanMatchAllQuery) {
                  newLeftBoundaryMaximum += msrq.getMaximumRecurrence();
                  if (!items.get(i).isOptional()) {
                    newLeftBoundaryMinimum += msrq.getMinimumRecurrence();
                  }
                } else {
                  newItems.add(items.get(i));
                }
              } else {
                newItems.add(items.get(i));
              }
              // last item
            } else if (i == (items.size() - 1)) {
              if (items.get(i).getQuery() instanceof MtasSpanMatchAllQuery) {
                newRightBoundaryMaximum++;
                if (!items.get(i).isOptional()) {
                  newRightBoundaryMinimum++;
                }
              } else if (items.get(i)
                  .getQuery() instanceof MtasSpanRecurrenceQuery) {
                MtasSpanRecurrenceQuery msrq = (MtasSpanRecurrenceQuery) items
                    .get(i).getQuery();
                if (msrq.getQuery() instanceof MtasSpanMatchAllQuery) {
                  newRightBoundaryMaximum += msrq.getMaximumRecurrence();
                  if (!items.get(i).isOptional()) {
                    newRightBoundaryMinimum += msrq.getMinimumRecurrence();
                  }
                } else {
                  newItems.add(items.get(i));
                }
              } else {
                newItems.add(items.get(i));
              }
              // other items
            } else {
              newItems.add(items.get(i));
            }
          }
          leftBoundaryMaximum += newLeftBoundaryMaximum;
          leftBoundaryMinimum += newLeftBoundaryMinimum;
          rightBoundaryMaximum += newRightBoundaryMaximum;
          rightBoundaryMinimum += newRightBoundaryMinimum;
          if (newItems.isEmpty()) {
            rightBoundaryMaximum = Math.max(0,
                rightBoundaryMaximum + leftBoundaryMaximum - 1);
            rightBoundaryMinimum = Math.max(0,
                rightBoundaryMinimum + leftBoundaryMinimum - 1);
            leftBoundaryMaximum = 0;
            leftBoundaryMinimum = 0;
            newItems.add(new MtasSpanSequenceItem(
                new MtasSpanMatchAllQuery(field), false));
          }
          if (!items.equals(newItems) || newLeftBoundaryMaximum > 0
              || newRightBoundaryMaximum > 0) {
            newBigQuery = (new MtasSpanSequenceQuery(newItems, null, null))
                .rewrite(reader);
            // System.out.println("REPLACE WITH " + newBigQuery + " (["
            // + leftBoundaryMinimum + "," + leftBoundaryMaximum + "],["
            // + rightBoundaryMinimum + "," + rightBoundaryMaximum + "])");
            return new MtasSpanWithinQuery(newBigQuery, newSmallQuery,
                leftBoundaryMinimum, leftBoundaryMaximum, rightBoundaryMinimum,
                rightBoundaryMaximum, autoAdjustBigQuery).rewrite(reader);
          }
        }
      }
    }

    if (!newBigQuery.equals(bigQuery) || !newSmallQuery.equals(smallQuery)) {
      return (new MtasSpanWithinQuery(newBigQuery, newSmallQuery,
          leftBoundaryMinimum, leftBoundaryMaximum, rightBoundaryMinimum,
          rightBoundaryMaximum, autoAdjustBigQuery)).rewrite(reader);
    } else if (newBigQuery.equals(newSmallQuery)) {
      return newBigQuery;
    } else {
      baseQuery = (SpanWithinQuery) baseQuery.rewrite(reader);
      return super.rewrite(reader);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return field;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.spans.SpanQuery#createWeight(org.apache.lucene.
   * search.IndexSearcher, boolean)
   */
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return baseQuery.createWeight(searcher, needsScores);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    if (smallQuery != null) {
      buffer.append(smallQuery.toString(smallQuery.getField()));
    } else {
      buffer.append("null");
    }
    buffer.append(",");
    if (bigQuery != null) {
      buffer.append(bigQuery.toString(bigQuery.getField()));
    } else {
      buffer.append("null");
    }
    buffer.append("],[" + leftBoundaryMinimum + "," + leftBoundaryMaximum
        + "],[" + rightBoundaryMinimum + "," + rightBoundaryMaximum + "])");
    return buffer.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanWithinQuery that = (MtasSpanWithinQuery) obj;
    return baseQuery.equals(that.baseQuery)
        && leftBoundaryMinimum == that.leftBoundaryMinimum
        && leftBoundaryMaximum == that.leftBoundaryMaximum
        && rightBoundaryMinimum == that.rightBoundaryMinimum
        && rightBoundaryMaximum == that.rightBoundaryMaximum;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.Query#hashCode()
   */
  @Override
  public int hashCode() {
    int h = Integer.rotateLeft(classHash(), 1);
    h ^= smallQuery.hashCode();
    h = Integer.rotateLeft(h, 1);
    h ^= bigQuery.hashCode();
    h = Integer.rotateLeft(h, leftBoundaryMinimum) + leftBoundaryMinimum;
    h ^= 2;
    h = Integer.rotateLeft(h, leftBoundaryMaximum) + leftBoundaryMaximum;
    h ^= 3;
    h = Integer.rotateLeft(h, rightBoundaryMinimum) + rightBoundaryMinimum;
    h ^= 5;
    h = Integer.rotateLeft(h, rightBoundaryMaximum) + rightBoundaryMaximum;
    return h;
  }

}
