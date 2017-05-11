package mtas.search.spans;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSpanOrQuery.
 */
public class MtasSpanOrQuery extends MtasSpanQuery {

  /** The clauses. */
  private HashSet<MtasSpanQuery> clauses;

  private SpanQuery baseQuery;

  /**
   * Instantiates a new mtas span or query.
   *
   * @param clauses
   *          the clauses
   */
  public MtasSpanOrQuery(MtasSpanQuery... initialClauses) {
    super(null, null);
    Integer minimum = null, maximum = null;
    clauses = new HashSet<MtasSpanQuery>();
    for (MtasSpanQuery item : initialClauses) {
      if (!clauses.contains(item)) {
        minimum = clauses.isEmpty() ? item.getMinimumWidth()
            : (minimum != null && item.getMinimumWidth() != null)
                ? Math.min(minimum, item.getMinimumWidth()) : null;
        maximum = clauses.isEmpty() ? item.getMaximumWidth()
            : (maximum != null && item.getMaximumWidth() != null)
                ? Math.max(maximum, item.getMaximumWidth()) : null;
        clauses.add(item);
      }
    }
    setWidth(minimum, maximum);
    baseQuery = new SpanOrQuery(
        clauses.toArray(new MtasSpanQuery[clauses.size()]));
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

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    if (clauses.size() > 1) {
      // rewrite, count MtasSpanMatchAllQuery and check for
      // MtasSpanMatchNoneQuery
      MtasSpanQuery[] newClauses = new MtasSpanQuery[clauses.size()];
      MtasSpanQuery[] oldClauses = clauses
          .toArray(new MtasSpanQuery[clauses.size()]);
      int singlePositionQueries = 0;
      int matchAllSinglePositionQueries = 0;
      int matchNoneQueries = 0;
      boolean actuallyRewritten = false;
      for (int i = 0; i < oldClauses.length; i++) {
        newClauses[i] = oldClauses[i].rewrite(reader);
        actuallyRewritten |= oldClauses[i] != newClauses[i];
        if (newClauses[i] instanceof MtasSpanMatchNoneQuery) {
          matchNoneQueries++;
        } else if (newClauses[i].isSinglePositionQuery()) {
          singlePositionQueries++;
          if (newClauses[i] instanceof MtasSpanMatchAllQuery) {
            matchAllSinglePositionQueries++;
          }
        }
      }
      // filter clauses
      if (matchNoneQueries > 0 || matchAllSinglePositionQueries > 0) {
        // compute new number of clauses
        int newNumber = newClauses.length - matchNoneQueries;
        if (matchAllSinglePositionQueries > 0) {
          newNumber -= singlePositionQueries - 1;
        }
        MtasSpanQuery[] newFilteredClauses = new MtasSpanQuery[newNumber];
        int j = 0;
        for (int i = 0; i < newClauses.length; i++) {
          if (!(newClauses[i] instanceof MtasSpanMatchNoneQuery)) {
            if (!newClauses[i].isSinglePositionQuery()) {
              newFilteredClauses[j] = newClauses[i];
              j++;
            } else if (matchAllSinglePositionQueries == 0) {
              newFilteredClauses[j] = newClauses[i];
              j++;
            } else if (singlePositionQueries > 0) {
              newFilteredClauses[j] = newClauses[i];
              j++;
              singlePositionQueries = 0; // only match this condition once
            }
          }
        }
        newClauses = newFilteredClauses;
      }
      if (newClauses.length == 0) {
        return (new MtasSpanMatchNoneQuery(this.getField())).rewrite(reader);
      } else if (newClauses.length == 1) {
        return newClauses[0].rewrite(reader);
      } else if (actuallyRewritten || newClauses.length != clauses.size()) {
        return new MtasSpanOrQuery(newClauses).rewrite(reader);
      } else {
        return super.rewrite(reader);
      }
    } else if (clauses.size() == 1) {
      return clauses.iterator().next().rewrite(reader);
    } else {
      return (new MtasSpanMatchNoneQuery(this.getField())).rewrite(reader);
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
    h = (h * 7) ^ clauses.hashCode();
    return h;
  }

}
