package mtas.search.spans;

import java.io.IOException;
import java.util.HashSet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanWeight;

import mtas.search.spans.util.MtasExtendedSpanAndQuery;
import mtas.search.spans.util.MtasSpanQuery;

/**
 * The Class MtasSpanAndQuery.
 */
public class MtasSpanAndQuery extends MtasSpanQuery {

  /** The base query. */
  private SpanNearQuery baseQuery;
  
  /** The clauses. */
  private HashSet<MtasSpanQuery> clauses;

  /**
   * Instantiates a new mtas span and query.
   *
   * @param initialClauses the initial clauses
   */
  public MtasSpanAndQuery(MtasSpanQuery... initialClauses) {
    super(null, null);
    Integer minimum = null;
    Integer maximum = null;
    clauses = new HashSet<>();
    for (MtasSpanQuery item : initialClauses) {
      if (!clauses.contains(item)) {
        clauses.add(item);
        if (item.getMinimumWidth() != null) {
          if(minimum != null) {
            minimum = Math.max(minimum, item.getMinimumWidth());
          } else {
            minimum = item.getMinimumWidth();
          }
        }
        if (item.getMaximumWidth() != null) {          
          if(maximum != null) {
            maximum = Math.max(maximum, item.getMaximumWidth());
          } else {
            maximum = item.getMaximumWidth();
          }
        }
      }
    }
    setWidth(minimum, maximum);
    baseQuery = new MtasExtendedSpanAndQuery(
        clauses.toArray(new MtasSpanQuery[clauses.size()]));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanQuery#getField()
   */
  @Override
  public String getField() {
    return baseQuery.getField();
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

  /* (non-Javadoc)
   * @see mtas.search.spans.util.MtasSpanQuery#rewrite(org.apache.lucene.index.IndexReader)
   */
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
      boolean actuallyRewritten = false;
      for (int i = 0; i < oldClauses.length; i++) {
        newClauses[i] = oldClauses[i].rewrite(reader);
        actuallyRewritten |= !oldClauses[i].equals(newClauses[i]);
        if (newClauses[i] instanceof MtasSpanMatchNoneQuery) {
          return (new MtasSpanMatchNoneQuery(this.getField())).rewrite(reader);
        } else {
          if (newClauses[i].isSinglePositionQuery()) {
            singlePositionQueries++;
            if (newClauses[i] instanceof MtasSpanMatchAllQuery) {
              matchAllSinglePositionQueries++;
            }
          }
        }
      }
      // filter clauses
      if (matchAllSinglePositionQueries > 0) {
        // compute new number of clauses
        int newNumber = newClauses.length - matchAllSinglePositionQueries;
        if (matchAllSinglePositionQueries == singlePositionQueries) {
          newNumber++;
        }
        MtasSpanQuery[] newFilteredClauses = new MtasSpanQuery[newNumber];
        int j = 0;
        for (int i = 0; i < newClauses.length; i++) {
          if (!(newClauses[i].isSinglePositionQuery()
              && (newClauses[i] instanceof MtasSpanMatchAllQuery))) {
            newFilteredClauses[j] = newClauses[i];
            j++;
          } else if (matchAllSinglePositionQueries == singlePositionQueries) {
            newFilteredClauses[j] = newClauses[i];
            j++;
            singlePositionQueries++; // only match this condition once
          }
        }
        newClauses = newFilteredClauses;
      }
      if (newClauses.length == 0) {
        return (new MtasSpanMatchNoneQuery(this.getField())).rewrite(reader);
      } else if (newClauses.length == 1) {
        return newClauses[0].rewrite(reader);
      } else if (actuallyRewritten || newClauses.length != clauses.size()) {
        return new MtasSpanAndQuery(newClauses).rewrite(reader);
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
   * org.apache.lucene.search.spans.SpanNearQuery#toString(java.lang.String)
   */
  @Override
  public String toString(String field) {
    return baseQuery.toString(field);
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
    final MtasSpanAndQuery that = (MtasSpanAndQuery) obj;
    return baseQuery.equals(that.baseQuery);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.search.spans.SpanNearQuery#hashCode()
   */
  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ clauses.hashCode();
    return h;
  }

}
