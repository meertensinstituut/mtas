package mtas.search.spans;

import mtas.search.spans.util.MtasSpanQuery;
import mtas.search.spans.util.MtasSpanWeight;
import mtas.search.spans.util.MtasSpans;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class MtasSpanRecurrenceQuery extends MtasSpanQuery {
  private MtasSpanQuery query;
  private int minimumRecurrence;
  private int maximumRecurrence;
  private MtasSpanQuery ignoreQuery;
  private Integer maximumIgnoreLength;
  private String field;

  public MtasSpanRecurrenceQuery(MtasSpanQuery query, int minimumRecurrence,
      int maximumRecurrence, MtasSpanQuery ignoreQuery,
      Integer maximumIgnoreLength) {
    super(null, null);
    field = query.getField();
    this.query = query;
    if (field != null && ignoreQuery != null) {
      if (ignoreQuery.getField() == null
          || field.equals(ignoreQuery.getField())) {
        this.ignoreQuery = ignoreQuery;
        if (maximumIgnoreLength == null) {
          this.maximumIgnoreLength = 1;
        } else {
          this.maximumIgnoreLength = maximumIgnoreLength;
        }
      } else {
        throw new IllegalArgumentException(
            "ignore must have same field as clauses");
      }
    } else {
      this.ignoreQuery = null;
      this.maximumIgnoreLength = null;
    }
    setRecurrence(minimumRecurrence, maximumRecurrence);
  }

  public MtasSpanQuery getQuery() {
    return query;
  }

  public MtasSpanQuery getIgnoreQuery() {
    return ignoreQuery;
  }

  public Integer getMaximumIgnoreLength() {
    return maximumIgnoreLength;
  }

  public int getMinimumRecurrence() {
    return minimumRecurrence;
  }

  public int getMaximumRecurrence() {
    return maximumRecurrence;
  }

  public void setRecurrence(int minimumRecurrence, int maximumRecurrence) {
    if (minimumRecurrence > maximumRecurrence) {
      throw new IllegalArgumentException(
          "minimumRecurrence > maximumRecurrence");
    } else if (minimumRecurrence < 1) {
      throw new IllegalArgumentException("minimumRecurrence < 1 not supported");
    } else if (query == null) {
      throw new IllegalArgumentException("no clause");
    }
    this.minimumRecurrence = minimumRecurrence;
    this.maximumRecurrence = maximumRecurrence;
    // set minimum/maximum
    Integer minimum = null;
    Integer maximum = null;
    if (query.getMinimumWidth() != null) {
      minimum = minimumRecurrence * query.getMinimumWidth();
    }
    if (query.getMaximumWidth() != null) {
      maximum = maximumRecurrence * query.getMaximumWidth();
      if (ignoreQuery != null && maximumIgnoreLength != null) {
        if (ignoreQuery.getMaximumWidth() != null) {
          maximum += (maximumRecurrence - 1) * maximumIgnoreLength
              * ignoreQuery.getMaximumWidth();
        } else {
          maximum = null;
        }
      }
    }
    setWidth(minimum, maximum);
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    MtasSpanQuery newQuery = query.rewrite(reader);
    if (maximumRecurrence == 1) {
      return newQuery;
    } else {
      MtasSpanQuery newIgnoreQuery = (ignoreQuery != null)
          ? ignoreQuery.rewrite(reader) : null;
      if (newQuery instanceof MtasSpanRecurrenceQuery) {
        // for now too difficult, possibly merge later
      }
      if (!newQuery.equals(query)
          || (newIgnoreQuery != null && !newIgnoreQuery.equals(ignoreQuery))) {
        return new MtasSpanRecurrenceQuery(newQuery, minimumRecurrence,
            maximumRecurrence, newIgnoreQuery, maximumIgnoreLength)
                .rewrite(reader);
      } else {
        return super.rewrite(reader);
      }
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    buffer.append(query.toString(query.getField()));
    buffer.append("," + minimumRecurrence + "," + maximumRecurrence);
    buffer.append(", ");
    buffer.append(ignoreQuery);
    buffer.append("])");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSpanRecurrenceQuery other = (MtasSpanRecurrenceQuery) obj;
    boolean result;
    result = query.equals(other.query);
    result &= minimumRecurrence == other.minimumRecurrence;
    result &= maximumRecurrence == other.maximumRecurrence;
    if (result) {
      boolean subResult;
      subResult = ignoreQuery == null && other.ignoreQuery == null;
      subResult |= ignoreQuery != null && other.ignoreQuery != null
          && ignoreQuery.equals(other.ignoreQuery);
      return subResult;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 7) ^ query.hashCode();
    h = (h * 11) ^ minimumRecurrence;
    h = (h * 13) ^ maximumRecurrence;
    return h;
  }

  @Override
  public MtasSpanWeight createWeight(IndexSearcher searcher,
      boolean needsScores, float boost) throws IOException {
    SpanWeight subWeight = query.createWeight(searcher, false, boost);
    SpanWeight ignoreWeight = null;
    if (ignoreQuery != null) {
      ignoreWeight = ignoreQuery.createWeight(searcher, false, boost);
    }
    return new SpanRecurrenceWeight(subWeight, ignoreWeight,
        maximumIgnoreLength, searcher,
        needsScores ? getTermContexts(subWeight) : null, boost);
  }

  @Override
  public void disableTwoPhaseIterator() {
    super.disableTwoPhaseIterator();
    query.disableTwoPhaseIterator();
    if (ignoreQuery != null) {
      ignoreQuery.disableTwoPhaseIterator();
    }
  }

  protected class SpanRecurrenceWeight extends MtasSpanWeight {

    final SpanWeight subWeight;

    final SpanWeight ignoreWeight;

    final Integer maximumIgnoreLength;

    public SpanRecurrenceWeight(SpanWeight subWeight, SpanWeight ignoreWeight,
        Integer maximumIgnoreLength, IndexSearcher searcher,
        Map<Term, TermContext> terms, float boost) throws IOException {
      super(MtasSpanRecurrenceQuery.this, searcher, terms, boost);
      this.subWeight = subWeight;
      this.ignoreWeight = ignoreWeight;
      this.maximumIgnoreLength = maximumIgnoreLength;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      subWeight.extractTermContexts(contexts);
    }

    @Override
    public MtasSpans getSpans(LeafReaderContext context,
        Postings requiredPostings) throws IOException {
      if (field == null) {
        return null;
      } else {
        Terms terms = context.reader().terms(field);
        if (terms == null) {
          return null; // field does not exist
        }
        Spans subSpans = subWeight.getSpans(context, requiredPostings);
        if (subSpans == null) {
          return null;
        } else {
          Spans ignoreSpans = null;
          if (ignoreWeight != null) {
            ignoreSpans = ignoreWeight.getSpans(context, requiredPostings);
          }
          return new MtasSpanRecurrenceSpans(MtasSpanRecurrenceQuery.this,
              subSpans, minimumRecurrence, maximumRecurrence, ignoreSpans,
              maximumIgnoreLength);
        }
      }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      subWeight.extractTerms(terms);
    }
    
//    @Override
//    public boolean isCacheable(LeafReaderContext arg0) {
//      return subWeight.isCacheable(arg0) && (ignoreWeight==null || ignoreWeight.isCacheable(arg0));
//    }

  }
  
  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }
}
