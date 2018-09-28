package mtas.search.spans;

import mtas.search.spans.util.MtasExpandSpanQuery;
import mtas.search.spans.util.MtasIgnoreItem;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MtasSpanSequenceQuery extends MtasSpanQuery {
  private List<MtasSpanSequenceItem> items;
  private int leftMinimum;
  private int leftMaximum;
  private int rightMinimum;
  private int rightMaximum;
  private MtasSpanQuery ignoreQuery;
  private Integer maximumIgnoreLength;
  private String field;

  public MtasSpanSequenceQuery(List<MtasSpanSequenceItem> items,
      MtasSpanQuery ignoreQuery, Integer maximumIgnoreLength) {
    this(items, 0, 0, 0, 0, ignoreQuery, maximumIgnoreLength);
  }

  public MtasSpanSequenceQuery(List<MtasSpanSequenceItem> items,
      int leftMinimum, int leftMaximum, int rightMinimum, int rightMaximum,
      MtasSpanQuery ignoreQuery, Integer maximumIgnoreLength) {
    super(null, null);
    this.items = items;
    this.leftMinimum = leftMinimum;
    this.leftMaximum = leftMaximum;
    this.rightMinimum = rightMinimum;
    this.rightMaximum = rightMaximum;
    // get field and do checks
    Integer minimum = leftMinimum + rightMinimum;
    Integer maximum = leftMaximum + rightMaximum;
    for (MtasSpanSequenceItem item : items) {
      if (field == null) {
        field = item.getQuery().getField();
      } else if (item.getQuery().getField() != null
          && !item.getQuery().getField().equals(field)) {
        throw new IllegalArgumentException("Clauses must have same field.");
      }
      if (minimum != null && !item.isOptional()) {
        minimum = item.getQuery().getMinimumWidth() != null
            ? minimum + item.getQuery().getMinimumWidth() : null;
      }
      if (maximum != null) {
        maximum = item.getQuery().getMaximumWidth() != null
            ? maximum + item.getQuery().getMaximumWidth() : null;
      }
    }
    // check ignore
    if (field != null && ignoreQuery != null) {
      if (ignoreQuery.getField() == null
          || field.equals(ignoreQuery.getField())) {
        this.ignoreQuery = ignoreQuery;
        if (maximumIgnoreLength == null) {
          this.maximumIgnoreLength = MtasIgnoreItem.DEFAULT_MAXIMUM_IGNORE_LENGTH;
        } else {
          this.maximumIgnoreLength = maximumIgnoreLength;
        }
      } else {
        throw new IllegalArgumentException(
            "ignore must have same field as clauses");
      }
      if (maximum != null && items.size() > 1) {
        if (this.ignoreQuery.getMaximumWidth() != null) {
          maximum += (items.size() - 1) * this.maximumIgnoreLength
              * this.ignoreQuery.getMaximumWidth();
        } else {
          maximum = null;
        }
      }
    } else {
      this.ignoreQuery = null;
      this.maximumIgnoreLength = null;
    }
    setWidth(minimum, maximum);
  }

  @Override
  public String getField() {
    return field;
  }

  public List<MtasSpanSequenceItem> getItems() {
    return items;
  }

  public MtasSpanQuery getIgnoreQuery() {
    return ignoreQuery;
  }

  public Integer getMaximumIgnoreLength() {
    return maximumIgnoreLength;
  }

  @Override
  public MtasSpanQuery rewrite(IndexReader reader) throws IOException {
    if (items.size() == 1) {
      MtasSpanQuery singleQuery = items.get(0).getQuery();
      if (leftMaximum != 0 || rightMaximum != 0) {
        singleQuery = new MtasExpandSpanQuery(singleQuery, leftMinimum,
            leftMaximum, rightMinimum, rightMaximum);
      }
      return singleQuery.rewrite(reader);
    } else {
      MtasSpanSequenceItem newItem;
      MtasSpanSequenceItem previousNewItem = null;
      ArrayList<MtasSpanSequenceItem> newItems = new ArrayList<>(items.size());
      int newLeftMinimum = leftMinimum;
      int newLeftMaximum = leftMaximum;
      int newRightMinimum = rightMinimum;
      int newRightMaximum = rightMaximum;
      MtasSpanQuery newIgnoreClause = ignoreQuery != null
          ? ignoreQuery.rewrite(reader) : null;
      boolean actuallyRewritten = ignoreQuery != null && !newIgnoreClause.equals(ignoreQuery);
      for (int i = 0; i < items.size(); i++) {
        newItem = items.get(i).rewrite(reader);
        if (newItem.getQuery() instanceof MtasSpanMatchNoneQuery) {
          if (!newItem.isOptional()) {
            return new MtasSpanMatchNoneQuery(field);
          } else {
            actuallyRewritten = true;
          }
        } else {
          actuallyRewritten |= !items.get(i).equals(newItem);
          MtasSpanSequenceItem previousMergedItem = MtasSpanSequenceItem.merge(
              previousNewItem, newItem, ignoreQuery, maximumIgnoreLength);
          if (previousMergedItem != null) {
            newItems.set((newItems.size() - 1), previousMergedItem);
            actuallyRewritten = true;
          } else {
            newItems.add(newItem);
          }
          previousNewItem = newItem;
        }
      }
      // check first and last
      if (ignoreQuery == null) {
        ArrayList<MtasSpanSequenceItem> possibleTrimmedItems = new ArrayList<>(
            newItems.size());
        MtasSpanSequenceItem firstItem = newItems.get(0);
        MtasSpanQuery firstQuery = firstItem.getQuery();
        if (firstQuery instanceof MtasSpanMatchAllQuery) {
          newLeftMaximum++;
          if (!firstItem.isOptional()) {
            newLeftMinimum++;
          }
        } else if (firstQuery instanceof MtasSpanRecurrenceQuery) {
          MtasSpanRecurrenceQuery firstRecurrenceQuery = (MtasSpanRecurrenceQuery) firstQuery;
          if (firstRecurrenceQuery.getQuery() instanceof MtasSpanMatchAllQuery
              && firstRecurrenceQuery.getIgnoreQuery() == null) {
            if (!firstItem.isOptional()) {
              newLeftMinimum += firstRecurrenceQuery.getMinimumRecurrence();
              newLeftMaximum += firstRecurrenceQuery.getMaximumRecurrence();
            } else {
              if (firstRecurrenceQuery.getMinimumRecurrence() == 1
                  || firstRecurrenceQuery
                      .getMinimumRecurrence() <= newLeftMinimum) {
                newLeftMinimum += 0;
                newLeftMaximum += firstRecurrenceQuery.getMaximumRecurrence();
              } else {
                possibleTrimmedItems.add(firstItem);
              }
            }
          } else {
            possibleTrimmedItems.add(firstItem);
          }
        } else {
          possibleTrimmedItems.add(firstItem);
        }
        for (int i = 1; i < (newItems.size() - 1); i++) {
          possibleTrimmedItems.add(newItems.get(i));
        }
        if (newItems.size() > 1) {
          MtasSpanSequenceItem lastItem = newItems.get((newItems.size() - 1));
          MtasSpanQuery lastQuery = lastItem.getQuery();
          if (lastQuery instanceof MtasSpanMatchAllQuery) {
            newRightMaximum++;
            if (!lastItem.isOptional()) {
              newRightMinimum++;
            }
          } else if (lastQuery instanceof MtasSpanRecurrenceQuery) {
            MtasSpanRecurrenceQuery lastRecurrenceQuery = (MtasSpanRecurrenceQuery) lastQuery;
            if (lastRecurrenceQuery.getQuery() instanceof MtasSpanMatchAllQuery
                && lastRecurrenceQuery.getIgnoreQuery() == null) {
              if (!lastItem.isOptional()) {
                newRightMinimum += lastRecurrenceQuery.getMinimumRecurrence();
                newRightMaximum += lastRecurrenceQuery.getMaximumRecurrence();
              } else if (lastRecurrenceQuery.getMinimumRecurrence() == 1
                  || lastRecurrenceQuery
                      .getMinimumRecurrence() <= newRightMinimum) {
                newRightMinimum += 0;
                newRightMaximum += lastRecurrenceQuery.getMaximumRecurrence();
              } else {
                possibleTrimmedItems.add(lastItem);
              }
            } else {
              possibleTrimmedItems.add(lastItem);
            }
          } else {
            possibleTrimmedItems.add(lastItem);
          }
        }
        if (possibleTrimmedItems.size() < newItems.size()) {
          actuallyRewritten = true;
          newItems = possibleTrimmedItems;
        }
      }
      if (!actuallyRewritten) {
        if (leftMaximum != 0 || rightMaximum != 0) {
          newLeftMinimum = leftMinimum;
          newLeftMaximum = leftMaximum;
          newRightMinimum = rightMinimum;
          newRightMaximum = rightMaximum;
          leftMinimum = 0;
          leftMaximum = 0;
          rightMinimum = 0;
          rightMaximum = 0;
          MtasSpanQuery finalQuery = new MtasExpandSpanQuery(this,
              newLeftMinimum, newLeftMaximum, newRightMinimum, newRightMaximum);
          return finalQuery.rewrite(reader);
        } else {
          return super.rewrite(reader);
        }
      } else {
        if (!newItems.isEmpty()) {
          return new MtasSpanSequenceQuery(newItems, newLeftMinimum,
              newLeftMaximum, newRightMinimum, newRightMaximum, newIgnoreClause,
              maximumIgnoreLength).rewrite(reader);
        } else {
          return new MtasSpanMatchNoneQuery(field);
        }
      }
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName() + "([");
    Iterator<MtasSpanSequenceItem> i = items.iterator();
    while (i.hasNext()) {
      MtasSpanSequenceItem item = i.next();
      MtasSpanQuery clause = item.getQuery();
      buffer.append(clause.toString(field));
      if (item.isOptional()) {
        buffer.append("{OPTIONAL}");
      }
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("[" + leftMinimum + "," + leftMaximum + "]");
    buffer.append("[" + rightMinimum + "," + rightMaximum + "]");
    buffer.append("]");
    buffer.append(", ");
    buffer.append(ignoreQuery);
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    MtasSpanSequenceQuery other = (MtasSpanSequenceQuery) obj;
    return leftMinimum == other.leftMinimum &&
      leftMaximum == other.leftMaximum &&
      rightMinimum == other.rightMinimum &&
      rightMaximum == other.rightMaximum &&
      Objects.equals(items, other.items) &&
      Objects.equals(ignoreQuery, other.ignoreQuery) &&
      Objects.equals(maximumIgnoreLength, other.maximumIgnoreLength) &&
      Objects.equals(field, other.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      items, leftMinimum, leftMaximum, rightMinimum, rightMaximum, ignoreQuery, maximumIgnoreLength, field);
  }

  @Override
  public MtasSpanWeight createWeight(IndexSearcher searcher,
      boolean needsScores, float boost) throws IOException {
    List<MtasSpanSequenceQueryWeight> subWeights = new ArrayList<>();
    SpanWeight ignoreWeight = null;
    for (MtasSpanSequenceItem item : items) {
      subWeights.add(new MtasSpanSequenceQueryWeight(
          item.getQuery().createWeight(searcher, false, boost), item.isOptional()));
    }
    if (ignoreQuery != null) {
      ignoreWeight = ignoreQuery.createWeight(searcher, false, boost);
    }
    return new SpanSequenceWeight(subWeights, ignoreWeight, maximumIgnoreLength,
        searcher, needsScores ? getTermContexts(subWeights) : null, boost);
  }

  protected Map<Term, TermContext> getTermContexts(
      List<MtasSpanSequenceQueryWeight> items) {
    List<SpanWeight> weights = new ArrayList<>();
    for (MtasSpanSequenceQueryWeight item : items) {
      weights.add(item.spanWeight);
    }
    return getTermContexts(weights);
  }

  @Override
  public void disableTwoPhaseIterator() {
    super.disableTwoPhaseIterator();
    for (MtasSpanSequenceItem item : items) {
      item.getQuery().disableTwoPhaseIterator();
    }
    if (ignoreQuery != null) {
      ignoreQuery.disableTwoPhaseIterator();
    }
  }

  protected class SpanSequenceWeight extends MtasSpanWeight {
    final List<MtasSpanSequenceQueryWeight> subWeights;
    final SpanWeight ignoreWeight;
    final Integer maximumIgnoreLength;

    public SpanSequenceWeight(List<MtasSpanSequenceQueryWeight> subWeights,
        SpanWeight ignoreWeight, Integer maximumIgnoreLength,
        IndexSearcher searcher, Map<Term, TermContext> terms, float boost)
        throws IOException {
      super(MtasSpanSequenceQuery.this, searcher, terms, boost);
      this.subWeights = subWeights;
      this.ignoreWeight = ignoreWeight;
      this.maximumIgnoreLength = maximumIgnoreLength;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      for (MtasSpanSequenceQueryWeight w : subWeights) {
        w.spanWeight.extractTermContexts(contexts);
      }
      if (ignoreWeight != null) {
        ignoreWeight.extractTermContexts(contexts);
      }
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
        List<MtasSpanSequenceQuerySpans> setSequenceSpans = new ArrayList<>(
            items.size());
        Spans ignoreSpans = null;
        boolean allSpansEmpty = true;
        for (MtasSpanSequenceQueryWeight w : subWeights) {
          Spans sequenceSpans = w.spanWeight.getSpans(context,
              requiredPostings);
          if (sequenceSpans != null) {
            setSequenceSpans.add(new MtasSpanSequenceQuerySpans(
                MtasSpanSequenceQuery.this, sequenceSpans, w.optional));
            allSpansEmpty = false;
          } else {
            if (w.optional) {
              setSequenceSpans.add(new MtasSpanSequenceQuerySpans(
                  MtasSpanSequenceQuery.this, null, w.optional));
            } else {
              return null;
            }
          }
        }
        if (allSpansEmpty) {
          return null; // at least one required
        } else if (ignoreWeight != null) {
          ignoreSpans = ignoreWeight.getSpans(context, requiredPostings);
        }
        return new MtasSpanSequenceSpans(MtasSpanSequenceQuery.this,
            setSequenceSpans, ignoreSpans, maximumIgnoreLength);
      }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (MtasSpanSequenceQueryWeight w : subWeights) {
        w.spanWeight.extractTerms(terms);
      }
      if (ignoreWeight != null) {
        ignoreWeight.extractTerms(terms);
      }
    }
    
//    @Override
//    public boolean isCacheable(LeafReaderContext arg0) {      
//      for(MtasSpanSequenceQueryWeight sqw : subWeights) {
//        if(!sqw.spanWeight.isCacheable(arg0)) {
//          return false;
//        }
//      }
//      if(ignoreWeight!=null) {
//        return ignoreWeight.isCacheable(arg0);
//      }
//      return true;            
//    }

  }

  protected static class MtasSpanSequenceQuerySpans {
    public Spans spans;
    public boolean optional;

    public MtasSpanSequenceQuerySpans(MtasSpanSequenceQuery query, Spans spans,
        boolean optional) {
      this.spans = spans != null ? spans : new MtasSpanMatchNoneSpans(query);
      this.optional = optional;
    }
  }

  private static class MtasSpanSequenceQueryWeight {
    public SpanWeight spanWeight;
    public boolean optional;

    public MtasSpanSequenceQueryWeight(SpanWeight spanWeight,
        boolean optional) {
      this.spanWeight = spanWeight;
      this.optional = optional;
    }
  }
  
  @Override
  public boolean isMatchAllPositionsQuery() {
    return false;
  }
}
