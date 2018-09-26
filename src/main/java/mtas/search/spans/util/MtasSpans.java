package mtas.search.spans.util;

import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.Spans;

abstract public class MtasSpans extends Spans {
  @Override
  public abstract TwoPhaseIterator asTwoPhaseIterator();
}
