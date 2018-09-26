package mtas.search.similarities;

import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;

public class MtasSimScorer extends SimScorer {
  @Override
  public float score(int doc, float freq) {
    return 0;
  }

  @Override
  public float computeSlopFactor(int distance) {
    return 0;
  }

  @Override
  public float computePayloadFactor(int doc, int start, int end,
      BytesRef payload) {
    return 0;
  }
}
