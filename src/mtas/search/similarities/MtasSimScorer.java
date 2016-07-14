package mtas.search.similarities;

import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;

/**
 * The Class MtasSimScorer.
 */
public class MtasSimScorer extends SimScorer {

  /* (non-Javadoc)
   * @see org.apache.lucene.search.similarities.Similarity.SimScorer#score(int, float)
   */
  @Override
  public float score(int doc, float freq) {
    return 0;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.similarities.Similarity.SimScorer#computeSlopFactor(int)
   */
  @Override
  public float computeSlopFactor(int distance) {
    return 0;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.similarities.Similarity.SimScorer#computePayloadFactor(int, int, int, org.apache.lucene.util.BytesRef)
   */
  @Override
  public float computePayloadFactor(int doc, int start, int end,
      BytesRef payload) {   
    return 0;
  }

}
