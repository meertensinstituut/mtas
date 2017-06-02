package mtas.queries;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;

/**
 * The Class MtasScoreProvider.
 */
public class MtasScoreProvider extends CustomScoreProvider {

  /**
   * Instantiates a new mtas score provider.
   *
   * @param context the context
   */
  public MtasScoreProvider(LeafReaderContext context) {
    super(context);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.queries.CustomScoreProvider#customScore(int, float,
   * float)
   */
  @Override
  public float customScore(int doc, float subQueryScore, float valSrcScore) {
    return (float) 0.0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.queries.CustomScoreProvider#customScore(int, float,
   * float[])
   */
  @Override
  public float customScore(int doc, float subQueryScore, float[] valSrcScores)
      throws IOException {
    return (float) 0.0;
  }

}
