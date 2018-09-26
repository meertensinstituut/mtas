package mtas.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;

import java.io.IOException;

public class MtasScoreProvider extends CustomScoreProvider {
  public MtasScoreProvider(LeafReaderContext context) {
    super(context);
  }

  @Override
  public float customScore(int doc, float subQueryScore, float valSrcScore) {
    return (float) 0.0;
  }

  @Override
  public float customScore(int doc, float subQueryScore, float[] valSrcScores)
      throws IOException {
    return (float) 0.0;
  }
}
