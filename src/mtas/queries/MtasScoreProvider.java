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
    System.out.println("* Init MTAS scorerprovider doc " + doc + " - "
        + subQueryScore + " - " + valSrcScore);
    return (float) 3.0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.queries.CustomScoreProvider#customScore(int, float,
   * float[])
   */
  @Override
  public float customScore(int doc, float subQueryScore, float valSrcScores[])
      throws IOException {
    System.out.print("** Init MTAS scorerprovider doc " + doc + " - "
        + subQueryScore + " - ");
    System.out.print(valSrcScores.length + ":");
    for (int i = 0; i < valSrcScores.length; i++) {
      System.out.print(valSrcScores[i] + ",");
    }
    System.out.println(" voor veld ");
    return (float) 4.0;
  }

}
