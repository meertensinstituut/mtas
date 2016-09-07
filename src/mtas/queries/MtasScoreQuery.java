package mtas.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.search.Query;

/**
 * The Class MtasScoreQuery.
 */
public class MtasScoreQuery extends CustomScoreQuery {

  /**
   * Instantiates a new mtas score query.
   *
   * @param subQuery
   *          the sub query
   */
  public MtasScoreQuery(Query subQuery) {
    super(subQuery);
    System.out.println("* Init MTAS scorer " + subQuery.toString());
  }

  /**
   * Instantiates a new mtas score query.
   *
   * @param subQuery
   *          the sub query
   * @param scoringQuery
   *          the scoring query
   */
  public MtasScoreQuery(Query subQuery, FunctionQuery scoringQuery) {
    super(subQuery, scoringQuery);
    System.out.println("** Init MTAS scorer " + subQuery.toString());
  }

  /**
   * Instantiates a new mtas score query.
   *
   * @param subQuery
   *          the sub query
   * @param scoringQueries
   *          the scoring queries
   */
  public MtasScoreQuery(Query subQuery, FunctionQuery... scoringQueries) {
    super(subQuery, scoringQueries);
    System.out.println("*** Init MTAS scorer " + subQuery.toString());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.queries.CustomScoreQuery#getCustomScoreProvider(org.
   * apache.lucene.index.LeafReaderContext)
   */
  @Override
  public CustomScoreProvider getCustomScoreProvider(
      final LeafReaderContext context) {
    return new MtasScoreProvider(context);
  }

}
