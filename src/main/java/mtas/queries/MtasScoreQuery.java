package mtas.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.search.Query;

public class MtasScoreQuery extends CustomScoreQuery {
  public MtasScoreQuery(Query subQuery) {
    super(subQuery);
  }

  public MtasScoreQuery(Query subQuery, FunctionQuery scoringQuery) {
    super(subQuery, scoringQuery);
  }

  public MtasScoreQuery(Query subQuery, FunctionQuery... scoringQueries) {
    super(subQuery, scoringQueries);
  }

  @Override
  public CustomScoreProvider getCustomScoreProvider(
      final LeafReaderContext context) {
    return new MtasScoreProvider(context);
  }
}
