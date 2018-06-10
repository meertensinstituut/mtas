package mtas.solr.search;

import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.PluginBag.PluginHolder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasJoinQParser.
 */
public class MtasJoinQParser extends QParser {

  /** The Constant MTAS_JOIN_QPARSER_COLLECTION. */
  public static final String MTAS_JOIN_QPARSER_COLLECTION = "collection";

  /** The Constant MTAS_JOIN_QPARSER_FIELD. */
  public static final String MTAS_JOIN_QPARSER_FIELD = "field";

  /** The id. */
  private String id = null;

  /** The fields. */
  private String[] fields = null;

  /**
   * Instantiates a new mtas join Q parser.
   *
   * @param qstr the qstr
   * @param localParams the local params
   * @param params the params
   * @param req the req
   */
  public MtasJoinQParser(String qstr, SolrParams localParams, SolrParams params,
      SolrQueryRequest req) {
    super(qstr, localParams, params, req);

    if ((localParams.getParams(MTAS_JOIN_QPARSER_COLLECTION) != null)
        && (localParams.getParams(MTAS_JOIN_QPARSER_COLLECTION).length == 1)) {
      id = localParams.getParams(MTAS_JOIN_QPARSER_COLLECTION)[0];
    }
    if ((localParams.getParams(MTAS_JOIN_QPARSER_FIELD) != null)
        && (localParams.getParams(MTAS_JOIN_QPARSER_FIELD).length > 0)) {
      fields = new String[localParams
          .getParams(MTAS_JOIN_QPARSER_FIELD).length];
      System.arraycopy(localParams.getParams(MTAS_JOIN_QPARSER_FIELD), 0,
          fields, 0, localParams.getParams(MTAS_JOIN_QPARSER_FIELD).length);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.search.QParser#parse()
   */
  @Override
  public Query parse() throws SyntaxError {
    if (id == null) {
      throw new SyntaxError("no " + MTAS_JOIN_QPARSER_COLLECTION);
    } else if (fields == null) {
      throw new SyntaxError("no " + MTAS_JOIN_QPARSER_FIELD);
    } else {

      BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

      MtasSolrCollectionCache mtasSolrJoinCache = null;
      for (PluginHolder<SearchComponent> item : req.getCore()
          .getSearchComponents().getRegistry().values()) {
        if (item.get() instanceof MtasSolrSearchComponent) {
          mtasSolrJoinCache = ((MtasSolrSearchComponent) item.get())
              .getCollectionCache();
        }
      }
      if (mtasSolrJoinCache != null) {
        Automaton automaton;
        try {
          automaton = mtasSolrJoinCache.getAutomatonById(id);
          if (automaton != null) {
            for (String field : fields) {
              booleanQueryBuilder.add(
                  new AutomatonQuery(new Term(field), automaton), Occur.SHOULD);
            }
          } else {
            throw new IOException("no data for collection '" + id + "'");
          }
        } catch (IOException e) {
          throw new SyntaxError(
              "could not construct automaton: " + e.getMessage(), e);
        }
        return booleanQueryBuilder.build();
      } else {
        throw new SyntaxError("no MtasSolrSearchComponent found");
      }
    }
  }

}
