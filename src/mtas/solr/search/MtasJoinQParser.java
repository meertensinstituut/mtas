package mtas.solr.search;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

/**
 * The Class MtasJoinQParser.
 */
public class MtasJoinQParser extends QParser {

  /** The Constant MTAS_JOIN_QPARSER_URL. */
  public static final String MTAS_JOIN_QPARSER_URL = "url";

  /** The Constant MTAS_JOIN_QPARSER_REQUEST. */
  public static final String MTAS_JOIN_QPARSER_REQUEST = "request";

  /** The url. */
  String url = null;

  /** The request. */
  String request = null;

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

    // SearchComponent sc = req.getCore().getSearchComponent("mtas");
    // if ((sc != null) && (sc instanceof MtasSolrSearchComponent)) {
    // msc = (MtasSolrSearchComponent) sc;
    // }
    if ((localParams.getParams(MTAS_JOIN_QPARSER_URL) != null)
        && (localParams.getParams(MTAS_JOIN_QPARSER_URL).length == 1)) {
      url = localParams.getParams(MTAS_JOIN_QPARSER_URL)[0];
    }
    if ((localParams.getParams(MTAS_JOIN_QPARSER_REQUEST) != null)
        && (localParams.getParams(MTAS_JOIN_QPARSER_REQUEST).length == 1)) {
      request = localParams.getParams(MTAS_JOIN_QPARSER_REQUEST)[0];
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.search.QParser#parse()
   */
  @Override
  public Query parse() throws SyntaxError {
    if (url == null) {
      throw new SyntaxError("no " + MTAS_JOIN_QPARSER_URL);
    } else if (request == null) {
      throw new SyntaxError("no " + MTAS_JOIN_QPARSER_REQUEST);
    } else {
      return null;
    }
  }

}
