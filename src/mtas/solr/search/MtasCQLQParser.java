package mtas.solr.search;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import mtas.parser.cql.MtasCQLParser;
import mtas.search.spans.util.MtasSpanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

/**
 * The Class MtasCQLQParser.
 */
public class MtasCQLQParser extends QParser {

  /** The mtas cql qparser field. */
  public static final String MTAS_CQL_QPARSER_FIELD = "field";

  /** The mtas cql qparser query. */
  public static final String MTAS_CQL_QPARSER_QUERY = "query";

  /** The mtas cql qparser query. */
  public static final String MTAS_CQL_QPARSER_IGNORE = "ignore";

  /** The mtas cql qparser default prefix. */
  public static final String MTAS_CQL_QPARSER_PREFIX = "prefix";

  /** The field. */
  String field = null;

  /** The query. */
  String cql = null;
  
  String ignoreQuery = null;
  
  Integer maximumIgnoreLength = null;

  /** The default prefix. */
  String defaultPrefix = null;

  /** The variables. */
  HashMap<String, String[]> variables = null;

  /**
   * Instantiates a new mtas cqlq parser.
   *
   * @param qstr the qstr
   * @param localParams the local params
   * @param params the params
   * @param req the req
   */
  public MtasCQLQParser(String qstr, SolrParams localParams, SolrParams params,
      SolrQueryRequest req) {
    super(qstr, localParams, params, req);

    if ((localParams.getParams(MTAS_CQL_QPARSER_FIELD) != null)
        && (localParams.getParams(MTAS_CQL_QPARSER_FIELD).length == 1)) {
      field = localParams.getParams(MTAS_CQL_QPARSER_FIELD)[0];
    }
    if ((localParams.getParams(MTAS_CQL_QPARSER_QUERY) != null)
        && (localParams.getParams(MTAS_CQL_QPARSER_QUERY).length == 1)) {
      cql = localParams.getParams(MTAS_CQL_QPARSER_QUERY)[0];
    }
    if ((localParams.getParams(MTAS_CQL_QPARSER_IGNORE) != null)
        && (localParams.getParams(MTAS_CQL_QPARSER_IGNORE).length == 1)) {
      ignoreQuery = localParams.getParams(MTAS_CQL_QPARSER_IGNORE)[0];
    }
    if ((localParams.getParams(MTAS_CQL_QPARSER_PREFIX) != null)
        && (localParams
            .getParams(MTAS_CQL_QPARSER_PREFIX).length == 1)) {
      defaultPrefix = localParams.getParams(MTAS_CQL_QPARSER_PREFIX)[0];
    }
    variables = new HashMap<>();
    Iterator<String> it = localParams.getParameterNamesIterator();
    while (it.hasNext()) {
      String item = it.next();      
      if (item.startsWith("variable_")) {       
        if(localParams.getParams(item).length==0 || (localParams.getParams(item).length==1 && localParams.getParams(item)[0].isEmpty())) {
          variables.put(item.substring(9),new String[0]);
        } else {
          ArrayList<String> list = new ArrayList<>();
          for(int i=0; i<localParams.getParams(item).length; i++) {
            String[] subList = localParams.getParams(item)[i].split("(?<!\\\\),");
            for(int j=0; j<subList.length; j++) {
              list.add(subList[j].replace("\\,", ",").replace("\\\\", "\\"));
            }
          }
          variables.put(item.substring(9), list.toArray(new String[list.size()]));
        }
      }
    }    
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.search.QParser#parse()
   */
  @Override
  public Query parse() throws SyntaxError {
    if (field == null) {
      throw new SyntaxError("no " + MTAS_CQL_QPARSER_FIELD);
    } else if (cql == null) {
      throw new SyntaxError("no " + MTAS_CQL_QPARSER_QUERY);
    } else {
      MtasSpanQuery q = null;
      MtasSpanQuery iq =null;
      if(ignoreQuery!=null) {
        Reader ignoreReader = new BufferedReader(new StringReader(ignoreQuery));
        MtasCQLParser ignoreParser = new MtasCQLParser(ignoreReader);
        try {
          iq = ignoreParser.parse(field, null, null, null, null);
        } catch (mtas.parser.cql.TokenMgrError | mtas.parser.cql.ParseException e) {
          throw new SyntaxError(e.getMessage());
        }
      }
      Reader queryReader = new BufferedReader(new StringReader(cql));
      MtasCQLParser queryParser = new MtasCQLParser(queryReader);
      try {
        q = queryParser.parse(field, defaultPrefix, variables, iq, maximumIgnoreLength);
      } catch (mtas.parser.cql.TokenMgrError | mtas.parser.cql.ParseException e) {
        throw new SyntaxError(e.getMessage());
      }
      return q;
    }
  }

}
