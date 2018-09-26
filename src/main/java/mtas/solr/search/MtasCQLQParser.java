package mtas.solr.search;

import mtas.parser.cql.MtasCQLParser;
import mtas.search.spans.util.MtasSpanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class MtasCQLQParser extends QParser {
  public static final String MTAS_CQL_QPARSER_FIELD = "field";
  public static final String MTAS_CQL_QPARSER_QUERY = "query";
  public static final String MTAS_CQL_QPARSER_IGNORE = "ignore";
  public static final String MTAS_CQL_QPARSER_MAXIMUM_IGNORE_LENGTH = "maximumIgnoreLength";
  public static final String MTAS_CQL_QPARSER_PREFIX = "prefix";

  String field = null;
  String cql = null;
  String ignoreQuery = null;
  Integer maximumIgnoreLength = null;
  String defaultPrefix = null;
  HashMap<String, String[]> variables = null;

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
    if ((localParams.getParams(MTAS_CQL_QPARSER_MAXIMUM_IGNORE_LENGTH) != null)
        && (localParams
            .getParams(MTAS_CQL_QPARSER_MAXIMUM_IGNORE_LENGTH).length == 1)) {
      try {
        maximumIgnoreLength = Integer.parseInt(
            localParams.getParams(MTAS_CQL_QPARSER_MAXIMUM_IGNORE_LENGTH)[0]);
      } catch (NumberFormatException e) {
        maximumIgnoreLength = null;
      }
    }
    if ((localParams.getParams(MTAS_CQL_QPARSER_PREFIX) != null)
        && (localParams.getParams(MTAS_CQL_QPARSER_PREFIX).length == 1)) {
      defaultPrefix = localParams.getParams(MTAS_CQL_QPARSER_PREFIX)[0];
    }
    variables = new HashMap<>();
    Iterator<String> it = localParams.getParameterNamesIterator();
    while (it.hasNext()) {
      String item = it.next();
      if (item.startsWith("variable_")) {
        if (localParams.getParams(item).length == 0
            || (localParams.getParams(item).length == 1
                && localParams.getParams(item)[0].isEmpty())) {
          variables.put(item.substring(9), new String[0]);
        } else {
          ArrayList<String> list = new ArrayList<>();
          for (int i = 0; i < localParams.getParams(item).length; i++) {
            String[] subList = localParams.getParams(item)[i]
                .split("(?<!\\\\),");
            for (int j = 0; j < subList.length; j++) {
              list.add(subList[j].replace("\\,", ",").replace("\\\\", "\\"));
            }
          }
          variables.put(item.substring(9),
              list.toArray(new String[list.size()]));
        }
      }
    }    
  }

  @Override
  public Query parse() throws SyntaxError {
    if (field == null) {
      throw new SyntaxError("no " + MTAS_CQL_QPARSER_FIELD);
    } else if (cql == null) {
      throw new SyntaxError("no " + MTAS_CQL_QPARSER_QUERY);
    } else {
      MtasSpanQuery q = null;
      MtasSpanQuery iq = null;
      if (ignoreQuery != null) {
        Reader ignoreReader = new BufferedReader(new StringReader(ignoreQuery));
        MtasCQLParser ignoreParser = new MtasCQLParser(ignoreReader);
        try {
          iq = ignoreParser.parse(field, null, null, null, null);
        } catch (mtas.parser.cql.TokenMgrError
            | mtas.parser.cql.ParseException e) {
          throw new SyntaxError(e);
        }
      }
      Reader queryReader = new BufferedReader(new StringReader(cql));
      MtasCQLParser queryParser = new MtasCQLParser(queryReader);
      try {
        q = queryParser.parse(field, defaultPrefix, variables, iq,
            maximumIgnoreLength);
      } catch (mtas.parser.cql.TokenMgrError
          | mtas.parser.cql.ParseException e) {
        throw new SyntaxError(e);
      }
      return q;
    }
  }
}
