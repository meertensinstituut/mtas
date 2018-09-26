package mtas.solr.search;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

public class MtasSolrCQLQParserPlugin extends QParserPlugin {
  @SuppressWarnings("rawtypes")
  @Override
  public void init(NamedList args) {
    // do nothing for now
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    return new MtasCQLQParser(qstr, localParams, params, req);
  }
}
