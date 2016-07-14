package mtas.solr.search;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;
import mtas.parser.cql.MtasCQLParser;
import mtas.solr.handler.component.MtasSolrSearchComponent;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

/**
 * The Class MtasCQLQParser.
 */
public class MtasCQLQParser extends QParser {

  /** The field. */
  String field = null;
  
  /** The cql. */
  String cql = null;
  
  /** The msc. */
  MtasSolrSearchComponent msc = null;
  
  /**
   * Instantiates a new mtas cqlq parser.
   *
   * @param qstr the qstr
   * @param localParams the local params
   * @param params the params
   * @param req the req
   */
  public MtasCQLQParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
    
    SearchComponent sc = req.getCore().getSearchComponent("mtas");
    if((sc!=null) && (sc instanceof MtasSolrSearchComponent)) {
      msc = (MtasSolrSearchComponent) sc;      
    }    
    if((localParams.getParams("field")!=null) && (localParams.getParams("field").length==1)) {
      field = localParams.getParams("field")[0]; 
    } 
    if((localParams.getParams("cql")!=null) && (localParams.getParams("cql").length==1)) {
      cql = localParams.getParams("cql")[0]; 
    }     
   
  }

  /* (non-Javadoc)
   * @see org.apache.solr.search.QParser#parse()
   */
  @Override
  public Query parse() throws SyntaxError {
    if(field==null) {
      throw new SyntaxError("no field");
    } else if(cql==null) { 
      throw new SyntaxError("no cql");
    } else {        
      Reader reader = new BufferedReader(new StringReader(cql));
      MtasCQLParser p = new MtasCQLParser(reader);

      SpanQuery q = null;
      
      try {
        q = p.parse(field);                   
      } catch (mtas.parser.cql.ParseException e) {
        throw new SyntaxError(e.getMessage());
      }  
//      Map<String,Object> j = req.getJSON();   
//      if(j==null) {
//        j = new HashMap<String,Object>();
//      }
//      Set<SpanQuery> l;
//      if(j.containsKey("mtas.stats")) {
//        l = (HashSet<SpanQuery>) j.get("mtas.stats");
//      } else {
//        l = new HashSet<SpanQuery>();
//      }
//      l.add(q);
//      j.put("mtas.stats", l);
      //req.setJSON(j);
      return q;
    }  
  }

}
