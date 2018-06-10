package mtas.solr.handler.component.util;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;

import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentStatus;
import mtas.solr.handler.MtasRequestHandler.ShardInformation;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentStatus.
 */
public class MtasSolrComponentStatus
    implements MtasSolrComponent<ComponentStatus> {

  /** The Constant log. */
  private static final Log log = LogFactory
      .getLog(MtasSolrComponentStatus.class);

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant NAME. */
  public static final String NAME = "status";

  /** The Constant PARAM_MTAS_STATUS. */
  public static final String PARAM_MTAS_STATUS = MtasSolrSearchComponent.PARAM_MTAS
      + "." + NAME;

  /** The Constant NAME_MTAS_STATUS_KEY. */
  public static final String NAME_MTAS_STATUS_KEY = "key";

  public static final String NAME_MTAS_STATUS_MTASHANDLER = "mtasHandler";

  public static final String NAME_MTAS_STATUS_NUMBEROFSEGMENTS = "numberOfSegments";
  public static final String NAME_MTAS_STATUS_NUMBEROFDOCUMENTS = "numberOfDocuments";

  /**
   * Instantiates a new mtas solr component status.
   *
   * @param searchComponent
   *          the search component
   */
  public MtasSolrComponentStatus(MtasSolrSearchComponent searchComponent) {
    this.searchComponent = searchComponent;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#prepare(org.apache.solr.
   * handler.component.ResponseBuilder,
   * mtas.codec.util.CodecComponent.ComponentFields)
   */
  @Override
  public void prepare(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    mtasFields.doStatus = true;
    String key = rb.req.getParams()
        .get(PARAM_MTAS_STATUS + "." + NAME_MTAS_STATUS_KEY, null);
    boolean getHandler = rb.req.getParams()
        .getBool(MtasSolrComponentStatus.PARAM_MTAS_STATUS + "."
            + NAME_MTAS_STATUS_MTASHANDLER, false);
    boolean getNumberOfDocuments = rb.req.getParams()
        .getBool(MtasSolrComponentStatus.PARAM_MTAS_STATUS + "."
            + NAME_MTAS_STATUS_NUMBEROFDOCUMENTS, false);
    boolean getNumberOfSegments = rb.req.getParams()
        .getBool(MtasSolrComponentStatus.PARAM_MTAS_STATUS + "."
            + NAME_MTAS_STATUS_NUMBEROFSEGMENTS, false);
    mtasFields.status = new ComponentStatus(rb.req.getCore().getName(), key,
        getHandler, getNumberOfDocuments, getNumberOfSegments);
    mtasFields.status.numberOfDocuments = rb.req.getSearcher().getRawReader()
        .numDocs();
    mtasFields.status.numberOfSegments = rb.req.getSearcher().getRawReader()
        .leaves().size();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#create(mtas.codec.util.
   * CodecComponent.BasicComponent, java.lang.Boolean)
   */
  @Override
  public SimpleOrderedMap<Object> create(ComponentStatus status, Boolean encode)
      throws IOException {
    SimpleOrderedMap<Object> mtasStatusResponse = new SimpleOrderedMap<>();
    if (status.getMtasHandler) {
      mtasStatusResponse.add(NAME_MTAS_STATUS_MTASHANDLER, status.handler);
    }
    if (status.getNumberOfDocuments) {
      mtasStatusResponse.add(NAME_MTAS_STATUS_NUMBEROFDOCUMENTS,
          status.numberOfDocuments);
    }
    if (status.getNumberOfSegments) {
      mtasStatusResponse.add(NAME_MTAS_STATUS_NUMBEROFSEGMENTS,
          status.numberOfSegments);
    }
    if(mtasStatusResponse.size()>0) {
      mtasStatusResponse.add(ShardInformation.NAME_NAME, status.name);
      if (status.key != null) {
        mtasStatusResponse.add(NAME_MTAS_STATUS_KEY, status.key); 
      }
      return mtasStatusResponse;
    } else {
      return null;
    }  
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#modifyRequest(org.apache
   * .solr.handler.component.ResponseBuilder,
   * org.apache.solr.handler.component.SearchComponent,
   * org.apache.solr.handler.component.ShardRequest)
   */
  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq) {
    sreq.params.remove(PARAM_MTAS_STATUS + "." + NAME_MTAS_STATUS_KEY);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#finishStage(org.apache.
   * solr.handler.component.ResponseBuilder)
   */
  @Override
  public void finishStage(ResponseBuilder rb) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#distributedProcess(org.
   * apache.solr.handler.component.ResponseBuilder,
   * mtas.codec.util.CodecComponent.ComponentFields)
   */
  @Override
  public void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    // TODO Auto-generated method stub

  }

}
