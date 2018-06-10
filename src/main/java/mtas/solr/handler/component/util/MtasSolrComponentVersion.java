package mtas.solr.handler.component.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;

import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentVersion;
import mtas.solr.handler.component.MtasSolrSearchComponent;

// TODO: Auto-generated Javadoc
/**
 * The Class MtasSolrComponentStatus.
 */
public class MtasSolrComponentVersion
    implements MtasSolrComponent<ComponentVersion> {

  /** The Constant log. */
  private static final Log log = LogFactory
      .getLog(MtasSolrComponentVersion.class);

  /** The search component. */
  MtasSolrSearchComponent searchComponent;
  
  String propertyVersion = null;
  
  String propertyArtifactId = null;
  
  String propertyGroupId = null;
  
  String propertyTimestamp = null;

  /** The Constant NAME. */
  public static final String NAME = "version";

  /** The Constant PARAM_MTAS_VERSION. */
  public static final String PARAM_MTAS_VERSION = MtasSolrSearchComponent.PARAM_MTAS
      + "." + NAME;
  
  public static final String NAME_MTAS_VERSION_VERSION = "version";
  
  public static final String NAME_MTAS_VERSION_GROUPID = "groupId";
  
  public static final String NAME_MTAS_VERSION_ARTIFACTID = "artifactId";
  
  public static final String NAME_MTAS_VERSION_TIMESTAMP = "timestamp";

  /**
   * Instantiates a new mtas solr component status.
   *
   * @param searchComponent
   *          the search component
   */
  public MtasSolrComponentVersion(MtasSolrSearchComponent searchComponent) {
    this.searchComponent = searchComponent;
    String resourceName = "project.properties"; // could also be a constant
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties props = new Properties();
    try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
        props.load(resourceStream);
        propertyVersion = props.getProperty(NAME_MTAS_VERSION_VERSION);
        propertyArtifactId = props.getProperty(NAME_MTAS_VERSION_ARTIFACTID);
        propertyGroupId = props.getProperty(NAME_MTAS_VERSION_GROUPID);
        propertyTimestamp = props.getProperty(NAME_MTAS_VERSION_TIMESTAMP);
    } catch (IOException e) {
		log.info("couldn't read project.properties: "+e.getMessage());
	}
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
	  mtasFields.doVersion = true;
	  mtasFields.version = new ComponentVersion();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#create(mtas.codec.util.
   * CodecComponent.BasicComponent, java.lang.Boolean)
   */
  @Override
  public SimpleOrderedMap<Object> create(ComponentVersion version, Boolean encode)
      throws IOException {
    SimpleOrderedMap<Object> mtasVersionResponse = new SimpleOrderedMap<>();
    mtasVersionResponse.add(NAME_MTAS_VERSION_GROUPID, propertyGroupId);
    mtasVersionResponse.add(NAME_MTAS_VERSION_ARTIFACTID, propertyArtifactId);
    mtasVersionResponse.add(NAME_MTAS_VERSION_VERSION, propertyVersion);
    mtasVersionResponse.add(NAME_MTAS_VERSION_TIMESTAMP, propertyTimestamp);
    return mtasVersionResponse;
     
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
	  sreq.params.remove(PARAM_MTAS_VERSION);
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
