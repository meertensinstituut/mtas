package mtas.solr.handler.component.util;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;

import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentVersion;
import mtas.solr.handler.component.MtasSolrSearchComponent;

// TODO: Auto-generated Javadoc
/**
 * The Class MtasSolrComponentStatus.
 */
public class MtasSolrComponentVersion implements MtasSolrComponent<ComponentVersion> {

	/** The Constant log. */
	private static final Log log = LogFactory.getLog(MtasSolrComponentVersion.class);

	/** The search component. */
	MtasSolrSearchComponent searchComponent;

	String propertyVersion = null;

	String propertyArtifactId = null;

	String propertyGroupId = null;

	String propertyTimestamp = null;

	/** The Constant NAME. */
	public static final String NAME = "version";

	/** The Constant PARAM_MTAS_VERSION. */
	public static final String PARAM_MTAS_VERSION = MtasSolrSearchComponent.PARAM_MTAS + "." + NAME;

	public static final String NAME_MTAS_VERSION_VERSION = "version";

	public static final String NAME_MTAS_VERSION_GROUPID = "groupId";

	public static final String NAME_MTAS_VERSION_ARTIFACTID = "artifactId";

	public static final String NAME_MTAS_VERSION_TIMESTAMP = "timestamp";

	public static final String NAME_MTAS_VERSION_SHARDS = "shards";

	public static final String NAME_MTAS_VERSION_SHARD = "shard";

	/**
	 * Instantiates a new mtas solr component status.
	 *
	 * @param searchComponent
	 *            the search component
	 */
	public MtasSolrComponentVersion(MtasSolrSearchComponent searchComponent) {
		this.searchComponent = searchComponent;
		// get classPath
		Class<MtasSolrComponentVersion> clazz = MtasSolrComponentVersion.class;
		String className = clazz.getSimpleName() + ".class";
		String classPath = clazz.getResource(className).toString();
		if (classPath.startsWith("jar")) {
			String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
			try {
				Manifest manifest = new Manifest(new URL(manifestPath).openStream());
				Attributes attr = manifest.getMainAttributes();
				propertyVersion = attr.getValue("Specification-version");
				propertyArtifactId = attr.getValue("Specification-artifactId");
				propertyGroupId = attr.getValue("Specification-groupId");
				propertyTimestamp = attr.getValue("Specification-timestamp");
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
	public void prepare(ResponseBuilder rb, ComponentFields mtasFields) throws IOException {
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
	public SimpleOrderedMap<Object> create(ComponentVersion version, Boolean encode) throws IOException {
		return getVersion();
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
	public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
		// do nothing
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
		if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
			if (rb.stage >= ResponseBuilder.STAGE_EXECUTE_QUERY && rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
				for (ShardRequest sreq : rb.finished) {
					if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
							&& sreq.params.getBool(PARAM_MTAS_VERSION, false)) {
						for (ShardResponse shardResponse : sreq.responses) {
							NamedList<Object> solrShardResponse = shardResponse.getSolrResponse().getResponse();
							try {
								SimpleOrderedMap<Object> data = (SimpleOrderedMap<Object>) solrShardResponse
										.findRecursive(MtasSolrSearchComponent.NAME, NAME);
								// create shardInfo
								SimpleOrderedMap<Object> dataShard = new SimpleOrderedMap<>();
								dataShard.add(NAME_MTAS_VERSION_SHARD, shardResponse.getShard());
								dataShard.add(NAME, data.clone());
								List<SimpleOrderedMap> dataShards = new ArrayList<>();
								dataShards.add(dataShard);
								// create main data
								data.clear();
								data.addAll(getVersion());
								// add shardInfo
								data.add(NAME_MTAS_VERSION_SHARDS, dataShards);
							} catch (ClassCastException | IOException e) {
								log.debug(e);
								// shouldn't happen
							}

						}
					}
				}

			}
		}

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
	public void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields) throws IOException {
		// TODO Auto-generated method stub

	}

	private SimpleOrderedMap<Object> getVersion() throws IOException {
		SimpleOrderedMap<Object> mtasVersionResponse = new SimpleOrderedMap<>();
		mtasVersionResponse.add(NAME_MTAS_VERSION_GROUPID, propertyGroupId);
		mtasVersionResponse.add(NAME_MTAS_VERSION_ARTIFACTID, propertyArtifactId);
		mtasVersionResponse.add(NAME_MTAS_VERSION_VERSION, propertyVersion);
		mtasVersionResponse.add(NAME_MTAS_VERSION_TIMESTAMP, propertyTimestamp);
		return mtasVersionResponse;
	}

}
