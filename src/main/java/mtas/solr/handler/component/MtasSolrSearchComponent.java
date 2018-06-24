package mtas.solr.handler.component;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

import mtas.codec.util.CodecComponent.ComponentDocument;
import mtas.codec.util.CodecComponent.ComponentFacet;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentGroup;
import mtas.codec.util.CodecComponent.ComponentCollection;
import mtas.codec.util.CodecComponent.ComponentKwic;
import mtas.codec.util.CodecComponent.ComponentList;
import mtas.codec.util.CodecComponent.ComponentPosition;
import mtas.codec.util.CodecComponent.ComponentSpan;
import mtas.codec.util.CodecComponent.ComponentTermVector;
import mtas.codec.util.CodecComponent.ComponentToken;
import mtas.codec.util.CodecUtil;
import mtas.codec.util.Status;
import mtas.solr.handler.component.util.MtasSolrResultMerge;
import mtas.solr.handler.util.MtasSolrStatus;
import mtas.solr.handler.util.MtasSolrStatus.ShardStatus;
import mtas.solr.search.MtasSolrCollectionCache;
import mtas.solr.handler.component.util.MtasSolrComponentDocument;
import mtas.solr.handler.component.util.MtasSolrComponentFacet;
import mtas.solr.handler.component.util.MtasSolrComponentGroup;
import mtas.solr.handler.MtasRequestHandler;
import mtas.solr.handler.MtasRequestHandler.ShardInformation;
import mtas.solr.handler.component.util.MtasSolrComponentCollection;
import mtas.solr.handler.component.util.MtasSolrComponentKwic;
import mtas.solr.handler.component.util.MtasSolrComponentList;
import mtas.solr.handler.component.util.MtasSolrComponentPrefix;
import mtas.solr.handler.component.util.MtasSolrComponentStats;
import mtas.solr.handler.component.util.MtasSolrComponentStatus;
import mtas.solr.handler.component.util.MtasSolrComponentTermvector;
import mtas.solr.handler.component.util.MtasSolrComponentVersion;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * The Class MtasSolrSearchComponent.
 */
public class MtasSolrSearchComponent extends SearchComponent {

	/** The log. */
	private static Log log = LogFactory.getLog(MtasSolrSearchComponent.class);

	/** The search component. */
	MtasSolrSearchComponent searchComponent;

	/** The Constant CONFIG_COLLECTION_CACHE_DIRECTORY. */
	public static final String CONFIG_COLLECTION_CACHE_DIRECTORY = "collectionCacheDirectory";

	/** The Constant CONFIG_COLLECTION_LIFETIME. */
	public static final String CONFIG_COLLECTION_LIFETIME = "collectionLifetime";

	/** The Constant CONFIG_COLLECTION_MAXIMUM_NUMBER. */
	public static final String CONFIG_COLLECTION_MAXIMUM_NUMBER = "collectionMaximumNumber";

	/** The Constant CONFIG_COLLECTION_MAXIMUM_OVERFLOW. */
	public static final String CONFIG_COLLECTION_MAXIMUM_OVERFLOW = "collectionMaximumOverflow";

	/** The Constant NAME. */
	public static final String NAME = "mtas";

	/** The Constant PARAM_MTAS. */
	public static final String PARAM_MTAS = "mtas";

	/** The Constant STAGE_TERMVECTOR_MISSING_TOP. */
	public static final int STAGE_TERMVECTOR_MISSING_TOP = ResponseBuilder.STAGE_EXECUTE_QUERY + 10;

	/** The Constant STAGE_TERMVECTOR_MISSING_KEY. */
	public static final int STAGE_TERMVECTOR_MISSING_KEY = ResponseBuilder.STAGE_EXECUTE_QUERY + 11;

	/** The Constant STAGE_TERMVECTOR_FINISH. */
	public static final int STAGE_TERMVECTOR_FINISH = ResponseBuilder.STAGE_EXECUTE_QUERY + 12;

	/** The Constant STAGE_LIST. */
	public static final int STAGE_LIST = ResponseBuilder.STAGE_EXECUTE_QUERY + 20;

	/** The Constant STAGE_PREFIX. */
	public static final int STAGE_PREFIX = ResponseBuilder.STAGE_EXECUTE_QUERY + 30;

	/** The Constant STAGE_STATS. */
	public static final int STAGE_STATS = ResponseBuilder.STAGE_EXECUTE_QUERY + 40;

	/** The Constant STAGE_FACET. */
	public static final int STAGE_FACET = ResponseBuilder.STAGE_EXECUTE_QUERY + 50;

	/** The Constant STAGE_GROUP. */
	public static final int STAGE_GROUP = ResponseBuilder.STAGE_EXECUTE_QUERY + 60;

	/** The Constant STAGE_COLLECTION_INIT. */
	public static final int STAGE_COLLECTION_INIT = ResponseBuilder.STAGE_EXECUTE_QUERY + 70;

	/** The Constant STAGE_COLLECTION_FINISH. */
	public static final int STAGE_COLLECTION_FINISH = ResponseBuilder.STAGE_EXECUTE_QUERY + 71;

	/** The Constant STAGE_DOCUMENT. */
	public static final int STAGE_DOCUMENT = ResponseBuilder.STAGE_GET_FIELDS + 10;

	/** The mtas solr result merge. */
	private MtasSolrResultMerge mtasSolrResultMerge;

	/** The search stats. */
	private MtasSolrComponentStats searchStats;

	/** The search termvector. */
	private MtasSolrComponentTermvector searchTermvector;

	/** The search prefix. */
	private MtasSolrComponentPrefix searchPrefix;

	/** The search facet. */
	private MtasSolrComponentFacet searchFacet;

	/** The search group. */
	private MtasSolrComponentGroup searchGroup;

	/** The search list. */
	private MtasSolrComponentList searchList;

	/** The search kwic. */
	private MtasSolrComponentKwic searchKwic;

	/** The search document. */
	private MtasSolrComponentDocument searchDocument;

	/** The search collection. */
	private MtasSolrComponentCollection searchCollection;

	/** The search status. */
	private MtasSolrComponentStatus searchStatus;

	private MtasSolrComponentVersion searchVersion;

	/** The collection cache. */
	private MtasSolrCollectionCache collectionCache = null;

	/** The request handler. */
	private MtasRequestHandler requestHandler = null;

	/** The request handler name. */
	private String requestHandlerName = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.solr.handler.component.SearchComponent#init(org.apache.solr.
	 * common.util.NamedList)
	 */
	@Override
	public void init(NamedList args) {
		super.init(args);
		// init components
		searchStatus = new MtasSolrComponentStatus(this);
		searchVersion = new MtasSolrComponentVersion(this);
		searchDocument = new MtasSolrComponentDocument(this);
		searchKwic = new MtasSolrComponentKwic(this);
		searchList = new MtasSolrComponentList(this);
		searchGroup = new MtasSolrComponentGroup(this);
		searchTermvector = new MtasSolrComponentTermvector(this);
		searchPrefix = new MtasSolrComponentPrefix(this);
		searchStats = new MtasSolrComponentStats(this);
		searchFacet = new MtasSolrComponentFacet(this);
		searchCollection = new MtasSolrComponentCollection(this);
		// init collection
		String collectionCacheDirectory = null;
		Long collectionLifetime = null;
		Integer collectionMaximumNumber = null;
		Integer collectionMaximumOverflow = null;
		if (args.get(CONFIG_COLLECTION_CACHE_DIRECTORY) != null
				&& args.get(CONFIG_COLLECTION_CACHE_DIRECTORY) instanceof String) {
			collectionCacheDirectory = (String) args.get(CONFIG_COLLECTION_CACHE_DIRECTORY);
		} else {
			log.error("no " + CONFIG_COLLECTION_CACHE_DIRECTORY + " defined for " + this.getClass().getSimpleName());
		}
		if (args.get(CONFIG_COLLECTION_LIFETIME) != null && args.get(CONFIG_COLLECTION_LIFETIME) instanceof Long) {
			collectionLifetime = (Long) args.get(CONFIG_COLLECTION_LIFETIME);
		} else {
			log.error("no " + CONFIG_COLLECTION_LIFETIME + " defined for " + this.getClass().getSimpleName());
		}
		if (args.get(CONFIG_COLLECTION_MAXIMUM_NUMBER) != null
				&& args.get(CONFIG_COLLECTION_MAXIMUM_NUMBER) instanceof Integer) {
			collectionMaximumNumber = (Integer) args.get(CONFIG_COLLECTION_MAXIMUM_NUMBER);
		} else {
			log.error("no " + CONFIG_COLLECTION_MAXIMUM_NUMBER + " defined for " + this.getClass().getSimpleName());
		}
		if (args.get(CONFIG_COLLECTION_MAXIMUM_OVERFLOW) != null
				&& args.get(CONFIG_COLLECTION_MAXIMUM_OVERFLOW) instanceof Integer) {
			collectionMaximumNumber = (Integer) args.get(CONFIG_COLLECTION_MAXIMUM_OVERFLOW);
		} else {
			log.error("no " + CONFIG_COLLECTION_MAXIMUM_OVERFLOW + " defined for " + this.getClass().getSimpleName());
		}
		collectionCache = new MtasSolrCollectionCache(collectionCacheDirectory, collectionLifetime,
				collectionMaximumNumber, collectionMaximumOverflow);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.solr.handler.component.SearchComponent#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Mtas";
	}

	/**
	 * Gets the collection cache.
	 *
	 * @return the collection cache
	 */
	public MtasSolrCollectionCache getCollectionCache() {
		return collectionCache;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.solr.handler.component.SearchComponent#prepare(org.apache.solr.
	 * handler.component.ResponseBuilder)
	 */
	@Override
	public void prepare(ResponseBuilder rb) throws IOException {
		// System.out
		// .println(System.nanoTime() + " - " + Thread.currentThread().getId()
		// + " - " + rb.req.getParams().getBool(ShardParams.IS_SHARD, false)
		// + " PREPARE " + rb.stage + " " + rb.req.getParamString());
		// always create status
		initializeRequestHandler(rb);
		MtasSolrStatus solrStatus = new MtasSolrStatus(rb.req.getOriginalParams().toQueryString(),
				rb.req.getParams().getBool(ShardParams.IS_SHARD, false), rb.shards, rb.rsp);
		rb.req.getContext().put(MtasSolrStatus.class, solrStatus);
		solrStatus.setStage(rb.stage);
		if (rb.req.getParams().getBool(PARAM_MTAS, false)) {
			try {
				// initialize
				mtasSolrResultMerge = new MtasSolrResultMerge();
				// prepare components
				ComponentFields mtasFields = new ComponentFields();
				// get settings version
				if (rb.req.getParams().getBool(MtasSolrComponentVersion.PARAM_MTAS_VERSION, false)) {
					searchVersion.prepare(rb, mtasFields);
				}
				// get settings status
				if (rb.req.getParams().getBool(MtasSolrComponentStatus.PARAM_MTAS_STATUS, false)) {
					searchStatus.prepare(rb, mtasFields);
					mtasFields.status.handler = requestHandlerName;
					if (mtasFields.status.key != null) {
						solrStatus.setKey(mtasFields.status.key);
					}
				}
				// now, register status
				registerStatus(solrStatus);
				// get settings document
				if (rb.req.getParams().getBool(MtasSolrComponentDocument.PARAM_MTAS_DOCUMENT, false)) {
					searchDocument.prepare(rb, mtasFields);
				}
				// get settings kwic
				if (rb.req.getParams().getBool(MtasSolrComponentKwic.PARAM_MTAS_KWIC, false)) {
					searchKwic.prepare(rb, mtasFields);
				}
				// get settings list
				if (rb.req.getParams().getBool(MtasSolrComponentList.PARAM_MTAS_LIST, false)) {
					searchList.prepare(rb, mtasFields);
				}
				// get settings group
				if (rb.req.getParams().getBool(MtasSolrComponentGroup.PARAM_MTAS_GROUP, false)) {
					searchGroup.prepare(rb, mtasFields);
				}
				// get settings termvector
				if (rb.req.getParams().getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
					searchTermvector.prepare(rb, mtasFields);
				}
				// get settings prefix
				if (rb.req.getParams().getBool(MtasSolrComponentPrefix.PARAM_MTAS_PREFIX, false)) {
					searchPrefix.prepare(rb, mtasFields);
				}
				// get settings stats
				if (rb.req.getParams().getBool(MtasSolrComponentStats.PARAM_MTAS_STATS, false)) {
					searchStats.prepare(rb, mtasFields);
				}
				// get settings facet
				if (rb.req.getParams().getBool(MtasSolrComponentFacet.PARAM_MTAS_FACET, false)) {
					searchFacet.prepare(rb, mtasFields);
				}
				// get settings collection
				if (rb.req.getParams().getBool(MtasSolrComponentCollection.PARAM_MTAS_COLLECTION, false)) {
					searchCollection.prepare(rb, mtasFields);
				}
				rb.req.getContext().put(ComponentFields.class, mtasFields);
			} catch (ExitableDirectoryReader.ExitingReaderException e) {
				solrStatus.setError(e.getMessage());
			} catch (IOException e) {
				solrStatus.setError(e);
			} finally {
				checkStatus(solrStatus);
			}
		} else {
			// register and check status
			registerStatus(solrStatus);
			checkStatus(solrStatus);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.solr.handler.component.SearchComponent#process(org.apache.solr.
	 * handler.component.ResponseBuilder)
	 */
	@Override
	public void process(ResponseBuilder rb) throws IOException {
		// System.out
		// .println(System.nanoTime() + " - " + Thread.currentThread().getId()
		// + " - " + rb.req.getParams().getBool(ShardParams.IS_SHARD, false)
		// + " PROCESS " + rb.stage + " " + rb.req.getParamString());
		MtasSolrStatus solrStatus = Objects
				.requireNonNull((MtasSolrStatus) rb.req.getContext().get(MtasSolrStatus.class), "couldn't find status");
		solrStatus.setStage(rb.stage);
		try {
			if (rb.req.getParams().getBool(PARAM_MTAS, false)) {
				try {
					ComponentFields mtasFields = getMtasFields(rb);
					if (mtasFields != null) {
						DocSet docSet = rb.getResults().docSet;
						DocList docList = rb.getResults().docList;
						if (mtasFields.doStats || mtasFields.doDocument || mtasFields.doKwic || mtasFields.doList
								|| mtasFields.doGroup || mtasFields.doFacet || mtasFields.doCollection
								|| mtasFields.doTermVector || mtasFields.doPrefix || mtasFields.doStatus
								|| mtasFields.doVersion) {
							SolrIndexSearcher searcher = rb.req.getSearcher();
							ArrayList<Integer> docSetList = null;
							ArrayList<Integer> docListList = null;
							// initialise docSetList
							if (docSet != null) {
								docSetList = new ArrayList<>();
								Iterator<Integer> docSetIterator = docSet.iterator();
								while (docSetIterator.hasNext()) {
									docSetList.add(docSetIterator.next());
								}
								Collections.sort(docSetList);
							}
							// initialise docListList
							if (docList != null) {
								docListList = new ArrayList<>();
								Iterator<Integer> docListIterator = docList.iterator();
								while (docListIterator.hasNext()) {
									docListList.add(docListIterator.next());
								}
								Collections.sort(docListList);
							}
							solrStatus.status().addSubs(mtasFields.list.keySet());
							for (String field : mtasFields.list.keySet()) {
								try {
									CodecUtil.collectField(field, searcher, searcher.getRawReader(), docListList,
											docSetList, mtasFields.list.get(field), solrStatus.status());
								} catch (IllegalAccessException | IllegalArgumentException
										| InvocationTargetException e) {
									log.error(e);
									throw new IOException(e);
								}
							}
							for (ComponentCollection collection : mtasFields.collection) {
								CodecUtil.collectCollection(searcher.getRawReader(), docSetList, collection);
							}
							NamedList<Object> mtasResponse = new SimpleOrderedMap<>();
							if (mtasFields.doVersion) {
								SimpleOrderedMap<Object> versionResponse = searchVersion.create(mtasFields.version,
										false);
								mtasResponse.add(MtasSolrComponentVersion.NAME, versionResponse);
							}
							if (mtasFields.doStatus) {
								// add to response
								SimpleOrderedMap<Object> statusResponse = searchStatus.create(mtasFields.status, false);
								if (statusResponse != null) {
									mtasResponse.add(MtasSolrComponentStatus.NAME,
											searchStatus.create(mtasFields.status, false));
								}
							}
							if (mtasFields.doDocument) {
								ArrayList<NamedList<?>> mtasDocumentResponses = new ArrayList<>();
								for (String field : mtasFields.list.keySet()) {
									for (ComponentDocument document : mtasFields.list.get(field).documentList) {
										mtasDocumentResponses.add(searchDocument.create(document, false));
									}
								}
								// add to response
								mtasResponse.add(MtasSolrComponentDocument.NAME, mtasDocumentResponses);
							}
							if (mtasFields.doKwic) {
								ArrayList<NamedList<?>> mtasKwicResponses = new ArrayList<>();
								for (String field : mtasFields.list.keySet()) {
									for (ComponentKwic kwic : mtasFields.list.get(field).kwicList) {
										mtasKwicResponses.add(searchKwic.create(kwic, false));
									}
								}
								// add to response
								mtasResponse.add(MtasSolrComponentKwic.NAME, mtasKwicResponses);
							}
							if (mtasFields.doFacet) {
								ArrayList<NamedList<?>> mtasFacetResponses = new ArrayList<>();
								for (String field : mtasFields.list.keySet()) {
									for (ComponentFacet facet : mtasFields.list.get(field).facetList) {
										if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
											mtasFacetResponses.add(searchFacet.create(facet, true));
										} else {
											mtasFacetResponses.add(searchFacet.create(facet, false));
										}
									}
								}
								// add to response
								mtasResponse.add(MtasSolrComponentFacet.NAME, mtasFacetResponses);
							}
							if (mtasFields.doCollection) {
								ArrayList<NamedList<?>> mtasCollectionResponses = new ArrayList<>();
								for (ComponentCollection collection : mtasFields.collection) {
									if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
										mtasCollectionResponses.add(searchCollection.create(collection, true));
									} else {
										mtasCollectionResponses.add(searchCollection.create(collection, false));
									}
								}
								// add to response
								mtasResponse.add(MtasSolrComponentCollection.NAME, mtasCollectionResponses);
							}
							if (mtasFields.doList) {
								ArrayList<NamedList<?>> mtasListResponses = new ArrayList<>();
								for (String field : mtasFields.list.keySet()) {
									for (ComponentList list : mtasFields.list.get(field).listList) {
										mtasListResponses.add(searchList.create(list, false));
									}
								}
								// add to response
								mtasResponse.add(MtasSolrComponentList.NAME, mtasListResponses);
							}
							if (mtasFields.doGroup) {
								ArrayList<NamedList<?>> mtasGroupResponses = new ArrayList<>();
								for (String field : mtasFields.list.keySet()) {
									for (ComponentGroup group : mtasFields.list.get(field).groupList) {
										if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
											mtasGroupResponses.add(searchGroup.create(group, true));
										} else {
											mtasGroupResponses.add(searchGroup.create(group, false));
										}
									}
								}
								// add to response
								mtasResponse.add(MtasSolrComponentGroup.NAME, mtasGroupResponses);
							}
							if (mtasFields.doTermVector) {
								ArrayList<NamedList<?>> mtasTermVectorResponses = new ArrayList<>();
								for (String field : mtasFields.list.keySet()) {
									for (ComponentTermVector termVector : mtasFields.list.get(field).termVectorList) {
										if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
											mtasTermVectorResponses.add(searchTermvector.create(termVector, true));
										} else {
											mtasTermVectorResponses.add(searchTermvector.create(termVector, false));
										}
									}
								}
								// add to response
								mtasResponse.add(MtasSolrComponentTermvector.NAME, mtasTermVectorResponses);
							}
							if (mtasFields.doPrefix) {
								ArrayList<NamedList<?>> mtasPrefixResponses = new ArrayList<>();
								for (String field : mtasFields.list.keySet()) {
									if (mtasFields.list.get(field).prefix != null) {
										if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
											mtasPrefixResponses
													.add(searchPrefix.create(mtasFields.list.get(field).prefix, true));
										} else {
											mtasPrefixResponses
													.add(searchPrefix.create(mtasFields.list.get(field).prefix, false));
										}
									}
								}
								mtasResponse.add(MtasSolrComponentPrefix.NAME, mtasPrefixResponses);
							}
							if (mtasFields.doStats) {
								NamedList<Object> mtasStatsResponse = new SimpleOrderedMap<>();
								if (mtasFields.doStatsPositions || mtasFields.doStatsTokens
										|| mtasFields.doStatsSpans) {
									if (mtasFields.doStatsTokens) {
										ArrayList<Object> mtasStatsTokensResponses = new ArrayList<>();
										for (String field : mtasFields.list.keySet()) {
											for (ComponentToken token : mtasFields.list.get(field).statsTokenList) {
												if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
													mtasStatsTokensResponses.add(searchStats.create(token, true));
												} else {
													mtasStatsTokensResponses.add(searchStats.create(token, false));
												}
											}
										}
										mtasStatsResponse.add(MtasSolrComponentStats.NAME_TOKENS,
												mtasStatsTokensResponses);
									}
									if (mtasFields.doStatsPositions) {
										ArrayList<Object> mtasStatsPositionsResponses = new ArrayList<>();
										for (String field : mtasFields.list.keySet()) {
											for (ComponentPosition position : mtasFields.list
													.get(field).statsPositionList) {
												if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
													mtasStatsPositionsResponses.add(searchStats.create(position, true));
												} else {
													mtasStatsPositionsResponses
															.add(searchStats.create(position, false));
												}
											}
										}
										mtasStatsResponse.add(MtasSolrComponentStats.NAME_POSITIONS,
												mtasStatsPositionsResponses);
									}
									if (mtasFields.doStatsSpans) {
										ArrayList<Object> mtasStatsSpansResponses = new ArrayList<>();
										for (String field : mtasFields.list.keySet()) {
											for (ComponentSpan span : mtasFields.list.get(field).statsSpanList) {
												if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
													mtasStatsSpansResponses.add(searchStats.create(span, true));
												} else {
													mtasStatsSpansResponses.add(searchStats.create(span, false));
												}
											}
										}
										mtasStatsResponse.add(MtasSolrComponentStats.NAME_SPANS,
												mtasStatsSpansResponses);
									}
									// add to response
									mtasResponse.add(MtasSolrComponentStats.NAME, mtasStatsResponse);
								}
							}
							// add to response
							if (mtasResponse.size() > 0) {
								rb.rsp.add(NAME, mtasResponse);
							}
						}
					}
				} catch (IOException e) {
					errorStatus(solrStatus, e);
				}
			}
			if (!solrStatus.error()) {
				// always set status segments
				if (solrStatus.status().numberSegmentsTotal == null) {
					solrStatus.status().numberSegmentsTotal = rb.req.getSearcher().getRawReader().leaves().size();
					solrStatus.status().numberSegmentsFinished = solrStatus.status().numberSegmentsTotal;
				}
				// always try to set number of documents
				if (solrStatus.status().numberDocumentsTotal == null) {
					SolrIndexSearcher searcher;
					if ((searcher = rb.req.getSearcher()) != null) {
						solrStatus.status().numberDocumentsTotal = (long) searcher.numDocs();
						if (rb.getResults().docList != null) {
							solrStatus.status().numberDocumentsFinished = rb.getResults().docList.matches();
							solrStatus.status().numberDocumentsFound = rb.getResults().docList.matches();
						} else if (rb.getResults().docSet != null) {
							solrStatus.status().numberDocumentsFinished = (long) rb.getResults().docSet.size();
							solrStatus.status().numberDocumentsFound = (long) rb.getResults().docSet.size();
						}
					}
				}
			}
		} catch (ExitableDirectoryReader.ExitingReaderException e) {
			solrStatus.setError(e.getMessage());
		} finally {
			checkStatus(solrStatus);
			finishStatus(solrStatus);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.solr.handler.component.SearchComponent#modifyRequest(org.apache.
	 * solr.handler.component.ResponseBuilder,
	 * org.apache.solr.handler.component.SearchComponent,
	 * org.apache.solr.handler.component.ShardRequest)
	 */
	@Override
	public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
		 // System.out
		 // .println(System.nanoTime() + " - " + Thread.currentThread().getId()
		 // + " - " + rb.req.getParams().getBool(ShardParams.IS_SHARD, false)
		 // + " MODIFY REQUEST " + rb.stage + " " + rb.req.getParamString());
		MtasSolrStatus solrStatus = Objects
				.requireNonNull((MtasSolrStatus) rb.req.getContext().get(MtasSolrStatus.class), "couldn't find status");
		solrStatus.setStage(rb.stage);
		try {
			if (sreq.params.getBool(PARAM_MTAS, false)) {
				if (sreq.params.getBool(MtasSolrComponentStatus.PARAM_MTAS_STATUS, false)) {
					searchStatus.modifyRequest(rb, who, sreq);
				} else if (requestHandler != null) {
					sreq.params.add(MtasSolrComponentStatus.PARAM_MTAS_STATUS, CommonParams.TRUE);
				}
				if (requestHandler != null) {
					sreq.params.add(MtasSolrComponentStatus.PARAM_MTAS_STATUS + "."
							+ MtasSolrComponentStatus.NAME_MTAS_STATUS_KEY, solrStatus.shardKey(rb.stage));
				}
				if (sreq.params.getBool(MtasSolrComponentVersion.PARAM_MTAS_VERSION, false)) {
					searchVersion.modifyRequest(rb, who, sreq);
				}
				if (sreq.params.getBool(MtasSolrComponentStats.PARAM_MTAS_STATS, false)) {
					searchStats.modifyRequest(rb, who, sreq);
				}
				if (sreq.params.getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
					searchTermvector.modifyRequest(rb, who, sreq);
				}
				if (sreq.params.getBool(MtasSolrComponentPrefix.PARAM_MTAS_PREFIX, false)) {
					searchPrefix.modifyRequest(rb, who, sreq);
				}
				if (sreq.params.getBool(MtasSolrComponentFacet.PARAM_MTAS_FACET, false)) {
					searchFacet.modifyRequest(rb, who, sreq);
				}
				if (sreq.params.getBool(MtasSolrComponentCollection.PARAM_MTAS_COLLECTION, false)) {
					searchCollection.modifyRequest(rb, who, sreq);
				}
				if (sreq.params.getBool(MtasSolrComponentGroup.PARAM_MTAS_GROUP, false)) {
					searchGroup.modifyRequest(rb, who, sreq);
				}
				if (sreq.params.getBool(MtasSolrComponentList.PARAM_MTAS_LIST, false)) {
					searchList.modifyRequest(rb, who, sreq);
				}
				if (sreq.params.getBool(MtasSolrComponentDocument.PARAM_MTAS_DOCUMENT, false)) {
					searchDocument.modifyRequest(rb, who, sreq);
				}
				if (sreq.params.getBool(MtasSolrComponentKwic.PARAM_MTAS_KWIC, false)) {
					searchKwic.modifyRequest(rb, who, sreq);
				}
			}
		} catch (ExitableDirectoryReader.ExitingReaderException e) {
			solrStatus.setError(e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.solr.handler.component.SearchComponent#handleResponses(org.
	 * apache.solr.handler.component.ResponseBuilder,
	 * org.apache.solr.handler.component.ShardRequest)
	 */
	@Override
	public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
		 // System.out
		 // .println(System.nanoTime() + " - " + Thread.currentThread().getId()
		 // + " - " + rb.req.getParams().getBool(ShardParams.IS_SHARD, false)
		 // + " HANDLERESPONSES " + rb.stage + " " + rb.req.getParamString());
		MtasSolrStatus solrStatus = Objects
				.requireNonNull((MtasSolrStatus) rb.req.getContext().get(MtasSolrStatus.class), "couldn't find status");
		solrStatus.setStage(rb.stage);
		try {
			if (rb.req.getParams().getBool(PARAM_MTAS, false)) {

				// do nothing
			}
		} catch (ExitableDirectoryReader.ExitingReaderException e) {
			solrStatus.setError(e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.solr.handler.component.SearchComponent#distributedProcess(org.
	 * apache.solr.handler.component.ResponseBuilder)
	 */
	@Override
	public void finishStage(ResponseBuilder rb) {
		 // System.out
		 // .println(System.nanoTime() + " - " + Thread.currentThread().getId()
		 //  + " - " + rb.req.getParams().getBool(ShardParams.IS_SHARD, false)
		 // + " FINISHRESPONSES " + rb.stage + " " + rb.req.getParamString());
		MtasSolrStatus solrStatus = Objects
				.requireNonNull((MtasSolrStatus) rb.req.getContext().get(MtasSolrStatus.class), "couldn't find status");
		solrStatus.setStage(rb.stage);
		try {
			if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
				Status status = solrStatus.status();
				if (status.numberDocumentsFound == null) {
					status.numberDocumentsFound = rb.getNumberDocumentsFound();
				}
				// try to finish status from get fields stage
			} else if (rb.stage >= ResponseBuilder.STAGE_GET_FIELDS) {
				finishStatus(solrStatus);
			}
			if (rb.req.getParams().getBool(PARAM_MTAS, false)) {
				if (rb.req.getParams().getBool(MtasSolrComponentVersion.PARAM_MTAS_VERSION, false)) {
					searchVersion.finishStage(rb);
				}
				if (rb.req.getParams().getBool(MtasSolrComponentStats.PARAM_MTAS_STATS, false)) {
					searchStats.finishStage(rb);
				}
				if (rb.req.getParams().getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
					searchTermvector.finishStage(rb);
				}
				if (rb.req.getParams().getBool(MtasSolrComponentPrefix.PARAM_MTAS_PREFIX, false)) {
					searchPrefix.finishStage(rb);
				}
				if (rb.req.getParams().getBool(MtasSolrComponentFacet.PARAM_MTAS_FACET, false)) {
					searchFacet.finishStage(rb);
				}
				if (rb.req.getParams().getBool(MtasSolrComponentCollection.PARAM_MTAS_COLLECTION, false)) {
					searchCollection.finishStage(rb);
				}
				if (rb.req.getParams().getBool(MtasSolrComponentGroup.PARAM_MTAS_GROUP, false)) {
					searchGroup.finishStage(rb);
				}
				if (rb.req.getParams().getBool(MtasSolrComponentList.PARAM_MTAS_LIST, false)) {
					searchList.finishStage(rb);
				}
				if (rb.req.getParams().getBool(MtasSolrComponentDocument.PARAM_MTAS_DOCUMENT, false)) {
					searchDocument.finishStage(rb);
				}
				if (rb.req.getParams().getBool(MtasSolrComponentKwic.PARAM_MTAS_KWIC, false)) {
					searchKwic.finishStage(rb);
				}
				mtasSolrResultMerge.merge(rb);
			}
		} catch (ExitableDirectoryReader.ExitingReaderException e) {
			solrStatus.setError(e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.solr.handler.component.SearchComponent#distributedProcess(org.
	 * apache.solr.handler.component.ResponseBuilder)
	 */
	@Override
	public int distributedProcess(ResponseBuilder rb) throws IOException {
		 // System.out.println(System.nanoTime() + " - "
		 // + Thread.currentThread().getId() + " - "
		 // + rb.req.getParams().getBool(ShardParams.IS_SHARD, false)
		 // + " DISTIRBUTEDPROCESS " + rb.stage + " " + rb.req.getParamString());
		MtasSolrStatus solrStatus = Objects
				.requireNonNull((MtasSolrStatus) rb.req.getContext().get(MtasSolrStatus.class), "couldn't find status");
		solrStatus.setStage(rb.stage);
		try {
			if (rb.req.getParams().getBool(PARAM_MTAS, false)) {
				if (rb.stage == STAGE_TERMVECTOR_MISSING_TOP || rb.stage == STAGE_TERMVECTOR_MISSING_KEY
						|| rb.stage == STAGE_TERMVECTOR_FINISH) {
					ComponentFields mtasFields = getMtasFields(rb);
					searchTermvector.distributedProcess(rb, mtasFields);
				} else if (rb.stage == STAGE_LIST) {
					ComponentFields mtasFields = getMtasFields(rb);
					searchList.distributedProcess(rb, mtasFields);
				} else if (rb.stage == STAGE_PREFIX) {
					ComponentFields mtasFields = getMtasFields(rb);
					searchPrefix.distributedProcess(rb, mtasFields);
				} else if (rb.stage == STAGE_STATS) {
					ComponentFields mtasFields = getMtasFields(rb);
					searchStats.distributedProcess(rb, mtasFields);
				} else if (rb.stage == STAGE_FACET) {
					ComponentFields mtasFields = getMtasFields(rb);
					searchFacet.distributedProcess(rb, mtasFields);
				} else if (rb.stage == STAGE_COLLECTION_INIT || rb.stage == STAGE_COLLECTION_FINISH) {
					ComponentFields mtasFields = getMtasFields(rb);
					searchCollection.distributedProcess(rb, mtasFields);
				} else if (rb.stage == STAGE_GROUP) {
					ComponentFields mtasFields = getMtasFields(rb);
					searchGroup.distributedProcess(rb, mtasFields);
				} else if (rb.stage == STAGE_DOCUMENT) {
					ComponentFields mtasFields = getMtasFields(rb);
					searchDocument.distributedProcess(rb, mtasFields);
				}
				// compute new stage and return if not finished
				if (rb.stage >= ResponseBuilder.STAGE_EXECUTE_QUERY && rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
					if (rb.stage < STAGE_TERMVECTOR_MISSING_TOP
							&& rb.req.getParams().getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
						return STAGE_TERMVECTOR_MISSING_TOP;
					} else if (rb.stage < STAGE_TERMVECTOR_MISSING_KEY
							&& rb.req.getParams().getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
						return STAGE_TERMVECTOR_MISSING_KEY;
					} else if (rb.stage < STAGE_TERMVECTOR_FINISH
							&& rb.req.getParams().getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
						return STAGE_TERMVECTOR_FINISH;
					} else if (rb.stage < STAGE_LIST
							&& rb.req.getParams().getBool(MtasSolrComponentList.PARAM_MTAS_LIST, false)) {
						return STAGE_LIST;
					} else if (rb.stage < STAGE_PREFIX
							&& rb.req.getParams().getBool(MtasSolrComponentPrefix.PARAM_MTAS_PREFIX, false)) {
						return STAGE_PREFIX;
					} else if (rb.stage < STAGE_STATS
							&& rb.req.getParams().getBool(MtasSolrComponentStats.PARAM_MTAS_STATS, false)) {
						return STAGE_STATS;
					} else if (rb.stage < STAGE_FACET
							&& rb.req.getParams().getBool(MtasSolrComponentFacet.PARAM_MTAS_FACET, false)) {
						return STAGE_FACET;
					} else if (rb.stage < STAGE_GROUP
							&& rb.req.getParams().getBool(MtasSolrComponentGroup.PARAM_MTAS_GROUP, false)) {
						return STAGE_GROUP;
					} else if (rb.stage < STAGE_COLLECTION_INIT
							&& rb.req.getParams().getBool(MtasSolrComponentCollection.PARAM_MTAS_COLLECTION, false)) {
						return STAGE_COLLECTION_INIT;
					} else if (rb.stage < STAGE_COLLECTION_FINISH
							&& rb.req.getParams().getBool(MtasSolrComponentCollection.PARAM_MTAS_COLLECTION, false)) {
						return STAGE_COLLECTION_FINISH;
					}
				} else if (rb.stage >= ResponseBuilder.STAGE_GET_FIELDS && rb.stage < ResponseBuilder.STAGE_DONE) {
					if (rb.stage < STAGE_DOCUMENT
							&& rb.req.getParams().getBool(MtasSolrComponentDocument.PARAM_MTAS_DOCUMENT, false)) {
						return STAGE_DOCUMENT;
					}
				}
			}
		} catch (ExitableDirectoryReader.ExitingReaderException e) {
			solrStatus.setError(e.getMessage());
			finishStatus(solrStatus);
		} finally {
		  checkStatus(solrStatus);
		}
		return ResponseBuilder.STAGE_DONE;
	}

	/**
	 * Gets the mtas fields.
	 *
	 * @param rb
	 *            the rb
	 * @return the mtas fields
	 */

	private ComponentFields getMtasFields(ResponseBuilder rb) {
		return (ComponentFields) rb.req.getContext().get(ComponentFields.class);
	}

	/**
	 * Initialize request handler.
	 *
	 * @param rb
	 *            the rb
	 */
	private void initializeRequestHandler(ResponseBuilder rb) {
		if (requestHandler == null) {
			// try to initialize
			for (Entry<String, SolrInfoBean> entry : rb.req.getCore().getInfoRegistry().entrySet()) {
				if (entry.getValue() instanceof MtasRequestHandler) {
					requestHandlerName = entry.getKey();
					requestHandler = (MtasRequestHandler) entry.getValue();
					break;
				}
			}
		}
	}

	/**
	 * Check status.
	 *
	 * @param status
	 *            the status
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private void checkStatus(MtasSolrStatus status) throws IOException {
		if (!status.finished()) {
			if (status.error()) {
				status.setFinished();
				if (requestHandler != null) {
					requestHandler.finishStatus(status);
				}
				throw new IOException(status.errorMessage());
			} else if (status.abort()) {
				status.setFinished();
				if (requestHandler != null) {
					requestHandler.finishStatus(status);
				}
				throw new IOException(status.abortMessage());
			}
		}
	}

	/**
	 * Register status.
	 *
	 * @param solrStatus
	 *            the solr status
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private void registerStatus(MtasSolrStatus solrStatus) throws IOException {
		if (requestHandler != null) {
			Map<String, ShardStatus> shards = solrStatus.getShards();
			if (shards != null) {
				Status status = solrStatus.status();
				status.numberDocumentsTotal = Long.valueOf(0);
				status.numberSegmentsTotal = 0;
				for (Entry<String, ShardStatus> entry : shards.entrySet()) {
					// get shard info
					ShardInformation shardInformation = requestHandler.getShardInformation(entry.getKey());
					if (shardInformation == null) {
						throw new IOException("no shard information " + entry.getKey());
					}
					ShardStatus shardStatus = entry.getValue();
					shardStatus.name = shardInformation.name;
					shardStatus.location = entry.getKey();
					shardStatus.mtasHandler = shardInformation.mtasHandler;
					shardStatus.numberDocumentsTotal = shardInformation.numberOfDocuments;
					shardStatus.numberSegmentsTotal = shardInformation.numberOfSegments;
					status.numberDocumentsTotal += shardInformation.numberOfDocuments;
					status.numberSegmentsTotal += shardInformation.numberOfSegments;
				}
			}
			requestHandler.registerStatus(solrStatus);
		}
	}

	/**
	 * Error status.
	 *
	 * @param status
	 *            the status
	 * @param exception
	 *            the exception
	 */
	private void errorStatus(MtasSolrStatus status, IOException exception) {
		try {
			status.setError(exception);
			if (requestHandler != null) {
				requestHandler.finishStatus(status);
			}
		} catch (IOException e) {
			log.error(e);
		}
	}

	/**
	 * Finish status.
	 *
	 * @param status
	 *            the status
	 */
	private void finishStatus(MtasSolrStatus status) {
		if (!status.finished()) {
			status.setFinished();
			if (requestHandler != null) {
				try {
					requestHandler.finishStatus(status);
				} catch (IOException e) {
					log.error(e);
				}
			}
		}
	}

}