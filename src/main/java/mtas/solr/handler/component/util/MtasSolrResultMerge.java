package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;

import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrResultMerge.
 */
public class MtasSolrResultMerge {

	/** The Constant log. */
	private static final Log log = LogFactory.getLog(MtasSolrResultMerge.class);

	/**
	 * Merge.
	 *
	 * @param rb
	 *            the rb
	 */
	@SuppressWarnings("unchecked")
	public void merge(ResponseBuilder rb) {
		if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
			// mtas response
			NamedList<Object> mtasResponse = null;
			boolean newResponse = false;
			try {
				mtasResponse = (NamedList<Object>) rb.rsp.getValues().get(MtasSolrSearchComponent.NAME);
			} catch (ClassCastException e) {
				log.debug(e);
				mtasResponse = null;
			}
			if (mtasResponse == null) {
				newResponse = true;
				mtasResponse = new SimpleOrderedMap<>();
			}

			for (ShardRequest sreq : rb.finished) {
				if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
					// merge version
					if (rb.req.getParams().getBool(MtasSolrComponentVersion.PARAM_MTAS_VERSION, false)) {
						mergeNamedList(sreq, mtasResponse, MtasSolrComponentVersion.NAME, null);
					}
					// merge stats
					if (rb.req.getParams().getBool(MtasSolrComponentStats.PARAM_MTAS_STATS, false)) {
						mergeNamedList(sreq, mtasResponse, MtasSolrComponentStats.NAME, null);
					}
					// merge group
					if (rb.req.getParams().getBool(MtasSolrComponentGroup.PARAM_MTAS_GROUP, false)) {
						mergeArrayList(sreq, mtasResponse, MtasSolrComponentGroup.NAME, null, false);
					}
					// merge facet
					if (rb.req.getParams().getBool(MtasSolrComponentFacet.PARAM_MTAS_FACET, false)) {
						mergeArrayList(sreq, mtasResponse, MtasSolrComponentFacet.NAME, null, false);
					}
					// merge collection
					if (rb.req.getParams().getBool(MtasSolrComponentCollection.PARAM_MTAS_COLLECTION, false)) {
						mergeArrayList(sreq, mtasResponse, MtasSolrComponentCollection.NAME, null, false);
					}
					// merge prefix
					if (rb.req.getParams().getBool(MtasSolrComponentPrefix.PARAM_MTAS_PREFIX, false)) {
						mergeArrayList(sreq, mtasResponse, MtasSolrComponentPrefix.NAME, null, false);
					}
				} else if (rb.stage == MtasSolrSearchComponent.STAGE_COLLECTION_INIT) {
					// merge collection
					if (rb.req.getParams().getBool(MtasSolrComponentCollection.PARAM_MTAS_COLLECTION, false)) {
						mergeArrayList(sreq, mtasResponse, MtasSolrComponentCollection.NAME, null, false);
					}
				} else if (rb.stage == MtasSolrSearchComponent.STAGE_TERMVECTOR_MISSING_KEY) {
					// merge termvector
					if (rb.req.getParams().getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
						mergeArrayList(sreq, mtasResponse, MtasSolrComponentTermvector.NAME, null, false);
					}
				} else if (rb.stage == MtasSolrSearchComponent.STAGE_LIST) {
					// merge list
					if (rb.req.getParams().getBool(MtasSolrComponentList.PARAM_MTAS_LIST, false)) {
						mergeArrayList(sreq, mtasResponse, MtasSolrComponentList.NAME, ShardRequest.PURPOSE_PRIVATE,
								true);
					}
				} else if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
					// merge document
					if (rb.req.getParams().getBool(MtasSolrComponentDocument.PARAM_MTAS_DOCUMENT, false)) {
						mergeArrayList(sreq, mtasResponse, MtasSolrComponentDocument.NAME, ShardRequest.PURPOSE_PRIVATE,
								true);
					}
					// merge kwic
					if (rb.req.getParams().getBool(MtasSolrComponentKwic.PARAM_MTAS_KWIC, false)) {
						mergeArrayList(sreq, mtasResponse, MtasSolrComponentKwic.NAME, ShardRequest.PURPOSE_PRIVATE,
								true);
					}
				}
			}
			if (newResponse && mtasResponse.size() > 0) {
				rb.rsp.add(MtasSolrSearchComponent.NAME, mtasResponse);
			}
		}
	}

	/**
	 * Merge named list.
	 *
	 * @param sreq
	 *            the sreq
	 * @param mtasResponse
	 *            the mtas response
	 * @param key
	 *            the key
	 * @param preferredPurpose
	 *            the preferred purpose
	 */
	@SuppressWarnings("unchecked")
	private void mergeNamedList(ShardRequest sreq, NamedList<Object> mtasResponse, String key,
			Integer preferredPurpose) {
		// create new response for key
		NamedList<Object> mtasListResponse;
		Object o = mtasResponse.get(key);
		if (o instanceof NamedList) {
			mtasListResponse = (NamedList<Object>) o;
		} else {
			mtasListResponse = new SimpleOrderedMap<>();
			mtasResponse.removeAll(key);
			mtasResponse.add(key, mtasListResponse);
		}
		// collect responses for each shard
		HashMap<String, NamedList<Object>> mtasListShardResponses = new HashMap<>();
		for (ShardResponse response : sreq.responses) {
			// only continue if new shard or preferred purpose
			if (mtasListShardResponses.containsKey(response.getShard())
					&& ((preferredPurpose == null) || (sreq.purpose != preferredPurpose))) {
				break;
			}
			// update
			try {
				NamedList<Object> result = response.getSolrResponse().getResponse();
				NamedList<Object> data = (NamedList<Object>) result.findRecursive("mtas", key);
				if (data != null) {
					mtasListShardResponses.put(response.getShard(), MtasSolrResultUtil.decode(data));
				}
			} catch (ClassCastException e) {
				log.debug(e);
			}
		}
		try {
			for (NamedList<Object> mtasListShardResponse : mtasListShardResponses.values()) {
				mergeResponsesNamedList(mtasListResponse, mtasListShardResponse);
			}
		} catch (IOException e) {
			log.error(e);
		}
	}

	/**
	 * Merge array list.
	 *
	 * @param sreq
	 *            the sreq
	 * @param mtasResponse
	 *            the mtas response
	 * @param key
	 *            the key
	 * @param preferredPurpose
	 *            the preferred purpose
	 * @param mergeAllShardResponses
	 *            the merge all shard responses
	 */
	@SuppressWarnings("unchecked")
	private void mergeArrayList(ShardRequest sreq, NamedList<Object> mtasResponse, String key, Integer preferredPurpose,
			boolean mergeAllShardResponses) {
		// create new response for key
		ArrayList<Object> mtasListResponse;
		Object o = mtasResponse.get(key);
		if (o instanceof ArrayList) {
			mtasListResponse = (ArrayList<Object>) o;
		} else {
			mtasListResponse = new ArrayList<>();
			mtasResponse.removeAll(key);
			mtasResponse.add(key, mtasListResponse);
		}
		// collect responses for each shard
		HashMap<String, ArrayList<Object>> mtasListShardResponses = new HashMap<>();
		ArrayList<ArrayList<Object>> mtasListShardResponsesExtra = new ArrayList<>();
		for (ShardResponse response : sreq.responses) {
			// only continue if new shard or preferred purpose
			if (mtasListShardResponses.containsKey(response.getShard())
					&& ((preferredPurpose == null) || (sreq.purpose != preferredPurpose))) {
				break;
			}
			// update
			try {
				NamedList<Object> result = response.getSolrResponse().getResponse();
				ArrayList<Object> data = (ArrayList<Object>) result.findRecursive("mtas", key);
				if (data != null) {
					if (mtasListShardResponses.containsKey(response.getShardAddress())) {
						if (mergeAllShardResponses) {
							mtasListShardResponsesExtra.add(data);
						}
					} else {
						mtasListShardResponses.put(response.getShardAddress(), data);
					}
				}
			} catch (ClassCastException e) {
				log.error(e);
			}
		}

		try {
			for (ArrayList<Object> mtasListShardResponse : mtasListShardResponses.values()) {
				mergeResponsesArrayList(mtasListResponse, mtasListShardResponse);
			}
			for (ArrayList<Object> mtasListShardResponse : mtasListShardResponsesExtra) {
				mergeResponsesArrayList(mtasListResponse, mtasListShardResponse);
			}
		} catch (IOException e) {
			log.error(e);
		}
	}

	/**
	 * Merge responses sorted set.
	 *
	 * @param originalList
	 *            the original list
	 * @param shardList
	 *            the shard list
	 */
	private void mergeResponsesSortedSet(SortedSet<Object> originalList, SortedSet<Object> shardList) {
		for (Object item : shardList) {
			originalList.add(item);
		}
	}

	/**
	 * Merge responses array list.
	 *
	 * @param originalList
	 *            the original list
	 * @param shardList
	 *            the shard list
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings("unchecked")
	private void mergeResponsesArrayList(ArrayList<Object> originalList, ArrayList<Object> shardList)
			throws IOException {
		// get keys from original
		HashMap<String, Object> originalKeyList = new HashMap<>();
		for (Object item : originalList) {
			if (item instanceof NamedList<?>) {
				NamedList<Object> itemList = (NamedList<Object>) item;
				Object key = itemList.get("key");
				if ((key != null) && (key instanceof String)) {
					originalKeyList.put((String) key, item);
				}
			}
		}
		for (Object item : shardList) {
			if (item instanceof NamedList<?>) {
				NamedList<Object> itemList = (NamedList<Object>) item;
				Object key = itemList.get("key");
				// item with key
				if ((key != null) && (key instanceof String)) {
					// merge
					if (originalKeyList.containsKey(key)) {
						Object originalItem = originalKeyList.get(key);
						if (originalItem.getClass().equals(item.getClass())) {
							mergeResponsesNamedList((NamedList<Object>) originalItem, (NamedList<Object>) item);
						} else {
							// ignore?
						}
						// add
					} else {
						Object clonedItem = adjustablePartsCloned(item);
						originalList.add(clonedItem);
						originalKeyList.put((String) key, clonedItem);
					}
				} else {
					originalList.add(item);
				}
			} else {
				originalList.add(item);
			}
		}
	}

	/**
	 * Merge responses named list.
	 *
	 * @param mainResponse
	 *            the main response
	 * @param shardResponse
	 *            the shard response
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void mergeResponsesNamedList(NamedList<Object> mainResponse, NamedList<Object> shardResponse)
			throws IOException {
		Iterator<Entry<String, Object>> it = shardResponse.iterator();
		while (it.hasNext()) {
			Entry<String, Object> entry = it.next();
			String name = entry.getKey();
			Object shardValue = entry.getValue();
			int originalId = mainResponse.indexOf(name, 0);
			if (originalId < 0) {
				mainResponse.add(name, adjustablePartsCloned(shardValue));
			} else {
				Object original = mainResponse.getVal(originalId);
				if (original == null) {
					original = adjustablePartsCloned(shardValue);
				} else if (shardValue != null && original.getClass().equals(shardValue.getClass())) {
					// merge ArrayList
					if (original instanceof ArrayList) {
						ArrayList originalList = (ArrayList) original;
						ArrayList shardList = (ArrayList) shardValue;
						mergeResponsesArrayList(originalList, shardList);
						// merge Namedlist
					} else if (original instanceof NamedList<?>) {
						mergeResponsesNamedList((NamedList<Object>) original, (NamedList<Object>) shardValue);
						// merge SortedSet
					} else if (original instanceof SortedSet<?>) {
						mergeResponsesSortedSet((SortedSet<Object>) original, (SortedSet<Object>) shardValue);
					} else if (original instanceof MtasSolrMtasResult) {
						MtasSolrMtasResult originalComponentResult = (MtasSolrMtasResult) original;
						originalComponentResult.merge((MtasSolrMtasResult) shardValue);
					} else if (original instanceof MtasSolrCollectionResult) {
						MtasSolrCollectionResult originalComponentResult = (MtasSolrCollectionResult) original;
						originalComponentResult.merge((MtasSolrCollectionResult) shardValue);
					} else if (original instanceof String) {
						// ignore?
					} else if (original instanceof Integer) {
						original = (Integer) original + ((Integer) shardValue);
					} else if (original instanceof Long) {
						original = (Long) original + ((Long) shardValue);
					} else {
						// ignore?
					}
					mainResponse.setVal(originalId, original);
				} else {
					// ignore?
				}
			}
		}
	}

	/**
	 * Adjustable parts cloned.
	 *
	 * @param original
	 *            the original
	 * @return the object
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object adjustablePartsCloned(Object original) {
		if (original instanceof NamedList) {
			NamedList<Object> newObject = new SimpleOrderedMap();
			NamedList<Object> originalObject = (NamedList<Object>) original;
			for (int i = 0; i < originalObject.size(); i++) {
				newObject.add(originalObject.getName(i), adjustablePartsCloned(originalObject.getVal(i)));
			}
			return newObject;
		} else if (original instanceof ArrayList) {
			ArrayList<Object> newObject = new ArrayList<>();
			ArrayList<Object> originalObject = (ArrayList<Object>) original;
			for (int i = 0; i < originalObject.size(); i++) {
				newObject.add(adjustablePartsCloned(originalObject.get(i)));
			}
			return newObject;
		} else if (original instanceof Integer) {
			return original;
		}
		return original;
	}

}
