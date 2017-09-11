package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.noggit.JSONParser;
import org.noggit.JSONUtil;

import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentCollection;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentCollection.
 */
public class MtasSolrComponentCollection
    implements MtasSolrComponent<ComponentCollection> {

  /** The Constant log. */
  private static final Log log = LogFactory
      .getLog(MtasSolrComponentCollection.class);

  /** The Constant PARAM_MTAS_COLLECTION. */
  public static final String PARAM_MTAS_COLLECTION = MtasSolrSearchComponent.PARAM_MTAS
      + ".collection";

  /** The Constant NAME_MTAS_COLLECTION_ACTION. */
  public static final String NAME_MTAS_COLLECTION_ACTION = "action";

  /** The Constant NAME_MTAS_COLLECTION_ID. */
  public static final String NAME_MTAS_COLLECTION_ID = "id";

  /** The Constant NAME_MTAS_COLLECTION_FIELD. */
  public static final String NAME_MTAS_COLLECTION_FIELD = "field";

  /** The Constant NAME_MTAS_COLLECTION_POST. */
  public static final String NAME_MTAS_COLLECTION_POST = "post";

  /** The Constant NAME_MTAS_COLLECTION_URL. */
  public static final String NAME_MTAS_COLLECTION_URL = "url";

  /** The Constant NAME_MTAS_COLLECTION_COLLECTION. */
  public static final String NAME_MTAS_COLLECTION_COLLECTION = "collection";

  /** The Constant NAME_MTAS_COLLECTION_KEY. */
  public static final String NAME_MTAS_COLLECTION_KEY = "key";

  /** The search component. */
  private MtasSolrSearchComponent searchComponent;

  /**
   * Instantiates a new mtas solr component collection.
   *
   * @param searchComponent the search component
   */
  public MtasSolrComponentCollection(MtasSolrSearchComponent searchComponent) {
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
  public void prepare(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    // System.out.println(
    // "collection: " + System.nanoTime() + " - " +
    // Thread.currentThread().getId()
    // + " - " + rb.req.getParams().getBool("isShard", false) + " PREPARE "
    // + rb.stage + " " + rb.req.getParamString());
    Set<String> ids = MtasSolrResultUtil
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_COLLECTION);
    if (!ids.isEmpty()) {
      int tmpCounter = 0;
      String[] keys = new String[ids.size()];
      String[] actions = new String[ids.size()];
      String[] fields = new String[ids.size()];
      String[] collectionIds = new String[ids.size()];
      String[] posts = new String[ids.size()];
      String[] urls = new String[ids.size()];
      String[] collections = new String[ids.size()];
      for (String id : ids) {
        actions[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_COLLECTION + "."
            + id + "." + NAME_MTAS_COLLECTION_ACTION, null);
        keys[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_COLLECTION + "." + id + "." + NAME_MTAS_COLLECTION_KEY,
            String.valueOf(tmpCounter)).trim();
        fields[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_COLLECTION + "." + id + "." + NAME_MTAS_COLLECTION_FIELD,
            null);
        collectionIds[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_COLLECTION + "." + id + "." + NAME_MTAS_COLLECTION_ID,
            null);
        posts[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_COLLECTION + "." + id + "." + NAME_MTAS_COLLECTION_POST,
            null);
        urls[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_COLLECTION + "." + id + "." + NAME_MTAS_COLLECTION_URL,
            null);
        collections[tmpCounter] = rb.req.getParams().get(PARAM_MTAS_COLLECTION
            + "." + id + "." + NAME_MTAS_COLLECTION_COLLECTION, null);
        tmpCounter++;
      }
      mtasFields.doCollection = true;
      MtasSolrResultUtil.compareAndCheck(keys, actions,
          NAME_MTAS_COLLECTION_KEY, NAME_MTAS_COLLECTION_ACTION, true);
      MtasSolrResultUtil.compareAndCheck(keys, fields, NAME_MTAS_COLLECTION_KEY,
          NAME_MTAS_COLLECTION_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(keys, collectionIds,
          NAME_MTAS_COLLECTION_KEY, NAME_MTAS_COLLECTION_ID, false);
      MtasSolrResultUtil.compareAndCheck(keys, posts, NAME_MTAS_COLLECTION_KEY,
          NAME_MTAS_COLLECTION_POST, false);
      MtasSolrResultUtil.compareAndCheck(keys, urls, NAME_MTAS_COLLECTION_KEY,
          NAME_MTAS_COLLECTION_URL, false);
      MtasSolrResultUtil.compareAndCheck(keys, collections,
          NAME_MTAS_COLLECTION_KEY, NAME_MTAS_COLLECTION_COLLECTION, false);
      for (int i = 0; i < actions.length; i++) {
        if (actions[i] != null) {
          ComponentCollection componentCollection;
          switch (actions[i]) {
          case ComponentCollection.ACTION_LIST:
            componentCollection = new ComponentCollection(keys[i],
                ComponentCollection.ACTION_LIST);
            componentCollection.setListVariables();
            mtasFields.collection.add(componentCollection);
            break;
          case ComponentCollection.ACTION_CHECK:
            if (collectionIds[i] != null) {
              componentCollection = new ComponentCollection(keys[i],
                  ComponentCollection.ACTION_CHECK);
              componentCollection.setCheckVariables(collectionIds[i]);
              mtasFields.collection.add(componentCollection);
            } else {
              throw new IOException(
                  "no id defined for collection (" + actions[i] + ")");
            }
            break;
          case ComponentCollection.ACTION_GET:
            if (collectionIds[i] != null) {
              componentCollection = new ComponentCollection(keys[i],
                  ComponentCollection.ACTION_GET);
              componentCollection.setGetVariables(collectionIds[i]);
              mtasFields.collection.add(componentCollection);
            } else {
              throw new IOException(
                  "no id defined for collection (" + actions[i] + ")");
            }
            break;
          case ComponentCollection.ACTION_CREATE:
            if (fields[i] != null) {
              Set<String> fieldList = new HashSet<>(
                  Arrays.asList(fields[i].split(",")));
              componentCollection = new ComponentCollection(keys[i],
                  ComponentCollection.ACTION_CREATE);
              componentCollection.setCreateVariables(collectionIds[i],
                  fieldList);
              mtasFields.doCollection = true;
              mtasFields.collection.add(componentCollection);
              rb.setNeedDocSet(true);
            } else {
              throw new IOException(
                  "no field defined for collection (" + actions[i] + ")");
            }
            break;
          case ComponentCollection.ACTION_POST:
            if (posts[i] != null) {
              componentCollection = new ComponentCollection(keys[i],
                  ComponentCollection.ACTION_POST);
              componentCollection.setPostVariables(collectionIds[i],
                  stringToStringValues(posts[i]));
              mtasFields.collection.add(componentCollection);
            } else {
              throw new IOException(
                  "no post defined for collection (" + actions[i] + ")");
            }
            break;
          case ComponentCollection.ACTION_IMPORT:
            if (urls[i] != null && collections[i] != null) {
              componentCollection = new ComponentCollection(keys[i],
                  ComponentCollection.ACTION_IMPORT);
              componentCollection.setImportVariables(collectionIds[i], urls[i],
                  collections[i]);
              mtasFields.collection.add(componentCollection);
            } else {
              throw new IOException(
                  "no url or collection defined for collection (" + actions[i] + ")");
            }
            break;
          case ComponentCollection.ACTION_DELETE:
            if (collectionIds[i] != null) {
              componentCollection = new ComponentCollection(keys[i],
                  ComponentCollection.ACTION_DELETE);
              componentCollection.setDeleteVariables(collectionIds[i]);
              mtasFields.collection.add(componentCollection);
            } else {
              throw new IOException(
                  "no id defined for collection (" + actions[i] + ")");
            }
            break;
          case ComponentCollection.ACTION_EMPTY:
            componentCollection = new ComponentCollection(keys[i],
                ComponentCollection.ACTION_EMPTY);
            mtasFields.collection.add(componentCollection);
            break;
          default:
            throw new IOException(
                "unrecognized action '" + actions[i] + "' for collection");
          }
        } else {
          throw new IOException("no action defined for collection");
        }
      }
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
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq) {
    // System.out.println(
    // "collection: " + System.nanoTime() + " - " +
    // Thread.currentThread().getId()
    // + " - " + rb.req.getParams().getBool("isShard", false)
    // + " MODIFYREQUEST " + rb.stage + " " + rb.req.getParamString());
    if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
        && sreq.params.getBool(PARAM_MTAS_COLLECTION, false)) {
      if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
        // do nothing
      } else {
        // remove for other requests
        Set<String> keys = MtasSolrResultUtil
            .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_COLLECTION);
        sreq.params.remove(PARAM_MTAS_COLLECTION);
        for (String key : keys) {
          sreq.params.remove(PARAM_MTAS_COLLECTION + "." + key + "."
              + NAME_MTAS_COLLECTION_ACTION);
          sreq.params.remove(PARAM_MTAS_COLLECTION + "." + key + "."
              + NAME_MTAS_COLLECTION_ID);
          sreq.params.remove(PARAM_MTAS_COLLECTION + "." + key + "."
              + NAME_MTAS_COLLECTION_FIELD);
          sreq.params.remove(PARAM_MTAS_COLLECTION + "." + key + "."
              + NAME_MTAS_COLLECTION_POST);
          sreq.params.remove(PARAM_MTAS_COLLECTION + "." + key + "."
              + NAME_MTAS_COLLECTION_KEY);
          sreq.params.remove(PARAM_MTAS_COLLECTION + "." + key + "."
              + NAME_MTAS_COLLECTION_URL);
          sreq.params.remove(PARAM_MTAS_COLLECTION + "." + key + "."
              + NAME_MTAS_COLLECTION_COLLECTION);
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#create(mtas.codec.util.
   * CodecComponent.BasicComponent, java.lang.Boolean)
   */
  public SimpleOrderedMap<Object> create(
      ComponentCollection componentCollection, Boolean encode)
      throws IOException {
    MtasSolrCollectionResult data = createMtasSolrCollectionResult(
        componentCollection, encode ? false : true);
    // Create response
    SimpleOrderedMap<Object> mtasCollectionResponse = new SimpleOrderedMap<>();
    mtasCollectionResponse.add("key", componentCollection.key);
    if (encode) {
      mtasCollectionResponse.add("_encoded_data",
          MtasSolrResultUtil.encode(data));
    } else {
      mtasCollectionResponse.add("data", data);
      MtasSolrResultUtil.rewrite(mtasCollectionResponse, searchComponent);
    }
    return mtasCollectionResponse;
  }

  /**
   * Creates the mtas solr collection result.
   *
   * @param componentCollection the component collection
   * @param storeIfRelevant the store if relevant
   * @return the mtas solr collection result
   * @throws IOException Signals that an I/O exception has occurred.
   */
  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#create(mtas.codec.util.
   * CodecComponent.BasicComponent, java.lang.Boolean)
   */
  private MtasSolrCollectionResult createMtasSolrCollectionResult(
      ComponentCollection componentCollection, boolean storeIfRelevant)
      throws IOException {
    // System.out.println("collection: " + System.nanoTime() + " - "
    // + Thread.currentThread().getId() + " - " + " CREATE ");
    if (componentCollection != null) {
      // Create response
      MtasSolrCollectionResult data = new MtasSolrCollectionResult(
          componentCollection);
      if (componentCollection.action()
          .equals(ComponentCollection.ACTION_CREATE)) {
        if (storeIfRelevant && componentCollection.version == null) {
          componentCollection.version = searchComponent.getCollectionCache()
              .create(componentCollection.id,
                  componentCollection.values().size(),
                  componentCollection.values());
        }
        data.setCreate(searchComponent.getCollectionCache().now(),
            searchComponent.getCollectionCache().check(componentCollection.id));
      } else if (componentCollection.action()
          .equals(ComponentCollection.ACTION_LIST)) {
        // retrieve and add list to result
        data.setList(searchComponent.getCollectionCache().now(),
            searchComponent.getCollectionCache().list());
      } else if (componentCollection.action()
          .equals(ComponentCollection.ACTION_CHECK)) {
        // retrieve and add status to result
        data.setCheck(searchComponent.getCollectionCache().now(),
            searchComponent.getCollectionCache().check(componentCollection.id));
      } else if (componentCollection.action()
          .equals(ComponentCollection.ACTION_GET)) {
        // retrieve and add status to result
        HashSet<String> values = searchComponent.getCollectionCache()
            .getDataById(componentCollection.id);
        if (values != null) {
          data.setGet(searchComponent.getCollectionCache().now(),
              searchComponent.getCollectionCache()
                  .check(componentCollection.id),
              values);
        }
      } else if (componentCollection.action()
          .equals(ComponentCollection.ACTION_EMPTY)) {
        // empty
        searchComponent.getCollectionCache().empty();
      } else if (componentCollection.action()
          .equals(ComponentCollection.ACTION_POST)) {
        // store if not already stored
        if (componentCollection.version == null) {
          componentCollection.version = searchComponent.getCollectionCache()
              .create(componentCollection.id,
                  componentCollection.values().size(),
                  componentCollection.values());
        }
        // add status to result
        data.setPost(searchComponent.getCollectionCache().now(),
            searchComponent.getCollectionCache().check(componentCollection.id));
      } else if (componentCollection.action()
          .equals(ComponentCollection.ACTION_IMPORT)) {
        // import if not already stored
        if (componentCollection.version == null) {
          componentCollection.version = searchComponent.getCollectionCache()
              .create(componentCollection.id,
                  componentCollection.values().size(),
                  componentCollection.values());
        }
        // add status to result
        data.setImport(searchComponent.getCollectionCache().now(),
            searchComponent.getCollectionCache().check(componentCollection.id));
      } else if (componentCollection.action()
          .equals(ComponentCollection.ACTION_DELETE)) {
        searchComponent.getCollectionCache().deleteById(componentCollection.id);
      }
      return data;
    } else {
      throw new IOException("no componentCollection available");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * mtas.solr.handler.component.util.MtasSolrComponent#finishStage(org.apache.
   * solr.handler.component.ResponseBuilder)
   */
  public void finishStage(ResponseBuilder rb) {
    // System.out.println(
    // "collection: " + System.nanoTime() + " - " +
    // Thread.currentThread().getId()
    // + " - " + rb.req.getParams().getBool("isShard", false)
    // + " FINISHSTAGE " + rb.stage + " " + rb.req.getParamString());
    if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
      if (rb.stage >= ResponseBuilder.STAGE_EXECUTE_QUERY
          && rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
        ComponentFields mtasFields = getMtasFields(rb);
        if (mtasFields.doCollection) {
          if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
            // mtas response
            NamedList<Object> mtasResponse = null;
            try {
              mtasResponse = (NamedList<Object>) rb.rsp.getValues().get("mtas");
            } catch (ClassCastException e) {
              log.debug(e);
              mtasResponse = null;
            }
            if (mtasResponse == null) {
              mtasResponse = new SimpleOrderedMap<>();
              rb.rsp.add("mtas", mtasResponse);
            }
            ArrayList<Object> mtasCollectionResponses;
            if (mtasResponse.get("collection") != null
                && mtasResponse.get("collection") instanceof ArrayList) {
              mtasCollectionResponses = (ArrayList<Object>) mtasResponse
                  .get("collection");
            } else {
              mtasCollectionResponses = new ArrayList<>();
              mtasResponse.add("collection", mtasCollectionResponses);
            }
            MtasSolrCollectionResult collectionResult;
            for (ComponentCollection componentCollection : mtasFields.collection) {
              try {
                collectionResult = createMtasSolrCollectionResult(
                    componentCollection, false);
                // Create response
                SimpleOrderedMap<Object> mtasCollectionResponse = new SimpleOrderedMap<>();
                mtasCollectionResponse.add("key", componentCollection.key);
                mtasCollectionResponse.add("data", collectionResult);
                mtasCollectionResponses.add(mtasCollectionResponse);
              } catch (IOException e) {
                log.debug(e);
              }
            }
          }
          // decode shard responses
          for (ShardRequest sreq : rb.finished) {
            if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
                && sreq.params.getBool(PARAM_MTAS_COLLECTION, false)) {
              for (ShardResponse shardResponse : sreq.responses) {
                NamedList<Object> solrShardResponse = shardResponse
                    .getSolrResponse().getResponse();
                try {
                  ArrayList<SimpleOrderedMap<Object>> data = (ArrayList<SimpleOrderedMap<Object>>) solrShardResponse
                      .findRecursive("mtas", "collection");
                  if (data != null) {
                    MtasSolrResultUtil.decode(data);
                    if (rb.stage > ResponseBuilder.STAGE_EXECUTE_QUERY) {
                      ArrayList<SimpleOrderedMap<Object>> filteredData = new ArrayList<>();
                      for (SimpleOrderedMap<Object> dataItem : data) {
                        if (dataItem.get("data") != null && dataItem
                            .get("data") instanceof MtasSolrCollectionResult) {
                          MtasSolrCollectionResult collectionResult = (MtasSolrCollectionResult) dataItem
                              .get("data");
                          if (rb.stage <= MtasSolrSearchComponent.STAGE_COLLECTION_INIT) {
                            if (!collectionResult.action()
                                .equals(ComponentCollection.ACTION_CREATE)
                                && !collectionResult.action()
                                    .equals(ComponentCollection.ACTION_LIST)
                                && !collectionResult.action()
                                    .equals(ComponentCollection.ACTION_CHECK)) {
                              filteredData.add(dataItem);
                            }
                          } else if (rb.stage <= MtasSolrSearchComponent.STAGE_COLLECTION_FINISH) {
                            if (!collectionResult.action()
                                .equals(ComponentCollection.ACTION_POST)
                                && !collectionResult.action()
                                    .equals(ComponentCollection.ACTION_IMPORT)
                                && !collectionResult.action()
                                    .equals(ComponentCollection.ACTION_CHECK)) {
                              filteredData.add(dataItem);
                            }
                          }
                        } else {
                          filteredData.add(dataItem);
                        }
                      }
                      data.clear();
                      data.addAll(filteredData);
                    }
                  }
                } catch (ClassCastException e) {
                  log.debug(e);
                  // shouldn't happen
                }
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
  @SuppressWarnings("unchecked")
  public void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    // System.out.println("collection: " + System.nanoTime() + " - "
    // + Thread.currentThread().getId() + " - "
    // + rb.req.getParams().getBool("isShard", false) + " DISTRIBUTEDPROCESS "
    // + rb.stage + " " + rb.req.getParamString());
    NamedList<Object> mtasResponse = null;
    try {
      mtasResponse = (NamedList<Object>) rb.rsp.getValues().get("mtas");
    } catch (ClassCastException e) {
      log.debug(e);
      mtasResponse = null;
    }
    if (mtasResponse != null) {
      if (rb.stage == MtasSolrSearchComponent.STAGE_COLLECTION_INIT) {
        // build index
        Map<String, MtasSolrCollectionResult> index = new HashMap<>();
        ArrayList<Object> mtasResponseCollection;
        try {
          mtasResponseCollection = (ArrayList<Object>) mtasResponse
              .get("collection");
          for (Object item : mtasResponseCollection) {
            if (item instanceof SimpleOrderedMap) {
              SimpleOrderedMap<Object> itemMap = (SimpleOrderedMap<Object>) item;
              if (itemMap.get("data") != null
                  && itemMap.get("data") instanceof MtasSolrCollectionResult) {
                MtasSolrCollectionResult collectionItem = (MtasSolrCollectionResult) itemMap
                    .get("data");
                index.put(collectionItem.id(), collectionItem);
              }
            }
          }
        } catch (ClassCastException e) {
          log.debug(e);
          mtasResponse.remove("collection");
        }
        // check and remove previous responses
        Map<String, Set<String>> createPostAfterMissingCheckResult = new HashMap<>();
        for (ShardRequest sreq : rb.finished) {
          if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
              && sreq.params.getBool(PARAM_MTAS_COLLECTION, false)) {
            for (ShardResponse shardResponse : sreq.responses) {
              NamedList<Object> solrShardResponse = shardResponse
                  .getSolrResponse().getResponse();
              try {
                ArrayList<SimpleOrderedMap<Object>> data = (ArrayList<SimpleOrderedMap<Object>>) solrShardResponse
                    .findRecursive("mtas", "collection");
                if (data != null) {
                  for (SimpleOrderedMap<Object> dataItem : data) {
                    if (dataItem.get("data") != null && dataItem
                        .get("data") instanceof MtasSolrCollectionResult) {
                      MtasSolrCollectionResult dataItemResult = (MtasSolrCollectionResult) dataItem
                          .get("data");
                      if (index.containsKey(dataItemResult.id())
                          && index.get(dataItemResult.id()).action()
                              .equals(ComponentCollection.ACTION_CHECK)) {
                        if (dataItemResult.status == null) {
                          if (!createPostAfterMissingCheckResult
                              .containsKey(shardResponse.getShard())) {
                            createPostAfterMissingCheckResult
                                .put(shardResponse.getShard(), new HashSet<>());
                          }
                          createPostAfterMissingCheckResult
                              .get(shardResponse.getShard())
                              .add(dataItemResult.id());
                        }
                      }
                    }
                  }
                  data.clear();
                }
              } catch (ClassCastException e) {
                log.debug(e);
                // shouldn't happen
              }
            }
          }
        }
        // construct new requests
        HashMap<String, ModifiableSolrParams> requestParamList = new HashMap<>();
        int id = 0;
        for (ComponentCollection componentCollection : mtasFields.collection) {
          if (componentCollection.action()
              .equals(ComponentCollection.ACTION_CHECK)) {
            for (String shardAddress : rb.shards) {
              if (createPostAfterMissingCheckResult.containsKey(shardAddress)) {
                if (createPostAfterMissingCheckResult.get(shardAddress)
                    .contains(componentCollection.id)) {
                  HashSet<String> values = searchComponent.getCollectionCache()
                      .getDataById(componentCollection.id);
                  if (values != null) {
                    ModifiableSolrParams paramsNewRequest;
                    if (!requestParamList.containsKey(shardAddress)) {
                      paramsNewRequest = new ModifiableSolrParams();
                      requestParamList.put(shardAddress, paramsNewRequest);
                    } else {
                      paramsNewRequest = requestParamList.get(shardAddress);
                    }
                    paramsNewRequest.add(
                        PARAM_MTAS_COLLECTION + "." + id + "."
                            + NAME_MTAS_COLLECTION_KEY,
                        componentCollection.key);
                    paramsNewRequest.add(PARAM_MTAS_COLLECTION + "." + id + "."
                        + NAME_MTAS_COLLECTION_ID, componentCollection.id);
                    paramsNewRequest.add(
                        PARAM_MTAS_COLLECTION + "." + id + "."
                            + NAME_MTAS_COLLECTION_ACTION,
                        ComponentCollection.ACTION_POST);
                    paramsNewRequest.add(
                        PARAM_MTAS_COLLECTION + "." + id + "."
                            + NAME_MTAS_COLLECTION_POST,
                        stringValuesToString(values));
                    id++;
                  }
                }
              }
            }
          } else if (componentCollection.action()
              .equals(ComponentCollection.ACTION_CREATE)) {
            if (componentCollection.version == null) {
              componentCollection.version = searchComponent.getCollectionCache()
                  .create(componentCollection.id,
                      componentCollection.values().size(),
                      componentCollection.values());
            }
            if (index.containsKey(componentCollection.id)) {
              index.get(componentCollection.id).setCreate(
                  searchComponent.getCollectionCache().now(), searchComponent
                      .getCollectionCache().check(componentCollection.id));
            }
            for (String shardAddress : rb.shards) {
              ModifiableSolrParams paramsNewRequest;
              if (!requestParamList.containsKey(shardAddress)) {
                paramsNewRequest = new ModifiableSolrParams();
                requestParamList.put(shardAddress, paramsNewRequest);
              } else {
                paramsNewRequest = requestParamList.get(shardAddress);
              }
              paramsNewRequest.add(PARAM_MTAS_COLLECTION + "." + id + "."
                  + NAME_MTAS_COLLECTION_KEY, componentCollection.key);
              paramsNewRequest.add(PARAM_MTAS_COLLECTION + "." + id + "."
                  + NAME_MTAS_COLLECTION_ID, componentCollection.id);
              paramsNewRequest.add(
                  PARAM_MTAS_COLLECTION + "." + id + "."
                      + NAME_MTAS_COLLECTION_ACTION,
                  ComponentCollection.ACTION_POST);
              paramsNewRequest.add(
                  PARAM_MTAS_COLLECTION + "." + id + "."
                      + NAME_MTAS_COLLECTION_POST,
                  stringValuesToString(componentCollection.values()));
            }
          }
          id++;
        }
        // add new requests
        for (Entry<String, ModifiableSolrParams> entry : requestParamList
            .entrySet()) {
          ShardRequest newSreq = new ShardRequest();
          newSreq.shards = new String[] { entry.getKey() };
          newSreq.purpose = ShardRequest.PURPOSE_PRIVATE;
          newSreq.params = entry.getValue();
          newSreq.params.add("q", "*");
          newSreq.params.add("rows", "0");
          newSreq.params.add(MtasSolrSearchComponent.PARAM_MTAS,
              rb.req.getOriginalParams()
                  .getParams(MtasSolrSearchComponent.PARAM_MTAS));
          newSreq.params.add(PARAM_MTAS_COLLECTION,
              rb.req.getOriginalParams().getParams(PARAM_MTAS_COLLECTION));
          rb.addRequest(searchComponent, newSreq);
        }
      } else if (rb.stage == MtasSolrSearchComponent.STAGE_COLLECTION_FINISH) {
        // just rewrite
        ArrayList<Object> mtasResponseCollection;
        try {
          mtasResponseCollection = (ArrayList<Object>) mtasResponse
              .get("collection");
          if (mtasResponseCollection != null) {
            MtasSolrResultUtil.rewrite(mtasResponseCollection, searchComponent);
          }
        } catch (ClassCastException e) {
          log.debug(e);
          mtasResponse.remove("collection");
        }
      }
    }
  }

  /**
   * Gets the mtas fields.
   *
   * @param rb the rb
   * @return the mtas fields
   */
  private ComponentFields getMtasFields(ResponseBuilder rb) {
    return (ComponentFields) rb.req.getContext().get(ComponentFields.class);
  }

  /**
   * String values to string.
   *
   * @param stringValues the string values
   * @return the string
   */
  private static String stringValuesToString(HashSet<String> stringValues) {
    return JSONUtil.toJSON(stringValues);
  }

  /**
   * String to string values.
   *
   * @param stringValue the string value
   * @return the hash set
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static HashSet<String> stringToStringValues(String stringValue)
      throws IOException {
    // should be improved to support escaped characters
    HashSet<String> stringValues = new HashSet<>();
    JSONParser jsonParser = new JSONParser(stringValue);
    int event = jsonParser.nextEvent();
    if (event == JSONParser.ARRAY_START) {
      while ((event = jsonParser.nextEvent()) != JSONParser.ARRAY_END) {
        if (jsonParser.getLevel() == 1) {
          switch (event) {
          case JSONParser.STRING:
            stringValues.add(jsonParser.getString());
            break;
          case JSONParser.BIGNUMBER:
          case JSONParser.NUMBER:
          case JSONParser.LONG:
            stringValues.add(jsonParser.getNumberChars().toString());
            break;
          case JSONParser.BOOLEAN:
            stringValues.add(Boolean.toString(jsonParser.getBoolean()));
            break;
          default:
            // do nothing
          }
        }
      }
    } else {
      throw new IOException("unsupported json structure");
    }
    return stringValues;
  }

}
