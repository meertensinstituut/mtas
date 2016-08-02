package mtas.solr.handler.component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Map.Entry;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.search.SolrIndexSearcher;

import mtas.solr.handler.component.MtasSolrSearchComponent.ComponentSortSelect;

public class MtasMergeStrategy implements MergeStrategy {

  @Override
  public void merge(ResponseBuilder rb, ShardRequest sreq) {

    if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
      // mtas response
      NamedList<Object> mtasResponse = null;
      try {
        mtasResponse = (NamedList<Object>) rb.rsp.getValues().get("mtas");
      } catch (ClassCastException e) {
        mtasResponse = null;
      }
      if (mtasResponse == null) {
        mtasResponse = new SimpleOrderedMap<>();
        rb.rsp.add("mtas", mtasResponse);
      }
      if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
        // merge stats
        if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS_STATS,
            false)) {
          mergeNamedList(sreq, mtasResponse, "stats", null);
        }
        // merge group
        if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS_GROUP,
            false)) {
          mergeArrayList(sreq, mtasResponse, "group", null, false);
        }
        // merge facet
        if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS_FACET,
            false)) {
          mergeArrayList(sreq, mtasResponse, "facet", null, false);
        }
        // merge prefix
        if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS_PREFIX,
            false)) {
          mergeArrayList(sreq, mtasResponse, "prefix", null, false);
        }
      } else if (rb.stage == MtasSolrSearchComponent.STAGE_TERMVECTOR_MISSING_TOP) {
        // merge termvector
        if (rb.req.getParams()
            .getBool(MtasSolrSearchComponent.PARAM_MTAS_TERMVECTOR, false)) {
          mergeArrayList(sreq, mtasResponse, "termvector",
              ShardRequest.PURPOSE_PRIVATE, true);
        }
      } else if (rb.stage == MtasSolrSearchComponent.STAGE_LIST) {
        // merge list
        if (rb.req.getParams()
            .getBool(MtasSolrSearchComponent.PARAM_MTAS_LIST, false)) {
          mergeArrayList(sreq, mtasResponse, "list",
              ShardRequest.PURPOSE_PRIVATE, true);
        }
      } else if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
         // merge kwic
        if (rb.req.getParams()
            .getBool(MtasSolrSearchComponent.PARAM_MTAS_KWIC, false)) {
          mergeArrayList(sreq, mtasResponse, "kwic",
              ShardRequest.PURPOSE_PRIVATE, true);
        }
      }
    }
  }

  @Override
  public boolean mergesIds() {
    return false;
  }

  @Override
  public boolean handlesMergeFields() {
    return false;
  }

  @Override
  public void handleMergeFields(ResponseBuilder rb,
      SolrIndexSearcher searcher) {
  }

  @Override
  public int getCost() {
    return 0;
  }

  private void mergeNamedList(ShardRequest sreq,
      NamedList<Object> mtasResponse, String key, Integer preferredPurpose) {
    // create new response for key
    NamedList<Object> mtasListResponse;
    Object o = mtasResponse.get(key);
    if(o instanceof NamedList) {
      mtasListResponse = (NamedList<Object>) o;
    } else {
      mtasListResponse = new SimpleOrderedMap<>();
      mtasResponse.removeAll(key);
      mtasResponse.add(key, mtasListResponse);
    }  
    // collect responses for each shard
    HashMap<String, NamedList<Object>> mtasListShardResponses = new HashMap<String, NamedList<Object>>();
    for (ShardResponse response : sreq.responses) {
      // only continue if new shard or preferred purpose
      if (mtasListShardResponses.containsKey(response.getShard())
          && ((preferredPurpose == null)
              || (sreq.purpose != preferredPurpose))) {
        break;
      }
      // update
      try {
        NamedList<Object> result = response.getSolrResponse().getResponse();
        NamedList<Object> data = (NamedList<Object>) result
            .findRecursive("mtas", key);
        if (data != null) {
          mtasListShardResponses.put(response.getShard(), decode(data));
        }
      } catch (ClassCastException e) {

      }
    }
    try {
      for (NamedList<Object> mtasListShardResponse : mtasListShardResponses
          .values()) {
        mergeResponsesNamedList(mtasListResponse, mtasListShardResponse);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void mergeArrayList(ShardRequest sreq,
      NamedList<Object> mtasResponse, String key, Integer preferredPurpose,
      boolean mergeAllShardResponses) {
    // create new response for key
    ArrayList<Object> mtasListResponse;
    Object o = mtasResponse.get(key);
    if(o instanceof ArrayList) {
      mtasListResponse = (ArrayList<Object>) o;
    } else {
      mtasListResponse = new ArrayList<Object>();
      mtasResponse.removeAll(key);
      mtasResponse.add(key, mtasListResponse);
    }      
    // collect responses for each shard
    HashMap<String, ArrayList<Object>> mtasListShardResponses = new HashMap<String, ArrayList<Object>>();
    ArrayList<ArrayList<Object>> mtasListShardResponsesExtra = new ArrayList<ArrayList<Object>>();
    for (ShardResponse response : sreq.responses) {
      // only continue if new shard or preferred purpose
      if (mtasListShardResponses.containsKey(response.getShard())
          && ((preferredPurpose == null)
              || (sreq.purpose != preferredPurpose))) {
        break;
      }
      // update
      try {
        NamedList<Object> result = response.getSolrResponse().getResponse();
        ArrayList<Object> data = (ArrayList<Object>) result
            .findRecursive("mtas", key);
        if (data != null) {
          if (mtasListShardResponses.containsKey(response.getShardAddress())) {
            if (mergeAllShardResponses) {
              mtasListShardResponsesExtra.add(decode(data));
            }
          } else {
            mtasListShardResponses.put(response.getShardAddress(),
                decode(data));
          }
        }
      } catch (ClassCastException e) {

      }
    }

    try {
      for (ArrayList<Object> mtasListShardResponse : mtasListShardResponses
          .values()) {
        mergeResponsesArrayList(mtasListResponse, mtasListShardResponse);
      }
      for (ArrayList<Object> mtasListShardResponse : mtasListShardResponsesExtra) {
        mergeResponsesArrayList(mtasListResponse, mtasListShardResponse);
      }
    } catch (IOException e) {
      mtasListResponse.add(e.getMessage());
    }
  }

  String encode(Object o) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream;
    try {
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(o);
      objectOutputStream.close();
      return Base64.byteArrayToBase64(byteArrayOutputStream.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private Object decode(String s) {
    byte[] bytes = Base64.base64ToByteArray(s);
    ObjectInputStream objectInputStream;
    try {
      objectInputStream = new ObjectInputStream(
          new ByteArrayInputStream(bytes));
      return objectInputStream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }

  ArrayList decode(ArrayList l) {
    for (int i = 0; i < l.size(); i++) {
      if (l.get(i) instanceof NamedList) {
        l.set(i, decode((NamedList) l.get(i)));
      } else if (l.get(i) instanceof ArrayList) {
        l.set(i, decode((ArrayList) l.get(i)));
      }
    }
    return l;
  }

  private NamedList<Object> decode(NamedList<Object> nl) {
    for (int i = 0; i < nl.size(); i++) {
      String key = nl.getName(i);
      Object o = nl.getVal(i);
      if (key.matches("^_encoded_.*$")) {
        if (o instanceof String) {
          Object decodedObject = decode((String) nl.getVal(i));
          String decodedKey = key.replaceFirst("^_encoded_", "");
          if (decodedKey.equals("")) {
            decodedKey = "_" + decodedObject.getClass().getSimpleName() + "_";
          }
          nl.setName(i, decodedKey);
          nl.setVal(i, decodedObject);
        } else if (o instanceof NamedList) {
          NamedList nl2 = (NamedList) o;
          for (int j = 0; j < nl2.size(); j++) {
            if (nl2.getVal(j) instanceof String) {
              nl2.setVal(j, decode((String) nl2.getVal(j)));
            }
          }
        } else {
          System.out.println("unknown type " + o.getClass().getCanonicalName());
        }
      } else {
        if (o instanceof NamedList) {
          nl.setVal(i, decode((NamedList<Object>) o));
        } else if (o instanceof ArrayList) {
          nl.setVal(i, decode((ArrayList<Object>) o));
        }
      }
    }
    return nl;
  }


  private void mergeResponsesTreeSet(TreeSet<Object> originalList,
      TreeSet<Object> shardList) {
    for (Object item : shardList) {
      originalList.add(item);
    }
  }

  private void mergeResponsesArrayList(ArrayList<Object> originalList,
      ArrayList<Object> shardList) throws IOException {
    // get keys from original
    HashMap<String, Object> originalKeyList = new HashMap<String, Object>();
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
              mergeResponsesNamedList((NamedList<Object>) originalItem,
                  (NamedList<Object>) item);
            } else {
              // ignore?
            }
            // add
          } else {
            originalList.add(adjustablePartsCloned(item));
          }
        } else {
          originalList.add(item);
        }
      } else {
        originalList.add(item);
      }
    }
  }

  private void mergeResponsesNamedList(NamedList<Object> mainResponse,
      NamedList<Object> shardResponse) throws IOException {
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
        } else if (original.getClass().equals(shardValue.getClass())) {
          // merge ArrayList
          if (original instanceof ArrayList) {
            ArrayList originalList = (ArrayList) original;
            ArrayList shardList = (ArrayList) shardValue;
            mergeResponsesArrayList(originalList, shardList);
            // merge Namedlist
          } else if (original instanceof NamedList<?>) {
            mergeResponsesNamedList((NamedList<Object>) original,
                (NamedList<Object>) shardValue);
            // merge TreeSet
          } else if (original instanceof TreeSet<?>) {
            mergeResponsesTreeSet((TreeSet<Object>) original,
                (TreeSet<Object>) shardValue);
          } else if (original instanceof ComponentSortSelect) {
            ComponentSortSelect originalComponentSortSelect = (ComponentSortSelect) original;
            originalComponentSortSelect.merge((ComponentSortSelect) shardValue);
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

  private Object adjustablePartsCloned(Object original) {
    if (original instanceof NamedList) {
      NamedList<Object> newObject = new SimpleOrderedMap();
      NamedList<Object> originalObject = (NamedList<Object>) original;
      for (int i = 0; i < originalObject.size(); i++) {
        newObject.add(originalObject.getName(i),
            adjustablePartsCloned(originalObject.getVal(i)));
      }
      return newObject;
    } else if (original instanceof ArrayList) {
      ArrayList<Object> newObject = new ArrayList<Object>();
      ArrayList<Object> originalObject = (ArrayList<Object>) original;
      for (int i = 0; i < originalObject.size(); i++) {
        newObject.add(adjustablePartsCloned(originalObject.get(i)));
      }
      return newObject;
    } else if (original instanceof Integer) {
      Integer originalObject = (Integer) original;
      return new Integer(originalObject.intValue());
    }
    return original;
  }

}
