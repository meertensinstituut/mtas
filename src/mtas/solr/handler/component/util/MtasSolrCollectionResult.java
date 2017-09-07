package mtas.solr.handler.component.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.solr.common.util.SimpleOrderedMap;

import mtas.codec.util.CodecComponent.ComponentCollection;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrCollectionResult.
 */
public class MtasSolrCollectionResult implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The values. */
  private HashSet<String> values;

  /** The id. */
  private String id;

  /** The action. */
  private String action;

  /** The now. */
  private Long now;

  /** The list. */
  private List<SimpleOrderedMap<Object>> list;

  /** The status. */
  public SimpleOrderedMap<Object> status;

  /** The component collection. */
  private transient ComponentCollection componentCollection = null;

  /**
   * Instantiates a new mtas solr collection result.
   *
   * @param componentCollection the component collection
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasSolrCollectionResult(ComponentCollection componentCollection)
      throws IOException {
    this.componentCollection = componentCollection;
    if (componentCollection != null) {
      action = componentCollection.action();
      id = null;
      values = null;
      now = null;
      list = null;
      switch (action) {
      case ComponentCollection.ACTION_CREATE:
        values = componentCollection.values();
        id = componentCollection.id;
        break;
      case ComponentCollection.ACTION_CHECK:
      case ComponentCollection.ACTION_GET:
      case ComponentCollection.ACTION_DELETE:
        id = componentCollection.id;
        break;
      case ComponentCollection.ACTION_POST:
      case ComponentCollection.ACTION_IMPORT:
        id = componentCollection.id;
        values = componentCollection.values();
        break;
      case ComponentCollection.ACTION_LIST:
      case ComponentCollection.ACTION_EMPTY:
        // do nothing
        break;
      default:
        throw new IOException("action " + action + " not allowed");
      }
    } else {
      throw new IOException("no componentCollection available");
    }
  }

  /**
   * Sets the list.
   *
   * @param now the now
   * @param list the list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void setList(long now, List<SimpleOrderedMap<Object>> list)
      throws IOException {
    if (action.equals(ComponentCollection.ACTION_LIST)) {
      this.now = now;
      this.list = list;
    } else {
      throw new IOException("not allowed with action '" + action + "'");
    }
  }

  /**
   * Sets the check.
   *
   * @param now the now
   * @param status the status
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void setCheck(long now, SimpleOrderedMap<Object> status)
      throws IOException {
    if (action.equals(ComponentCollection.ACTION_CHECK)) {
      this.now = now;
      this.status = status;
    } else {
      throw new IOException("not allowed with action '" + action + "'");
    }
  }

  /**
   * Sets the get.
   *
   * @param now the now
   * @param status the status
   * @param stringValues the string values
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void setGet(long now, SimpleOrderedMap<Object> status,
      HashSet<String> stringValues) throws IOException {
    if (action.equals(ComponentCollection.ACTION_GET)) {
      this.now = now;
      this.status = status;
      this.values = stringValues;
    } else {
      throw new IOException("not allowed with action '" + action + "'");
    }
  }

  /**
   * Sets the post.
   *
   * @param now the now
   * @param status the status
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void setPost(long now, SimpleOrderedMap<Object> status)
      throws IOException {
    if (action.equals(ComponentCollection.ACTION_POST)) {
      this.now = now;
      this.status = status;
    } else {
      throw new IOException("not allowed with action '" + action + "'");
    }
  }
  
  public void setImport(long now, SimpleOrderedMap<Object> status)
      throws IOException {
    if (action.equals(ComponentCollection.ACTION_IMPORT)) {
      this.now = now;
      this.status = status;
    } else {
      throw new IOException("not allowed with action '" + action + "'");
    }
  }

  /**
   * Sets the create.
   *
   * @param now the now
   * @param status the status
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void setCreate(long now, SimpleOrderedMap<Object> status)
      throws IOException {
    if (action.equals(ComponentCollection.ACTION_CREATE)) {
      this.now = now;
      this.status = status;
    } else {
      throw new IOException("not allowed with action '" + action + "'");
    }
  }

  /**
   * Id.
   *
   * @return the string
   */
  public String id() {
    return id;
  }

  /**
   * Action.
   *
   * @return the string
   */
  public String action() {
    return action;
  }

  /**
   * Rewrite.
   *
   * @param searchComponent the search component
   * @return the simple ordered map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public SimpleOrderedMap<Object> rewrite(
      MtasSolrSearchComponent searchComponent) throws IOException {
    SimpleOrderedMap<Object> response = new SimpleOrderedMap<>();
    Iterator<Entry<String, Object>> it;
    switch (action) {
    case ComponentCollection.ACTION_LIST:
      response.add("now", now);
      response.add("list", list);
      break;
    case ComponentCollection.ACTION_CREATE:
    case ComponentCollection.ACTION_POST:
    case ComponentCollection.ACTION_IMPORT:      
        if (componentCollection != null && status != null) {
        it = status.iterator();
        while (it.hasNext()) {
          Entry<String, Object> entry = it.next();
          response.add(entry.getKey(), entry.getValue());
        }
      }
      break;
    case ComponentCollection.ACTION_CHECK:
      if (status != null) {
        it = status.iterator();
        while (it.hasNext()) {
          Entry<String, Object> entry = it.next();
          response.add(entry.getKey(), entry.getValue());
        }
      }
      break;
    case ComponentCollection.ACTION_GET:
      if (status != null) {
        it = status.iterator();
        while (it.hasNext()) {
          Entry<String, Object> entry = it.next();
          response.add(entry.getKey(), entry.getValue());
        }
      }
      if (values != null) {
        response.add("values", values);
      }
      break;
    default:
      break;
    }
    return response;
  }

  /**
   * Merge.
   *
   * @param newItem the new item
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void merge(MtasSolrCollectionResult newItem) throws IOException {
    if (action != null && newItem.action != null) {
      if (action.equals(ComponentCollection.ACTION_CREATE)
          && newItem.action.equals(ComponentCollection.ACTION_CREATE)) {
        values.addAll(newItem.values);
        if (id != null && (newItem.id == null || !newItem.id.equals(id))) {
          id = null;
        }
      } else if (action.equals(ComponentCollection.ACTION_LIST)) {
        if (list != null) {
          HashMap<String, SimpleOrderedMap<Object>> index = new HashMap<>();
          for (SimpleOrderedMap<Object> item : list) {
            if (item.get("id") != null && item.get("id") instanceof String) {
              index.put((String) item.get("id"), item);
              if (item.get("shards") == null
                  || !(item.get("shards") instanceof List)) {
                item.add("shards", new ArrayList<>());
              }
            }
          }
          for (SimpleOrderedMap<Object> item : newItem.list) {
            if (item.get("id") != null && item.get("id") instanceof String) {
              String id = (String) item.get("id");
              if (index.containsKey(id)) {
                SimpleOrderedMap<Object> indexItem = index.get(id);
                List<SimpleOrderedMap<Object>> shards;
                if (indexItem.get("shards") != null
                    && indexItem.get("shards") instanceof List) {
                  shards = (List<SimpleOrderedMap<Object>>) indexItem
                      .get("shards");
                } else {
                  shards = new ArrayList<>();
                  indexItem.add("shards", shards);
                }
                shards.add(item);
              }
            }
          }
        }
      } else if (action.equals(ComponentCollection.ACTION_CHECK)
          || action.equals(ComponentCollection.ACTION_POST)
          || action.equals(ComponentCollection.ACTION_IMPORT)
          || action.equals(ComponentCollection.ACTION_CREATE)
          || action.equals(ComponentCollection.ACTION_GET)) {
        if (status != null && status.get("id") != null
            && status.get("id") instanceof String) {
          String id = (String) status.get("id");
          if (id.equals(newItem.id)) {
            List<SimpleOrderedMap<Object>> shards;
            if (status.get("shards") != null
                && status.get("shards") instanceof List) {
              shards = (List<SimpleOrderedMap<Object>>) status.get("shards");
            } else {
              shards = new ArrayList<>();
              status.add("shards", shards);
            }
            if (newItem.status != null) {
              if (action.equals(ComponentCollection.ACTION_GET)) {
                newItem.status.add("values", newItem.values);
              }
              shards.add(newItem.status);
            }
          }
        }
      } else {
        throw new IOException("not allowed for action '" + action + "'");
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder text = new StringBuilder("");
    text.append(MtasSolrCollectionResult.class.getSimpleName() + "[");
    text.append(action + ", ");
    text.append(id + ", ");
    if (componentCollection != null) {
      text.append(componentCollection.version + ", ");
    } else if (status != null) {
      text.append(status.get("version") + ", ");
    } else {
      text.append("null, ");
    }
    text.append((values != null) ? values.size() : "null");
    text.append("]");
    return text.toString();
  }

}
