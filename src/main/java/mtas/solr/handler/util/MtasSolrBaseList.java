package mtas.solr.handler.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import org.apache.solr.common.util.SimpleOrderedMap;

// TODO: Auto-generated Javadoc
/**
 * The Class MtasSolrBaseList.
 */
public abstract class MtasSolrBaseList {

  /** The list. */
  protected List<MtasSolrStatus> data;
  
  /** The index. */
  protected Map<String, MtasSolrStatus> index;

  /** The enabled. */
  private boolean enabled = true;

  /** The Constant NAME_ENABLED. */
  private final static String NAME_ENABLED = "enabled";

  /** The Constant NAME_LIST. */
  private final static String NAME_LIST = "list";

  /** The Constant NAME_SIZE_TOTAL. */
  private final static String NAME_SIZE_TOTAL = "sizeTotal";

  /** The Constant NAME_SIZE_NORMAL. */
  private final static String NAME_SIZE_NORMAL = "sizeNormal";

  /** The Constant NAME_SIZE_SHARDREQUESTS. */
  private final static String NAME_SIZE_SHARDREQUESTS = "sizeShardRequests";

  /**
   * Instantiates a new mtas solr base list.
   */
  public MtasSolrBaseList() {
    data = Collections.synchronizedList(new ArrayList<MtasSolrStatus>());
    index = Collections.synchronizedMap(new HashMap<>());
  }

  /**
   * Gets the.
   *
   * @param key
   *          the key
   * @return the mtas solr status
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public final MtasSolrStatus get(String key) throws IOException {
    return index.get(Objects.requireNonNull(key, "no key provided"));
  }

  /**
   * Adds the.
   *
   * @param status
   *          the status
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void add(MtasSolrStatus status) throws IOException {
    Objects.requireNonNull(status);
    if (enabled) {
      data.add(status);
      if (!index.containsKey(status.key())) {
        index.put(status.key(), status);
        garbageCollect();
      } else {
        garbageCollect();
        // retry
        MtasSolrStatus oldStatus = index.get(status.key());
        if (oldStatus == null) {
          index.put(status.key(), status);
        } else if (oldStatus.finished()) {
          remove(oldStatus);
          index.put(status.key(), status);
        } else {
          throw new IOException("key " + status.key() + " already exists");
        }
      }
    }
  }

  /**
   * Removes the.
   *
   * @param status
   *          the status
   */
  public final void remove(MtasSolrStatus status) {
    Objects.requireNonNull(status);
    data.remove(status);
    index.remove(status.key());
  }

  /**
   * Garbage collect.
   */
  public abstract void garbageCollect();

  /**
   * Reset.
   */
  public final void reset() {
    data.clear();
    index.clear();
  }

  /**
   * Update key.
   *
   * @param key
   *          the key
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public final void updateKey(String key) throws IOException {
    Objects.requireNonNull(key, "old key required");
    if (index.containsKey(key)) {
      MtasSolrStatus status = index.get(key);
      index.remove(key);
      if (!index.containsKey(status.key())) {
        index.put(status.key(), status);
      } else {
        throw new IOException("key already exists");
      }
    }
  }

  /**
   * Sets the enabled.
   *
   * @param flag
   *          the new enabled
   */
  public final void setEnabled(boolean flag) {
    if (enabled != flag) {
      reset();
      enabled = flag;
    }
  }

  /**
   * Enabled.
   *
   * @return true, if successful
   */
  public final boolean enabled() {
    return enabled;
  }

  /**
   * Creates the list output.
   *
   * @param shardRequests          the shard requests
   * @param maxNumber the max number
   * @return the simple ordered map
   */
  public SimpleOrderedMap<Object> createListOutput(boolean shardRequests, int maxNumber) {
    garbageCollect();
    SimpleOrderedMap<Object> output = new SimpleOrderedMap<>();
    output.add(NAME_ENABLED, enabled());
    output.add(NAME_ENABLED, true);
    int numberTotal = data.size();
    ListData listData = new ListData();

    synchronized (data) {
      ListIterator<MtasSolrStatus> iter = data.listIterator(data.size());
      MtasSolrStatus item;
      int number = 0;
      while (iter.hasPrevious() && number < maxNumber) {
        item = iter.previous();
        if (item.shardRequest()) {
          listData.addShardRequest();
          if (shardRequests) {
            listData.outputList.add(item.createItemOutput());
            number++;
          }
        } else {
          listData.addNormal();
          listData.outputList.add(item.createItemOutput());
          number++;
        }
      }
    }
    output.add(NAME_SIZE_TOTAL, numberTotal);
    output.add(NAME_SIZE_NORMAL, listData.numberNormal);
    output.add(NAME_SIZE_SHARDREQUESTS, listData.numberShardRequests);
    output.add(NAME_LIST, listData.outputList);
    return output;
  }

  /**
   * The Class ListData.
   */
  static class ListData {

    /** The output list. */
    private List<SimpleOrderedMap<Object>> outputList;

    /** The number normal. */
    private int numberNormal;

    /** The number shard requests. */
    private int numberShardRequests;

    /**
     * Instantiates a new list data.
     */
    public ListData() {
      outputList = new ArrayList<>();
      numberNormal = 0;
      numberShardRequests = 0;
    }

    /**
     * Adds the normal.
     */
    public void addNormal() {
      numberNormal++;
    }

    /**
     * Adds the shard request.
     */
    public void addShardRequest() {
      numberShardRequests++;
    }

  }

}
