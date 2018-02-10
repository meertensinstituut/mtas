package mtas.solr.handler.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * The Class MtasSolrList.
 */
public abstract class MtasSolrBaseList {

  /** The list. */
  protected List<MtasSolrStatus> list;

  /** The index. */
  protected Map<String, MtasSolrStatus> index;

  /** The enabled. */
  private boolean enabled = true;

  /** The Constant NAME_ENABLED. */
  private final static String NAME_ENABLED = "enabled";

  /** The Constant NAME_LIST. */
  private final static String NAME_LIST = "list";

  /** The Constant NAME_SIZE. */
  private final static String NAME_SIZE = "size";

  public MtasSolrBaseList() {
    list = Collections.synchronizedList(new ArrayList<MtasSolrStatus>());
    index = Collections.synchronizedMap(new HashMap<>());
  }

  /**
   * Gets the.
   *
   * @param key the key
   * @return the mtas solr status
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public final MtasSolrStatus get(String key) throws IOException {
    return index.get(Objects.requireNonNull(key, "no key provided"));
  }

  /**
   * Adds the.
   *
   * @param status the status
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void add(MtasSolrStatus status) throws IOException {
    Objects.requireNonNull(status);
    if (enabled) {
      list.add(status);
      if (!index.containsKey(status.key())) {
        index.put(status.key(), status);
        garbageCollect();
      } else {
        garbageCollect();
        //retry
        if (!index.containsKey(status.key())) {
          index.put(status.key(), status);
        } else {          
          throw new IOException("key "+status.key()+" already exists in");
        }          
      }            
    }
  }

  /**
   * Removes the.
   *
   * @param status the status
   */
  public final void remove(MtasSolrStatus status) {
    Objects.requireNonNull(status);
    list.remove(status);
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
    list.clear();
    index.clear();
  }

  /**
   * Update key.
   *
   * @param key the key
   * @throws IOException Signals that an I/O exception has occurred.
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
   * @param flag the new enabled
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
   * Creates the output.
   *
   * @param shardRequests the shard requests
   * @return the simple ordered map
   */
  public SimpleOrderedMap<Object> createOutput(boolean shardRequests) {
    garbageCollect();
    SimpleOrderedMap<Object> output = new SimpleOrderedMap<>();
    output.add(NAME_ENABLED, enabled());
    output.add(NAME_ENABLED, true);
    int n = list.size();
    output.add(NAME_SIZE, n);
    if (n > 0) {
      // create list
      List<SimpleOrderedMap<Object>> outputList = new ArrayList<>();
      list.stream()
          .collect(Collectors.collectingAndThen(Collectors.toList(), lst -> {
            Collections.reverse(lst);
            return lst.stream();
          })).forEach((item) -> {
            if (shardRequests || !item.shardRequest()) {
              outputList.add(item.createOutput());
            }
          });
      output.add(NAME_LIST, outputList);
    }
    return output;
  }

}
