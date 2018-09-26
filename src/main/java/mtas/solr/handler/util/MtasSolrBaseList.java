package mtas.solr.handler.util;

import org.apache.solr.common.util.SimpleOrderedMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;

public abstract class MtasSolrBaseList {
  protected List<MtasSolrStatus> data;
  protected Map<String, MtasSolrStatus> index;

  private boolean enabled = true;
  private final static String NAME_ENABLED = "enabled";
  private final static String NAME_LIST = "list";
  private final static String NAME_SIZE_TOTAL = "sizeTotal";
  private final static String NAME_SIZE_NORMAL = "sizeNormal";
  private final static String NAME_SIZE_SHARDREQUESTS = "sizeShardRequests";

  public MtasSolrBaseList() {
    data = Collections.synchronizedList(new ArrayList<MtasSolrStatus>());
    index = Collections.synchronizedMap(new HashMap<>());
  }

  public final MtasSolrStatus get(String key) throws IOException {
    return index.get(Objects.requireNonNull(key, "no key provided"));
  }

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

  public final void remove(MtasSolrStatus status) {
    Objects.requireNonNull(status);
    data.remove(status);
    index.remove(status.key());
  }

  public abstract void garbageCollect();

  public final void reset() {
    data.clear();
    index.clear();
  }

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

  public final void setEnabled(boolean flag) {
    if (enabled != flag) {
      reset();
      enabled = flag;
    }
  }

  public final boolean enabled() {
    return enabled;
  }

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

  static class ListData {
    private List<SimpleOrderedMap<Object>> outputList;
    private int numberNormal;
    private int numberShardRequests;

    public ListData() {
      outputList = new ArrayList<>();
      numberNormal = 0;
      numberShardRequests = 0;
    }

    public void addNormal() {
      numberNormal++;
    }

    public void addShardRequest() {
      numberShardRequests++;
    }
  }
}
