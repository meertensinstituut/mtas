package mtas.solr.search;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.Base64;

public class MtasSolrJoinCache {

  private static Log log = LogFactory.getLog(MtasSolrJoinCache.class);

  private static final long DEFAULT_LIFETIME = 86400;
  private static final int DEFAULT_MAXIMUM_NUMBER = 1000;
  private static final int DEFAULT_MAXIMUM_OVERFLOW = 10;

  private HashMap<MtasSolrJoinCacheItem, String> administration;
  private HashMap<String, MtasSolrJoinCacheItem> index;
  private HashMap<String, Long> expiration;

  private Path joinCachePath;

  private long lifeTime;
  private int maximumNumber;
  private int maximumOverflow;

  public MtasSolrJoinCache(String cacheDirectory, Long lifeTime,
      Integer maximumNumber, Integer maximumOverflow) {
    joinCachePath = null;
    this.lifeTime = (lifeTime != null && lifeTime > 0) ? lifeTime
        : DEFAULT_LIFETIME;
    this.maximumNumber = (maximumNumber != null && maximumNumber > 0)
        ? maximumNumber : DEFAULT_MAXIMUM_NUMBER;
    this.maximumOverflow = (maximumOverflow != null && maximumOverflow > 0)
        ? maximumOverflow : DEFAULT_MAXIMUM_OVERFLOW;
    if (cacheDirectory != null) {
      try {
        joinCachePath = Files.createDirectories(Paths.get(cacheDirectory));
        for (File file : joinCachePath.toFile().listFiles()) {
          if (file.isFile() && !file.delete()) {
            log.error("couldn't delete " + file);
          } else if(file.isDirectory()) {
            log.info("unexpected directory "+file.getName());
          }
        }
      } catch (IOException e) {
        joinCachePath = null;
        log.info("couldn't create cache directory " + cacheDirectory, e);
      }
    }
    administration = new HashMap<>();
    expiration = new HashMap<>();
  }

  public String create(String url, String request, Serializable data)
      throws IOException {
    MtasSolrJoinCacheItem item = new MtasSolrJoinCacheItem(url, request, null);
    return create(item, data);
  }

  private String create(MtasSolrJoinCacheItem item, Serializable data)
      throws IOException {
    // initialisation
    Date date = clear();
    delete(item);
    // create always new key
    String key;
    do {
      key = UUID.randomUUID().toString();
    } while (index.containsKey(key));
    // register
    administration.put(item, key);
    expiration.put(key, date.getTime() + lifeTime);
    index.put(key, item);
    // store data
    if (joinCachePath != null) {
      File file = joinCachePath.resolve(key).toFile();
      try {
        FileWriter writer = new FileWriter(file, false);
        writer.write(encode(data));
        writer.close();
        return key;
      } catch (IOException e) {
        administration.remove(key);
        expiration.remove(key);
        log.error("couldn't create " + key, e);
        return null;
      }
    } else {
      item.data = encode(data);
      return key;
    }
  }

  public Object get(String url, String request) throws IOException {
    MtasSolrJoinCacheItem item = new MtasSolrJoinCacheItem(url, request, null);
    if (administration.containsKey(item)) {
      return get(item);
    } else {
      return null;
    }
  }

  public Object get(String key) throws IOException {
    if (index.containsKey(key)) {
      return get(index.get(key));
    } else {
      return null;
    }
  }

  private Object get(MtasSolrJoinCacheItem item) throws IOException {
    Date date = clear();
    if (administration.containsKey(item)) {
      String key = administration.get(item);
      expiration.put(key, date.getTime() + lifeTime);
      try {
        Path path = joinCachePath.resolve(key);
        String data = new String(Files.readAllBytes(path));
        return decode(data);
      } catch (IOException e) {
        if (!joinCachePath.resolve(key).toFile().delete()) {
          log.debug("couldn't delete " + key);
        }
        administration.remove(key);
        expiration.remove(key);
        log.error("couldn't get " + key, e);
      }
    } else {
      log.error("doesn't exist anymore");
    }
    return null;
  }

  private void delete(MtasSolrJoinCacheItem item) {
    if (administration.containsKey(item)) {
      String key = administration.remove(item);
      expiration.remove(key);
      index.remove(key);
      if (joinCachePath != null
          && !joinCachePath.resolve(key).toFile().delete()) {
        log.debug("couldn't delete " + key);
      }
    }
  }

  private Date clear() {
    Date date = new Date();
    Long timestamp = date.getTime();
    HashSet<MtasSolrJoinCacheItem> toBeRemoved = new HashSet<>();
    // check expiration
    for (Entry<String, Long> entry : expiration.entrySet()) {
      if (entry.getValue() < timestamp) {
        for (Entry<MtasSolrJoinCacheItem, String> subEntry : administration
            .entrySet()) {
          if (subEntry.getValue().equals(entry.getKey())) {
            toBeRemoved.add(subEntry.getKey());
          }
        }
      }
    }
    for (MtasSolrJoinCacheItem item : toBeRemoved) {
      delete(item);
    }
    // check size
    if (expiration.size() > maximumNumber + maximumOverflow) {
      Set<Entry<String, Long>> mapEntries = expiration.entrySet();
      List<Entry<String, Long>> aList = new LinkedList<Entry<String, Long>>(
          mapEntries);
      Collections.sort(aList, new Comparator<Entry<String, Long>>() {
        @Override
        public int compare(Entry<String, Long> ele1, Entry<String, Long> ele2) {
          return ele2.getValue().compareTo(ele1.getValue());
        }
      });
      aList.subList(maximumNumber, aList.size()).clear();
      for(Entry<String,MtasSolrJoinCacheItem> entry : index.entrySet()) {
        if(!aList.contains(entry.getKey())) {
          toBeRemoved.add(entry.getValue());
        }
      }
      for (MtasSolrJoinCacheItem item : toBeRemoved) {
        delete(item);
      }
    }
    return date;
  }

  private String encode(Serializable o) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream;
    objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    objectOutputStream.writeObject(o);
    objectOutputStream.close();
    byte[] byteArray = byteArrayOutputStream.toByteArray();
    return Base64.byteArrayToBase64(byteArray);
  }

  private Object decode(String s) throws IOException {
    byte[] bytes = Base64.base64ToByteArray(s);
    ObjectInputStream objectInputStream;
    objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    try {
      return objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

}

class MtasSolrJoinCacheItem {

  public String url;
  public String request;
  public String data;

  public MtasSolrJoinCacheItem(String url, String request, String data) {
    this.url = url == null ? "" : url;
    this.request = request == null ? "" : request;
    this.data = data == null ? "" : data;
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 3) ^ url.hashCode();
    h = (h * 5) ^ request.hashCode();
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasSolrJoinCacheItem that = (MtasSolrJoinCacheItem) obj;
    return (url.equals(that.url)) && (request.equals(that.request));
  }
}
