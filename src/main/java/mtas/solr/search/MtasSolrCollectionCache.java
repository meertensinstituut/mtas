package mtas.solr.search;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * The Class MtasSolrCollectionCache.
 */
public class MtasSolrCollectionCache {

  /** The Constant log. */
  private static final Log log = LogFactory
      .getLog(MtasSolrCollectionCache.class);

  /** The Constant DEFAULT_LIFETIME. */
  private static final long DEFAULT_LIFETIME = 86400;

  /** The Constant DEFAULT_MAXIMUM_NUMBER. */
  private static final int DEFAULT_MAXIMUM_NUMBER = 1000;

  /** The Constant DEFAULT_MAXIMUM_OVERFLOW. */
  private static final int DEFAULT_MAXIMUM_OVERFLOW = 10;

  /** The id to version. */
  private Map<String, String> idToVersion;

  /** The version to item. */
  private Map<String, MtasSolrCollectionCacheItem> versionToItem;

  /** The expiration version. */
  private Map<String, Long> expirationVersion;

  /** The collection cache path. */
  private Path collectionCachePath;

  /** The life time. */
  private long lifeTime;

  /** The maximum number. */
  private int maximumNumber;

  /** The maximum overflow. */
  private int maximumOverflow;

  /**
   * Instantiates a new mtas solr collection cache.
   *
   * @param cacheDirectory the cache directory
   * @param lifeTime the life time
   * @param maximumNumber the maximum number
   * @param maximumOverflow the maximum overflow
   */
  public MtasSolrCollectionCache(String cacheDirectory, Long lifeTime,
      Integer maximumNumber, Integer maximumOverflow) {
    this.lifeTime = (lifeTime != null && lifeTime > 0) ? lifeTime
        : DEFAULT_LIFETIME;
    this.maximumNumber = (maximumNumber != null && maximumNumber > 0)
        ? maximumNumber : DEFAULT_MAXIMUM_NUMBER;
    this.maximumOverflow = (maximumOverflow != null && maximumOverflow > 0)
        ? maximumOverflow : DEFAULT_MAXIMUM_OVERFLOW;
    idToVersion = new HashMap<>();
    expirationVersion = new HashMap<>();
    versionToItem = new HashMap<>();
    if (cacheDirectory != null) {
      try {
        collectionCachePath = Files
            .createDirectories(Paths.get(cacheDirectory));
        // reconstruct administration
        File[] fileList = collectionCachePath.toFile().listFiles();
        if (fileList != null) {
          for (File file : fileList) {
            if (file.isFile()) {
              String version = file.getName();
              MtasSolrCollectionCacheItem item = read(version, null);
              if (item != null) {
                if (idToVersion.containsKey(item.id)) {
                  expirationVersion.remove(idToVersion.get(item.id));
                  versionToItem.remove(idToVersion.get(item.id));
                  idToVersion.remove(item.id);
                  if (!file.delete()) {
                    log.error("couldn't delete " + file);
                  }
                }
                // don't keep data or automaton in memory
                item.data = null;
                // store in memory
                idToVersion.put(item.id, version);
                expirationVersion.put(version,
                    file.lastModified() + (1000 * lifeTime));
                versionToItem.put(version, item);
              } else {
                if (!file.delete()) {
                  log.error("couldn't delete " + file);
                }
              }
            } else if (file.isDirectory()) {
              log.info("unexpected directory " + file.getName());
            }
          }
          clear();
        }
      } catch (IOException e) {
        collectionCachePath = null;
        log.error("couldn't create cache directory " + cacheDirectory, e);
      }
    }
  }

  /**
   * Creates the.
   *
   * @param size the size
   * @param data the data
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public String create(Integer size, HashSet<String> data) throws IOException {
    return create(null, size, data, null);
  }

  /**
   * Creates the.
   *
   * @param id the id
   * @param size the size
   * @param data the data
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public String create(String id, Integer size, HashSet<String> data, String originalVersion)
      throws IOException {
    if (collectionCachePath != null) {
      // initialization
      Date date = clear();
      // create always new version, unless explicit original version is provided
      String version;
      if(originalVersion!=null&&versionToItem.containsKey(originalVersion)) {
        version = originalVersion;
      } else {
        do {
          version = UUID.randomUUID().toString();
        } while (versionToItem.containsKey(version));
      }  
      // create new item
      MtasSolrCollectionCacheItem item;
      if (id != null) {
        item = new MtasSolrCollectionCacheItem(id, size, data);
        // remove if item with id already exists
        deleteById(id);
      } else {
        item = new MtasSolrCollectionCacheItem(version, size, data);
      }
      // register
      idToVersion.put(id, version);
      expirationVersion.put(version, date.getTime() + (1000 * lifeTime));
      versionToItem.put(version, item);
      // store data in file
      File file = collectionCachePath.resolve(version).toFile();
      try (OutputStream outputStream = new FileOutputStream(file);
          Writer outputStreamWriter = new OutputStreamWriter(outputStream,
              StandardCharsets.UTF_8);) {
        outputStreamWriter.write(encode(item));
        // set correct time to reconstruct administration on restart
        if (!file.setLastModified(date.getTime())) {
          log.debug("couldn't change filetime " + file.getAbsolutePath());
        }
        // don't store data in memory
        item.data = null;
        // return version
        return version;
      } catch (IOException e) {
        idToVersion.remove(id);
        expirationVersion.remove(version);
        versionToItem.remove(version);
        throw new IOException("couldn't create " + version, e);
      }
    } else {
      throw new IOException("no cachePath available, can't store data");
    }
  }

  /**
   * List.
   *
   * @return the list
   */
  public List<SimpleOrderedMap<Object>> list() {
    List<SimpleOrderedMap<Object>> list = new ArrayList<>();
    for (Entry<String, String> entry : idToVersion.entrySet()) {
      SimpleOrderedMap<Object> item = new SimpleOrderedMap<>();
      item.add("id", entry.getKey());
      item.add("size", versionToItem.get(entry.getValue()).size);
      item.add("version", entry.getValue());
      item.add("expiration", expirationVersion.get(entry.getValue()));
      list.add(item);
    }
    return list;
  }

  /**
   * Check.
   *
   * @param id the id
   * @return the simple ordered map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public SimpleOrderedMap<Object> check(String id) throws IOException {
    if (idToVersion.containsKey(id)) {
      String version = idToVersion.get(id);
      MtasSolrCollectionCacheItem item = versionToItem.get(version);
      Date date = new Date();
      long now = date.getTime();
      if (verify(version, now)) {
        SimpleOrderedMap<Object> data = new SimpleOrderedMap<>();
        data.add("now", now);
        data.add("id", item.id);
        data.add("size", item.size);
        data.add("version", version);
        data.add("expiration", expirationVersion.get(version));
        return data;
      } else {
        idToVersion.remove(id);
        versionToItem.remove(version);
        expirationVersion.remove(version);
        return null;
      }
    } else {
      return null;
    }
  }

  /**
   * Now.
   *
   * @return the long
   */
  public long now() {
    return clear().getTime();
  }

  /**
   * Gets the data by id.
   *
   * @param id the id
   * @return the data by id
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public HashSet<String> getDataById(String id) throws IOException {
    if (idToVersion.containsKey(id)) {
      return get(id);
    } else {
      return null;
    }
  }

  /**
   * Gets the automaton by id.
   *
   * @param id the id
   * @return the automaton by id
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public Automaton getAutomatonById(String id) throws IOException {
    if (idToVersion.containsKey(id)) {
      List<BytesRef> bytesArray = new ArrayList<>();
      Set<String> data = get(id);
      if (data != null) {
        Term term;
        for (String item : data) {
          term = new Term("dummy", item);
          bytesArray.add(term.bytes());
        }
        Collections.sort(bytesArray);
        return Automata.makeStringUnion(bytesArray);
      }
    }
    return null;
  }

  /**
   * Delete by id.
   *
   * @param id the id
   */
  public void deleteById(String id) {
    if (idToVersion.containsKey(id)) {
      String version = idToVersion.remove(id);
      expirationVersion.remove(version);
      versionToItem.remove(version);
      if (collectionCachePath != null
          && !collectionCachePath.resolve(version).toFile().delete()) {
        log.debug("couldn't delete " + version);
      }
    }
  }

  /**
   * Gets the.
   *
   * @param id the id
   * @return the hash set
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private HashSet<String> get(String id) throws IOException {
    if (collectionCachePath != null) {
      Date date = clear();
      if (idToVersion.containsKey(id)) {
        String version = idToVersion.get(id);
        expirationVersion.put(version, date.getTime() + (1000 * lifeTime));
        MtasSolrCollectionCacheItem newItem = read(version, date.getTime());
        if (newItem != null && newItem.id.equals(id)) {
          return newItem.data;
        } else {
          log.error("couldn't get " + version);
          // delete file and remove from index
          if (!collectionCachePath.resolve(version).toFile().delete()) {
            log.debug("couldn't delete " + version);
          }
          idToVersion.remove(id);
          expirationVersion.remove(version);
          versionToItem.remove(version);
        }
      } else {
        log.error("doesn't exist anymore");
      }
      return null;
    } else

    {
      throw new IOException("no cachePath available, can't get data");
    }
  }

  /**
   * Read.
   *
   * @param version the version
   * @param time the time
   * @return the mtas solr collection cache item
   */
  private MtasSolrCollectionCacheItem read(String version, Long time) {
    try {
      Path path = collectionCachePath.resolve(version);
      String data = new String(Files.readAllBytes(path),
          StandardCharsets.UTF_8);
      MtasSolrCollectionCacheItem decodedData = decode(data);

      // set correct time to reconstruct administration on restart
      if (time != null) {
        File file = path.toFile();
        if (!file.setLastModified(time)) {
          log.debug("couldn't change filetime " + file.getAbsolutePath());
        }
      }
      return decodedData;
    } catch (IOException e) {
      log.error("couldn't read " + version, e);
    }
    return null;
  }

  /**
   * Verify.
   *
   * @param version the version
   * @param time the time
   * @return true, if successful
   */
  private boolean verify(String version, Long time) {
    if (versionToItem.containsKey(version)) {
      Path path = collectionCachePath.resolve(version);
      File file = path.toFile();
      if (file.exists() && file.canRead() && file.canWrite()) {
        if (time != null) {
          if (!file.setLastModified(time)) {
            log.debug("couldn't change filetime " + file.getAbsolutePath());
          } else {
            expirationVersion.put(version, time + (1000 * lifeTime));
          }
        }
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  /**
   * Clear.
   *
   * @return the date
   */
  private Date clear() {
    Date date = new Date();
    Long timestamp = date.getTime();
    HashSet<String> idsToBeRemoved = new HashSet<>();
    // check expiration
    Iterator<Entry<String, Long>> expirationVersionEntryIterator = expirationVersion.entrySet().iterator();
    while(expirationVersionEntryIterator.hasNext()) {
      Entry<String, Long> entry = expirationVersionEntryIterator.next();
      if (entry.getValue() < timestamp) {
        String version = entry.getKey();
        if (versionToItem.containsKey(version)) {
          idsToBeRemoved.add(versionToItem.get(version).id);
        } else {
          log.debug("could not remove " + version);
        }
      }
    }
    for (String id : idsToBeRemoved) {
      deleteById(id);
    }
    idsToBeRemoved.clear();
    // check size
    if (expirationVersion.size() > maximumNumber + maximumOverflow) {
      Set<Entry<String, Long>> mapEntries = expirationVersion.entrySet();
      List<Entry<String, Long>> aList = new LinkedList<>(mapEntries);
      Collections.sort(aList,
          (Entry<String, Long> ele1, Entry<String, Long> ele2) -> ele2
              .getValue().compareTo(ele1.getValue()));
      aList.subList(maximumNumber, aList.size()).clear();
      Iterator<Entry<String, MtasSolrCollectionCacheItem>> versionToItemEntryIterator = versionToItem.entrySet().iterator();
      while(versionToItemEntryIterator.hasNext()) {
        Entry<String, MtasSolrCollectionCacheItem> entry = versionToItemEntryIterator.next();
        if (!expirationVersion.containsKey(entry.getKey())) {
          idsToBeRemoved.add(entry.getValue().id);
        }
      }
      for (String id : idsToBeRemoved) {
        deleteById(id);
      }
      idsToBeRemoved.clear();
    }
    return date;
  }

  /**
   * Encode.
   *
   * @param o the o
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private String encode(MtasSolrCollectionCacheItem o) throws IOException {
    if (o != null) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream objectOutputStream;
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(o);
      objectOutputStream.close();
      byte[] byteArray = byteArrayOutputStream.toByteArray();
      return Base64.byteArrayToBase64(byteArray);
    } else {
      throw new IOException("nothing to encode");
    }
  }

  /**
   * Decode.
   *
   * @param s the s
   * @return the mtas solr collection cache item
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private MtasSolrCollectionCacheItem decode(String s) throws IOException {
    byte[] bytes = Base64.base64ToByteArray(s);
    ObjectInputStream objectInputStream;
    objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    try {
      Object o = objectInputStream.readObject();
      if (o instanceof MtasSolrCollectionCacheItem) {
        return (MtasSolrCollectionCacheItem) o;
      } else {
        throw new IOException("unexpected " + o.getClass().getSimpleName());
      }
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Empty.
   */
  public void empty() {
    for (Entry<String, String> entry : idToVersion.entrySet()) {
      expirationVersion.remove(entry.getValue());
      versionToItem.remove(entry.getValue());
      if (collectionCachePath != null
          && !collectionCachePath.resolve(entry.getValue()).toFile().delete()) {
        log.debug("couldn't delete " + entry.getValue());
      }
    }
    idToVersion.clear();
  }

}

class MtasSolrCollectionCacheItem implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  public String id;
  public Integer size;
  public HashSet<String> data = null;

  public MtasSolrCollectionCacheItem(String id, Integer size,
      HashSet<String> data) throws IOException {
    if (id != null) {
      this.id = id;
      this.size = size;
      this.data = data;
    } else {
      throw new IOException("no id provided");
    }
  }

  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 3) ^ id.hashCode();
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
    final MtasSolrCollectionCacheItem that = (MtasSolrCollectionCacheItem) obj;
    return (id.equals(that.id));
  }
}
