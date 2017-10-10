package mtas.solr.handler.component.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

import mtas.codec.util.DataCollector;
import mtas.codec.util.CodecComponent.GroupHit;
import mtas.codec.util.collector.MtasDataItem;
import mtas.parser.cql.MtasCQLParser;
import mtas.parser.cql.TokenMgrError;
import mtas.search.spans.util.MtasSpanQuery;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrResultUtil.
 */
public class MtasSolrResultUtil {

  /** The Constant log. */
  private static final Log log = LogFactory.getLog(MtasSolrResultUtil.class);

  /** The Constant QUERY_TYPE_CQL. */
  public static final String QUERY_TYPE_CQL = "cql";

  /** The Constant patternKeyStartGrouphit. */
  public static final Pattern patternKeyStartGrouphit = Pattern
      .compile("^" + GroupHit.KEY_START);

  /**
   * Instantiates a new mtas solr result util.
   */
  private MtasSolrResultUtil() {
    // do nothing
  }

  /**
   * Rewrite.
   *
   * @param al the al
   * @param searchComponent the search component
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void rewrite(ArrayList<?> al,
      MtasSolrSearchComponent searchComponent) throws IOException {
    for (int i = 0; i < al.size(); i++) {
      if (al.get(i) instanceof NamedList) {
        rewrite((NamedList) al.get(i), searchComponent);
      } else if (al.get(i) instanceof ArrayList) {
        rewrite((ArrayList) al.get(i), searchComponent);
      }
    }
  }

  /**
   * Rewrite.
   *
   * @param nl the nl
   * @param searchComponent the search component
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void rewrite(NamedList<Object> nl,
      MtasSolrSearchComponent searchComponent) throws IOException {
    rewrite(nl, searchComponent, true);
  }

  /**
   * Rewrite.
   *
   * @param nl the nl
   * @param searchComponent the search component
   * @param doCollapse the do collapse
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static void rewrite(NamedList<Object> nl,
      MtasSolrSearchComponent searchComponent, boolean doCollapse)
      throws IOException {
    boolean showDebugInfo = false;
    HashMap<String, NamedList<Object>> collapseNamedList = new HashMap<>();
    int length = nl.size();
    for (int i = 0; i < length; i++) {
      if (nl.getVal(i) instanceof NamedList) {
        NamedList o = (NamedList) nl.getVal(i);
        rewrite(o, searchComponent, true);
        nl.setVal(i, o);
      } else if (nl.getVal(i) instanceof ArrayList) {
        ArrayList o = (ArrayList) nl.getVal(i);
        rewrite(o, searchComponent);
        nl.setVal(i, o);
      } else if (nl.getVal(i) instanceof MtasDataItem) {
        MtasDataItem dataItem = (MtasDataItem) nl.getVal(i);
        nl.setVal(i, dataItem.rewrite(showDebugInfo));
      } else if (nl.getVal(i) instanceof MtasSolrCollectionResult) {
        MtasSolrCollectionResult o = (MtasSolrCollectionResult) nl.getVal(i);
        collapseNamedList.put(nl.getName(i), o.rewrite(searchComponent));
      } else if (nl.getVal(i) instanceof MtasSolrMtasResult) {
        MtasSolrMtasResult o = (MtasSolrMtasResult) nl.getVal(i);
        if (o.dataCollector.getCollectorType()
            .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
          if (!o.dataCollector.withTotal()) {
            NamedList<Object> nnl = o.getNamedList(showDebugInfo);
            for (int j = 0; j < nnl.size(); j++) {
              if (nnl.getVal(j) != null
                  && nnl.getVal(j) instanceof MtasDataItem) {
                MtasDataItem mdi = (MtasDataItem) nnl.getVal(j);
                mdi.rewrite(showDebugInfo);
                nnl.setVal(j, mdi);
              }
            }
            // System.out.println("rewrite -- "+nnl);
            nl.setVal(i, rewriteToArray(nnl));
            // System.out.println("rewrite! "+nl.getVal(i));
          } else {
            NamedList<Object> tmpResponse = new SimpleOrderedMap<>();
            tmpResponse.add(nl.getName(i) + "Total", o.dataCollector.getSize());
            NamedList<Object> nnl = o.getNamedList(showDebugInfo);
            for (int j = 0; j < nnl.size(); j++) {
              if (nnl.getVal(j) != null
                  && nnl.getVal(j) instanceof MtasDataItem) {
                MtasDataItem mdi = (MtasDataItem) nnl.getVal(j);
                mdi.rewrite(showDebugInfo);
                nnl.setVal(j, mdi);
              }
            }
            tmpResponse.add(nl.getName(i), rewriteToArray(nnl));
            nl.setVal(i, null);
            collapseNamedList.put(nl.getName(i), tmpResponse);
          }
        } else if (o.dataCollector.getCollectorType()
            .equals(DataCollector.COLLECTOR_TYPE_DATA)) {
          NamedList<Object> nnl = o.getData(showDebugInfo);
          if (nnl.size() > 0) {
            rewrite(nnl, searchComponent);
            collapseNamedList.put(nl.getName(i), nnl);
            nl.setVal(i, nnl);
          } else {
            nl.setVal(i, null);
          }
        }
      }
    }
    // collapse
    if (doCollapse && collapseNamedList.size() > 0) {
      for (String key : collapseNamedList.keySet()) {
        nl.remove(key);
      }
      for (NamedList<Object> items : collapseNamedList.values()) {
        nl.addAll(items);
      }
    }
  }

  /**
   * Rewrite to array.
   *
   * @param nnl the nnl
   * @return the array list
   */
  private static ArrayList<NamedList<Object>> rewriteToArray(
      NamedList<Object> nnl) {
    ArrayList<NamedList<Object>> al = new ArrayList<>();
    String key;
    Iterator<Entry<String, Object>> it = nnl.iterator();
    while (it.hasNext()) {
      Entry<String, Object> entry = it.next();
      NamedList<Object> item = (NamedList<Object>) entry.getValue();
      key = entry.getKey();
      if (key.startsWith(GroupHit.KEY_START)) {
        StringBuilder newKey = new StringBuilder("");
        item.add("group", GroupHit.keyToObject(key, newKey));
        item.add("key", newKey.toString().trim());
      } else {
        item.add("key", key);
      }
      al.add(item);
    }
    return al;
  }

  /**
   * Rewrite merge list.
   *
   * @param key the key
   * @param subKey the sub key
   * @param snl the snl
   * @param tnl the tnl
   */
  @SuppressWarnings({ "unchecked", "unused" })
  private static void rewriteMergeList(String key, String subKey,
      NamedList<Object> snl, NamedList<Object> tnl) {
    for (int i = 0; i < tnl.size(); i++) {
      Object item = snl.get(tnl.getName(i));
      if (item != null && tnl.getVal(i) instanceof NamedList) {
        NamedList<Object> tnnl = (NamedList<Object>) tnl.getVal(i);
        Object o = tnnl.get(key);
        NamedList<Object> tnnnl;
        if (o != null && o instanceof NamedList) {
          tnnnl = (NamedList<Object>) o;
        } else {
          tnnnl = new SimpleOrderedMap<>();
          tnnl.add(key, tnnnl);
        }
        tnnnl.add(subKey, item);
      }
    }
  }

  /**
   * Rewrite merge data.
   *
   * @param key the key
   * @param subKey the sub key
   * @param snl the snl
   * @param tnl the tnl
   */
  @SuppressWarnings({ "unused", "unchecked" })
  private static void rewriteMergeData(String key, String subKey,
      NamedList<Object> snl, NamedList<Object> tnl) {
    if (snl != null) {
      Object o = tnl.get(key);
      NamedList<Object> tnnnl;
      if (o != null && o instanceof NamedList) {
        tnnnl = (NamedList<Object>) o;
      } else {
        tnnnl = new SimpleOrderedMap<>();
        tnl.add(key, tnnnl);
      }
      tnnnl.add(subKey, snl);
    }
  }

  /**
   * Encode.
   *
   * @param o the o
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static String encode(Object o) throws IOException {
    if (o instanceof Serializable) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream objectOutputStream;
      try {
        objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(o);
        objectOutputStream.close();
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        return Base64.byteArrayToBase64(byteArray);
      } catch (IOException e) {
        e.printStackTrace();
        log.error(e);
        return null;
      }
    } else {
      throw new IOException("no serializable object");
    }
  }

  /**
   * Decode.
   *
   * @param s the s
   * @return the object
   */
  static Object decode(String s) {
    byte[] bytes = Base64.base64ToByteArray(s);
    ObjectInputStream objectInputStream;
    try {
      objectInputStream = new ObjectInputStream(
          new ByteArrayInputStream(bytes));
      return objectInputStream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      log.error(e);
      return null;
    }
  }

  /**
   * Decode.
   *
   * @param l the l
   * @return the array list
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  static ArrayList decode(ArrayList l) {
    for (int i = 0; i < l.size(); i++) {
      if (l.get(i) instanceof NamedList) {
        l.set(i, decode((NamedList) l.get(i)));
      } else if (l.get(i) instanceof ArrayList) {
        l.set(i, decode((ArrayList) l.get(i)));
      }
    }
    return l;
  }

  /**
   * Decode.
   *
   * @param nl the nl
   * @return the named list
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  static NamedList<Object> decode(NamedList<Object> nl) {
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
          // System.out.println("unknown type " +
          // o.getClass().getCanonicalName());
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

  /**
   * Gets the ids from parameters.
   *
   * @param params the params
   * @param prefix the prefix
   * @return the ids from parameters
   */
  public static SortedSet<String> getIdsFromParameters(SolrParams params,
      String prefix) {
    SortedSet<String> ids = new TreeSet<>();
    Iterator<String> it = params.getParameterNamesIterator();
    Pattern pattern = Pattern
        .compile("^" + Pattern.quote(prefix) + "\\.([^\\.]+)(\\..*|$)");
    while (it.hasNext()) {
      String item = it.next();
      Matcher m = pattern.matcher(item);
      if (m.matches()) {
        ids.add(m.group(1));
      }
    }
    return ids;
  }

  /**
   * Compare and check.
   *
   * @param list the list
   * @param original the original
   * @param nameNew the name new
   * @param nameOriginal the name original
   * @param unique the unique
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void compareAndCheck(String[] list, String[] original,
      String nameNew, String nameOriginal, Boolean unique) throws IOException {
    if (list != null) {
      if (list.length != original.length) {
        throw new IOException(
            "unequal size " + nameNew + " and " + nameOriginal);
      }
      if (unique) {
        Set<String> set = new HashSet<>();
        for (int i = 0; i < list.length; i++) {
          set.add(list[i]);
        }
        if (set.size() < list.length) {
          throw new IOException("duplicate " + nameNew);
        }
      }
    }
  }

  /**
   * Construct query.
   *
   * @param queryValue the query value
   * @param queryType the query type
   * @param queryPrefix the query prefix
   * @param queryVariables the query variables
   * @param field the field
   * @param queryIgnore the query ignore
   * @param maximumIgnoreLength the maximum ignore length
   * @return the mtas span query
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static MtasSpanQuery constructQuery(String queryValue,
      String queryType, String queryPrefix,
      HashMap<String, String[]> queryVariables, String field,
      String queryIgnore, Integer maximumIgnoreLength) throws IOException {
    if (queryType == null || queryType.isEmpty()) {
      throw new IOException("no (valid) type for query " + queryValue);
    } else if (queryValue == null || queryValue.isEmpty()) {
      throw new IOException("no (valid) value for " + queryType + " query");
    }
    MtasSpanQuery ignore = null;
    if (queryIgnore != null) {
      Reader queryIgnoreReader = new BufferedReader(
          new StringReader(queryIgnore));
      if (queryType.equals(QUERY_TYPE_CQL)) {
        MtasCQLParser ip = new MtasCQLParser(queryIgnoreReader);
        try {
          ignore = ip.parse(field, null, null, null, null);
        } catch (mtas.parser.cql.ParseException e) {
          throw new IOException("couldn't parse " + queryType + " query "
              + queryIgnore + " (" + e.getMessage() + ")", e);
        } catch (TokenMgrError e) {
          throw new IOException("couldn't parse " + queryType + " query "
              + queryIgnore + " (" + e.getMessage() + ")", e);
        }
      } else {
        throw new IOException(
            "unknown queryType " + queryType + " for query " + queryValue);
      }
    }
    Reader queryValueReader = new BufferedReader(new StringReader(queryValue));
    if (queryType.equals(QUERY_TYPE_CQL)) {
      MtasCQLParser qp = new MtasCQLParser(queryValueReader);
      try {
        return qp.parse(field, queryPrefix, queryVariables, ignore,
            maximumIgnoreLength);
      } catch (mtas.parser.cql.ParseException e) {
        throw new IOException("couldn't parse " + queryType + " query "
            + queryValue + " (" + e.getMessage() + ")", e);
      } catch (TokenMgrError e) {
        throw new IOException("couldn't parse " + queryType + " query "
            + queryValue + " (" + e.getMessage() + ")", e);
      }
    } else {
      throw new IOException(
          "unknown queryType " + queryType + " for query " + queryValue);
    }
  }

}
