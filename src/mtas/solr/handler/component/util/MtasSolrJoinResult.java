package mtas.solr.handler.component.util;

import java.io.Serializable;
import java.util.Set;

import org.apache.solr.common.util.NamedList;

import mtas.codec.util.CodecComponent.ComponentJoin;

/**
 * The Class MtasSolrJoinResult.
 */
public class MtasSolrJoinResult implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The values. */
  private Set<String> values;

  /** The key. */
  private String key;

  /**
   * Instantiates a new mtas solr join result.
   *
   * @param join the join
   */
  public MtasSolrJoinResult(ComponentJoin join) {
    values = join.values();
    key = join.key();
  }

  /**
   * Rewrite.
   *
   * @return the named list
   */
  public NamedList<Object> rewrite() {
    NamedList<Object> response = new NamedList<>();
    response.add("values", values);
    response.add("key", key);
    return response;
  }

  /**
   * Merge.
   *
   * @param newItem the new item
   */
  public void merge(MtasSolrJoinResult newItem) {
    values.addAll(newItem.values);
  }

}
