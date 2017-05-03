package mtas.solr.handler.component.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

import org.apache.solr.common.util.NamedList;

import mtas.codec.util.CodecComponent.ComponentJoin;

public class MtasSolrJoinResult implements Serializable {
  
  private Set<String> values;
  private String key;
  
  public MtasSolrJoinResult(ComponentJoin join) {
    values = join.values();
    key = join.key();
  }
  
  public NamedList<Object> rewrite() {
    NamedList<Object> response = new NamedList<>();
    response.add("values", values);
    response.add("key", key);
    return response;
  }
  
  public void merge(MtasSolrJoinResult newItem) {
    values.addAll(newItem.values);
  }

}
