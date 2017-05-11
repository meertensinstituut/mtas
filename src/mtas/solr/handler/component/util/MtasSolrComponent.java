package mtas.solr.handler.component.util;

import java.io.IOException;

import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;

import mtas.codec.util.CodecComponent.BasicComponent;
import mtas.codec.util.CodecComponent.ComponentFields;

abstract public interface MtasSolrComponent<T extends BasicComponent> {

  public abstract void prepare(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException;
  
  public abstract SimpleOrderedMap<Object> create(T response, Boolean encode)
      throws IOException;
  
  public abstract void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq);
  
  public abstract void finishStage(ResponseBuilder rb);
  
  public abstract void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException;

}
