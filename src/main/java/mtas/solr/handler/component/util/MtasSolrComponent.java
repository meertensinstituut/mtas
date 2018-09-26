package mtas.solr.handler.component.util;

import java.io.IOException;

import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;

import mtas.codec.util.CodecComponent.BasicComponent;
import mtas.codec.util.CodecComponent.ComponentFields;

/**
 * The Interface MtasSolrComponent.
 *
 * @param <T> the generic type
 */
public interface MtasSolrComponent<T extends BasicComponent> {

  /**
   * Prepare.
   *
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void prepare(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException;

  /**
   * Creates the.
   *
   * @param response the response
   * @param encode the encode
   * @return the simple ordered map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  SimpleOrderedMap<Object> create(T response, Boolean encode)
      throws IOException;

  /**
   * Modify request.
   *
   * @param rb the rb
   * @param who the who
   * @param sreq the sreq
   */
  void modifyRequest(ResponseBuilder rb, SearchComponent who,
                     ShardRequest sreq);

  /**
   * Finish stage.
   *
   * @param rb the rb
   */
  void finishStage(ResponseBuilder rb);

  /**
   * Distributed process.
   *
   * @param rb the rb
   * @param mtasFields the mtas fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void distributedProcess(ResponseBuilder rb,
                          ComponentFields mtasFields) throws IOException;

}
