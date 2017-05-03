package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;

import mtas.codec.util.CodecComponent.ComponentFacet;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentJoin;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentFacet.
 */
@SuppressWarnings("deprecation")
public class MtasSolrComponentJoin {

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant PARAM_MTAS_FACET. */
  public static final String PARAM_MTAS_JOIN = MtasSolrSearchComponent.PARAM_MTAS
      + ".join";

  public static final String NAME_MTAS_JOIN_FIELD = "field";

  /**
   * Instantiates a new mtas solr component facet.
   *
   * @param searchComponent
   *          the search component
   */
  public MtasSolrComponentJoin(MtasSolrSearchComponent searchComponent) {
    this.searchComponent = searchComponent;
  }

  /**
   * Prepare.
   *
   * @param rb
   *          the rb
   * @param mtasFields
   *          the mtas fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void prepare(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    if (rb.req.getParams().get(PARAM_MTAS_JOIN + "." + NAME_MTAS_JOIN_FIELD,
        null) != null) {
      Set<String> fields = new HashSet<>(Arrays.asList(rb.req.getParams()
          .get(PARAM_MTAS_JOIN + "." + NAME_MTAS_JOIN_FIELD).split(",")));
      String key = createKeyFromRequest(rb);
      mtasFields.doJoin = true;
      mtasFields.join = new ComponentJoin(fields, key);
      rb.setNeedDocSet(true);
    }

  }

  /**
   * Modify request.
   *
   * @param rb
   *          the rb
   * @param who
   *          the who
   * @param sreq
   *          the sreq
   */
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq) {
    if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
      if (sreq.params.getBool(PARAM_MTAS_JOIN, false)) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
          // do nothing
        } else {
          // remove for other requests
          Set<String> keys = MtasSolrResultUtil
              .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_JOIN);
          sreq.params.remove(PARAM_MTAS_JOIN);
          for (String key : keys) {
            sreq.params.remove(PARAM_MTAS_JOIN + "." + key);
          }
        }
      }
    }
  }

  /**
   * Creates the.
   *
   * @param facet
   *          the facet
   * @param encode
   *          the encode
   * @return the simple ordered map
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public Object create(ComponentJoin join, Boolean encode)
      throws IOException {
    MtasSolrJoinResult data = new MtasSolrJoinResult(join);
    if (encode) {
      return MtasSolrResultUtil.encode(data);
    } else {
      return data.rewrite();
    }
  }

  @SuppressWarnings("unchecked")
  public void finishStage(ResponseBuilder rb) {
    if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
      if (rb.stage == MtasSolrSearchComponent.STAGE_JOIN) {
        for (ShardRequest sreq : rb.finished) {
          if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
              && sreq.params.getBool(PARAM_MTAS_JOIN, false)) {
            for (ShardResponse shardResponse : sreq.responses) {
              NamedList<Object> response = shardResponse.getSolrResponse()
                  .getResponse();
              try {
                Object data = response
                    .findRecursive("mtas", "join");
                if (data != null && data instanceof String) {
                  NamedList<Object> mtasResponse = (NamedList<Object>) response.get("mtas");
                  mtasResponse.remove("join");
                  mtasResponse.add("join", MtasSolrResultUtil.decode((String) data));                  
                }
              } catch (ClassCastException e) {
                // shouldn't happen
              }              
            }
          }
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    // rewrite
    NamedList<Object> mtasResponse = null;
    try {
      mtasResponse = (NamedList<Object>) rb.rsp.getValues().get("mtas");
      if (mtasResponse != null) {
        MtasSolrJoinResult mtasSolrJoinResult;
        try {
          mtasSolrJoinResult = (MtasSolrJoinResult) mtasResponse.get("join");
          if (mtasSolrJoinResult != null) {
            mtasResponse.removeAll("join");
            mtasResponse.add("join", mtasSolrJoinResult.rewrite());
          }
        } catch (ClassCastException e) {
          mtasSolrJoinResult = null;
        }
      }
    } catch (ClassCastException e) {
      mtasResponse = null;
    }
  }
  
  private String createKeyFromRequest(ResponseBuilder rb) {
    return rb.req.getParams().toQueryString();
  }

}
