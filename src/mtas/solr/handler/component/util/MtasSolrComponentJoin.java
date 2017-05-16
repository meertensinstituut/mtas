package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;

import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentJoin;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentJoin.
 */
@SuppressWarnings("deprecation")
public class MtasSolrComponentJoin implements MtasSolrComponent<ComponentJoin> {

  /** The Constant log. */
  private static final Log log = LogFactory.getLog(MtasSolrComponentJoin.class);

  /** The Constant PARAM_MTAS_JOIN. */
  public static final String PARAM_MTAS_JOIN = MtasSolrSearchComponent.PARAM_MTAS
      + ".join";

  /** The Constant NAME_MTAS_JOIN_FIELD. */
  public static final String NAME_MTAS_JOIN_FIELD = "field";

  /**
   * Instantiates a new mtas solr component join.
   *
   * @param searchComponent the search component
   */
  public MtasSolrComponentJoin(MtasSolrSearchComponent searchComponent) {
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#prepare(org.apache.solr.handler.component.ResponseBuilder, mtas.codec.util.CodecComponent.ComponentFields)
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

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#modifyRequest(org.apache.solr.handler.component.ResponseBuilder, org.apache.solr.handler.component.SearchComponent, org.apache.solr.handler.component.ShardRequest)
   */
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq) {
    if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
        && sreq.params.getBool(PARAM_MTAS_JOIN, false)) {
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

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#create(mtas.codec.util.CodecComponent.BasicComponent, java.lang.Boolean)
   */
  public SimpleOrderedMap<Object> create(ComponentJoin join, Boolean encode) throws IOException {
    MtasSolrJoinResult data = new MtasSolrJoinResult(join);
    SimpleOrderedMap<Object> mtasJoinResponse = new SimpleOrderedMap<>();
    if (encode) {
      mtasJoinResponse.add("_encoded_data", MtasSolrResultUtil.encode(data));
    } else {
      mtasJoinResponse.add("data", data.rewrite());
    }
    return mtasJoinResponse;
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#finishStage(org.apache.solr.handler.component.ResponseBuilder)
   */
  @SuppressWarnings("unchecked")
  public void finishStage(ResponseBuilder rb) {
    if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
        && rb.stage == MtasSolrSearchComponent.STAGE_JOIN) {
      for (ShardRequest sreq : rb.finished) {
        if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
            && sreq.params.getBool(PARAM_MTAS_JOIN, false)) {
          for (ShardResponse shardResponse : sreq.responses) {
            NamedList<Object> response = shardResponse.getSolrResponse()
                .getResponse();
            try {
              Object data = response.findRecursive("mtas", "join");
              if (data != null && data instanceof String) {
                NamedList<Object> mtasResponse = (NamedList<Object>) response
                    .get("mtas");
                mtasResponse.remove("join");
                mtasResponse.add("join",
                    MtasSolrResultUtil.decode((String) data));
              }
            } catch (ClassCastException e) {
              log.debug(e);
              // shouldn't happen
            }
          }
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#distributedProcess(org.apache.solr.handler.component.ResponseBuilder, mtas.codec.util.CodecComponent.ComponentFields)
   */
  @SuppressWarnings("unchecked")
  public void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    // rewrite
    NamedList<Object> mtasResponse = null;
    try {
      mtasResponse = (NamedList<Object>) rb.rsp.getValues().get("mtas");
    } catch (ClassCastException e) {
      log.debug(e);
      mtasResponse = null;
    }
    if (mtasResponse != null) {
      MtasSolrJoinResult mtasSolrJoinResult;
      try {
        mtasSolrJoinResult = (MtasSolrJoinResult) mtasResponse.get("join");
        if (mtasSolrJoinResult != null) {
          mtasResponse.removeAll("join");
          mtasResponse.add("join", mtasSolrJoinResult.rewrite());
        }
      } catch (ClassCastException e) {
        log.debug(e);
        mtasResponse.remove("join");
      }
    }
  }

  /**
   * Creates the key from request.
   *
   * @param rb the rb
   * @return the string
   */
  private String createKeyFromRequest(ResponseBuilder rb) {
    return rb.req.getParams().toQueryString();
  }


  


}
