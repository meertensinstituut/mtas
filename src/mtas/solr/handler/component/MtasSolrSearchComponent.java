package mtas.solr.handler.component;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import mtas.codec.MtasCodecPostingsFormat;
import mtas.codec.util.CodecComponent.ComponentFacet;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentGroup;
import mtas.codec.util.CodecComponent.ComponentKwic;
import mtas.codec.util.CodecComponent.ComponentList;
import mtas.codec.util.CodecComponent.ComponentPosition;
import mtas.codec.util.CodecComponent.ComponentSpan;
import mtas.codec.util.CodecComponent.ComponentTermVector;
import mtas.codec.util.CodecComponent.ComponentToken;
import mtas.codec.util.CodecUtil;
import mtas.solr.handler.component.util.MtasSolrResultMerge;
import mtas.solr.handler.component.util.MtasSolrComponentFacet;
import mtas.solr.handler.component.util.MtasSolrComponentGroup;
import mtas.solr.handler.component.util.MtasSolrComponentKwic;
import mtas.solr.handler.component.util.MtasSolrComponentList;
import mtas.solr.handler.component.util.MtasSolrComponentPrefix;
import mtas.solr.handler.component.util.MtasSolrComponentStats;
import mtas.solr.handler.component.util.MtasSolrComponentTermvector;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * The Class MtasSolrSearchComponent.
 */
public class MtasSolrSearchComponent extends SearchComponent {

  /** The search component. */
  MtasSolrSearchComponent searchComponent;

  /** The Constant PARAM_MTAS. */
  public static final String PARAM_MTAS = "mtas";

  /** The Constant STAGE_TERMVECTOR_MISSING_TOP. */
  public static final int STAGE_TERMVECTOR_MISSING_TOP = ResponseBuilder.STAGE_EXECUTE_QUERY
      + 10;

  /** The Constant STAGE_TERMVECTOR_MISSING_KEY. */
  public static final int STAGE_TERMVECTOR_MISSING_KEY = ResponseBuilder.STAGE_EXECUTE_QUERY
      + 11;

  /** The Constant STAGE_TERMVECTOR_FINISH. */
  public static final int STAGE_TERMVECTOR_FINISH = ResponseBuilder.STAGE_EXECUTE_QUERY
      + 12;

  /** The Constant STAGE_LIST. */
  public static final int STAGE_LIST = ResponseBuilder.STAGE_EXECUTE_QUERY + 20;

  /** The Constant STAGE_PREFIX. */
  public static final int STAGE_PREFIX = ResponseBuilder.STAGE_EXECUTE_QUERY
      + 30;

  /** The Constant STAGE_STATS. */
  public static final int STAGE_STATS = ResponseBuilder.STAGE_EXECUTE_QUERY
      + 40;

  /** The Constant STAGE_FACET. */
  public static final int STAGE_FACET = ResponseBuilder.STAGE_EXECUTE_QUERY
      + 50;

  /** The Constant STAGE_GROUP. */
  public static final int STAGE_GROUP = ResponseBuilder.STAGE_EXECUTE_QUERY
      + 60;

  /** The mtas solr result merge. */
  private MtasSolrResultMerge mtasSolrResultMerge;

  /** The search stats. */
  private MtasSolrComponentStats searchStats;

  /** The search termvector. */
  private MtasSolrComponentTermvector searchTermvector;

  /** The search prefix. */
  private MtasSolrComponentPrefix searchPrefix;

  /** The search facet. */
  private MtasSolrComponentFacet searchFacet;

  /** The search group. */
  private MtasSolrComponentGroup searchGroup;

  /** The search list. */
  private MtasSolrComponentList searchList;

  /** The search kwic. */
  private MtasSolrComponentKwic searchKwic;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.component.SearchComponent#getVersion()
   */
  @Override
  public String getVersion() {
    return String.valueOf(MtasCodecPostingsFormat.VERSION_CURRENT);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.component.SearchComponent#getDescription()
   */
  @Override
  public String getDescription() {
    return "Mtas";
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#prepare(org.apache.solr.
   * handler.component.ResponseBuilder)
   */
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    // System.out.println(System.nanoTime()+" - "+Thread.currentThread().getId()
    // + " - "
    // + rb.req.getParams().getBool("isShard", false) + " PREPARE " + rb.stage
    // + " " + rb.req.getParamString());
    if (rb.req.getParams().getBool(PARAM_MTAS, false)) {

      mtasSolrResultMerge = new MtasSolrResultMerge();

      ComponentFields mtasFields = new ComponentFields();
      // get settings kwic
      if (rb.req.getParams().getBool(MtasSolrComponentKwic.PARAM_MTAS_KWIC,
          false)) {
        searchKwic = new MtasSolrComponentKwic(this);
        searchKwic.prepare(rb, mtasFields);
      }
      // get settings list
      if (rb.req.getParams().getBool(MtasSolrComponentList.PARAM_MTAS_LIST,
          false)) {
        searchList = new MtasSolrComponentList(this);
        searchList.prepare(rb, mtasFields);
      }
      // get settings group
      if (rb.req.getParams().getBool(MtasSolrComponentGroup.PARAM_MTAS_GROUP,
          false)) {
        searchGroup = new MtasSolrComponentGroup(this);
        searchGroup.prepare(rb, mtasFields);
      }
      // get settings termvector
      if (rb.req.getParams()
          .getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
        searchTermvector = new MtasSolrComponentTermvector(this);
        searchTermvector.prepare(rb, mtasFields);
      }
      // get settings prefix
      if (rb.req.getParams().getBool(MtasSolrComponentPrefix.PARAM_MTAS_PREFIX,
          false)) {
        searchPrefix = new MtasSolrComponentPrefix(this);
        searchPrefix.prepare(rb, mtasFields);
      }
      // get settings stats
      if (rb.req.getParams().getBool(MtasSolrComponentStats.PARAM_MTAS_STATS,
          false)) {
        searchStats = new MtasSolrComponentStats(this);
        searchStats.prepare(rb, mtasFields);
      }
      // get settings facet
      if (rb.req.getParams().getBool(MtasSolrComponentFacet.PARAM_MTAS_FACET,
          false)) {
        searchFacet = new MtasSolrComponentFacet(this);
        searchFacet.prepare(rb, mtasFields);
      }
      rb.req.getContext().put(ComponentFields.class, mtasFields);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#process(org.apache.solr.
   * handler.component.ResponseBuilder)
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException {
    // System.out
    // .println(System.nanoTime() + " - " + Thread.currentThread().getId()
    // + " - " + rb.req.getParams().getBool("isShard", false) + " PROCESS "
    // + rb.stage + " " + rb.req.getParamString());
    ComponentFields mtasFields = getMtasFields(rb);
    if (mtasFields != null) {
      DocSet docSet = rb.getResults().docSet;
      DocList docList = rb.getResults().docList;
      if (mtasFields.doStats || mtasFields.doKwic || mtasFields.doList
          || mtasFields.doGroup || mtasFields.doFacet || mtasFields.doTermVector
          || mtasFields.doPrefix) {
        SolrIndexSearcher searcher = rb.req.getSearcher();
        ArrayList<Integer> docSetList = null;
        ArrayList<Integer> docListList = null;
        if (docSet != null) {
          docSetList = new ArrayList<Integer>();
          Iterator<Integer> docSetIterator = docSet.iterator();
          while (docSetIterator.hasNext()) {
            docSetList.add(docSetIterator.next());
          }
          Collections.sort(docSetList);
        }
        if (docList != null) {
          docListList = new ArrayList<Integer>();
          Iterator<Integer> docListIterator = docList.iterator();
          while (docListIterator.hasNext()) {
            docListList.add(docListIterator.next());
          }
          Collections.sort(docListList);
        }
        for (String field : mtasFields.list.keySet()) {
          try {
            CodecUtil.collect(field, searcher, searcher.getRawReader(),
                docListList, docSetList, mtasFields.list.get(field));
          } catch (IllegalAccessException | IllegalArgumentException
              | InvocationTargetException e) {
            throw new IOException(e.getMessage());
          }
        }
        NamedList<Object> mtasResponse = new SimpleOrderedMap<>();
        if (mtasFields.doKwic) {
          ArrayList<NamedList<?>> mtasKwicResponses = new ArrayList<NamedList<?>>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentKwic kwic : mtasFields.list.get(field).kwicList) {
              mtasKwicResponses.add(searchKwic.create(kwic));
            }
          }
          // add to response
          mtasResponse.add("kwic", mtasKwicResponses);
        }
        if (mtasFields.doFacet) {
          ArrayList<NamedList<?>> mtasFacetResponses = new ArrayList<NamedList<?>>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentFacet facet : mtasFields.list.get(field).facetList) {
              if (rb.req.getParams().getBool("isShard", false)) {
                mtasFacetResponses.add(searchFacet.create(facet, true));
              } else {
                mtasFacetResponses.add(searchFacet.create(facet, false));
              }
            }
          }
          // add to response
          mtasResponse.add("facet", mtasFacetResponses);
        }
        if (mtasFields.doList) {
          ArrayList<NamedList<?>> mtasListResponses = new ArrayList<NamedList<?>>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentList list : mtasFields.list.get(field).listList) {
              mtasListResponses.add(searchList.create(list));
            }
          }
          // add to response
          mtasResponse.add("list", mtasListResponses);
        }
        if (mtasFields.doGroup) {
          ArrayList<NamedList<?>> mtasGroupResponses = new ArrayList<NamedList<?>>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentGroup group : mtasFields.list.get(field).groupList) {
              if (rb.req.getParams().getBool("isShard", false)) {
                mtasGroupResponses.add(searchGroup.create(group, true));
              } else {
                mtasGroupResponses.add(searchGroup.create(group, false));
              }
            }
          }
          // add to response
          mtasResponse.add("group", mtasGroupResponses);
        }
        if (mtasFields.doTermVector) {
          ArrayList<NamedList<?>> mtasTermVectorResponses = new ArrayList<NamedList<?>>();
          for (String field : mtasFields.list.keySet()) {
            for (ComponentTermVector termVector : mtasFields.list
                .get(field).termVectorList) {
              if (rb.req.getParams().getBool("isShard", false)) {
                mtasTermVectorResponses
                    .add(searchTermvector.create(termVector, true));
              } else {
                mtasTermVectorResponses
                    .add(searchTermvector.create(termVector, false));
              }
            }
          }
          // add to response
          mtasResponse.add("termvector", mtasTermVectorResponses);
        }
        if (mtasFields.doPrefix) {
          ArrayList<NamedList<?>> mtasPrefixResponses = new ArrayList<NamedList<?>>();
          for (String field : mtasFields.list.keySet()) {
            if (mtasFields.list.get(field).prefix != null) {
              if (rb.req.getParams().getBool("isShard", false)) {
                mtasPrefixResponses.add(searchPrefix
                    .create(mtasFields.list.get(field).prefix, true));
              } else {
                mtasPrefixResponses.add(searchPrefix
                    .create(mtasFields.list.get(field).prefix, false));
              }
            }
          }
          mtasResponse.add("prefix", mtasPrefixResponses);
        }
        if (mtasFields.doStats) {
          NamedList<Object> mtasStatsResponse = new SimpleOrderedMap<>();
          if (mtasFields.doStatsPositions || mtasFields.doStatsTokens
              || mtasFields.doStatsSpans) {
            if (mtasFields.doStatsTokens) {
              ArrayList<Object> mtasStatsTokensResponses = new ArrayList<Object>();
              for (String field : mtasFields.list.keySet()) {
                for (ComponentToken token : mtasFields.list
                    .get(field).statsTokenList) {
                  if (rb.req.getParams().getBool("isShard", false)) {
                    mtasStatsTokensResponses
                        .add(searchStats.createToken(token, true));
                  } else {
                    mtasStatsTokensResponses
                        .add(searchStats.createToken(token, false));
                  }
                }
              }
              mtasStatsResponse.add("tokens", mtasStatsTokensResponses);
            }
            if (mtasFields.doStatsPositions) {
              ArrayList<Object> mtasStatsPositionsResponses = new ArrayList<Object>();
              for (String field : mtasFields.list.keySet()) {
                for (ComponentPosition position : mtasFields.list
                    .get(field).statsPositionList) {
                  if (rb.req.getParams().getBool("isShard", false)) {
                    mtasStatsPositionsResponses
                        .add(searchStats.createPosition(position, true));
                  } else {
                    mtasStatsPositionsResponses
                        .add(searchStats.createPosition(position, false));
                  }
                }
              }
              mtasStatsResponse.add("positions", mtasStatsPositionsResponses);
            }
            if (mtasFields.doStatsSpans) {
              ArrayList<Object> mtasStatsSpansResponses = new ArrayList<Object>();
              for (String field : mtasFields.list.keySet()) {
                for (ComponentSpan span : mtasFields.list
                    .get(field).statsSpanList) {
                  if (rb.req.getParams().getBool("isShard", false)) {
                    mtasStatsSpansResponses
                        .add(searchStats.createSpan(span, true));
                  } else {
                    mtasStatsSpansResponses
                        .add(searchStats.createSpan(span, false));
                  }
                }
              }
              mtasStatsResponse.add("spans", mtasStatsSpansResponses);
            }
            // add to response
            mtasResponse.add("stats", mtasStatsResponse);
          }
        }
        // add to response
        rb.rsp.add("mtas", mtasResponse);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#modifyRequest(org.apache.
   * solr.handler.component.ResponseBuilder,
   * org.apache.solr.handler.component.SearchComponent,
   * org.apache.solr.handler.component.ShardRequest)
   */
  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq) {
    // System.out.println(Thread.currentThread().getId() + " - "
    // + rb.req.getParams().getBool("isShard", false) + " MODIFY REQUEST "
    // + rb.stage + " " + rb.req.getParamString());
    if (sreq.params.getBool(PARAM_MTAS, false)) {
      if (sreq.params.getBool(MtasSolrComponentStats.PARAM_MTAS_STATS, false)) {
        searchStats.modifyRequest(rb, who, sreq);
      }
      if (sreq.params.getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR,
          false)) {
        searchTermvector.modifyRequest(rb, who, sreq);
      }
      if (sreq.params.getBool(MtasSolrComponentPrefix.PARAM_MTAS_PREFIX,
          false)) {
        searchPrefix.modifyRequest(rb, who, sreq);
      }
      if (sreq.params.getBool(MtasSolrComponentFacet.PARAM_MTAS_FACET, false)) {
        searchFacet.modifyRequest(rb, who, sreq);
      }
      if (sreq.params.getBool(MtasSolrComponentGroup.PARAM_MTAS_GROUP, false)) {
        searchGroup.modifyRequest(rb, who, sreq);
      }
      if (sreq.params.getBool(MtasSolrComponentList.PARAM_MTAS_LIST, false)) {
        searchList.modifyRequest(rb, who, sreq);
      }
      if (sreq.params.getBool(MtasSolrComponentKwic.PARAM_MTAS_KWIC, false)) {
        searchKwic.modifyRequest(rb, who, sreq);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.component.SearchComponent#handleResponses(org.
   * apache.solr.handler.component.ResponseBuilder,
   * org.apache.solr.handler.component.ShardRequest)
   */
  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    // System.out
    // .println(System.nanoTime() + " - " + Thread.currentThread().getId()
    // + " - " + rb.req.getParams().getBool("isShard", false)
    // + " HANDLERESPONSES " + rb.stage + " " + rb.req.getParamString());
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#finishStage(org.apache.
   * solr.handler.component.ResponseBuilder)
   */
  @Override
  public void finishStage(ResponseBuilder rb) {
    // System.out
    // .println(System.nanoTime() + " - " + Thread.currentThread().getId()
    // + " - " + rb.req.getParams().getBool("isShard", false)
    // + " FINISHRESPONSES " + rb.stage + " " + rb.req.getParamString());
    if (rb.req.getParams().getBool(PARAM_MTAS, false)) {
      if (rb.req.getParams().getBool(MtasSolrComponentStats.PARAM_MTAS_STATS,
          false)) {
        searchStats.finishStage(rb);
      }
      if (rb.req.getParams()
          .getBool(MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
        searchTermvector.finishStage(rb);
      }
      if (rb.req.getParams().getBool(MtasSolrComponentPrefix.PARAM_MTAS_PREFIX,
          false)) {
        searchPrefix.finishStage(rb);
      }
      if (rb.req.getParams().getBool(MtasSolrComponentFacet.PARAM_MTAS_FACET,
          false)) {
        searchFacet.finishStage(rb);
      }
      if (rb.req.getParams().getBool(MtasSolrComponentGroup.PARAM_MTAS_GROUP,
          false)) {
        searchGroup.finishStage(rb);
      }
      if (rb.req.getParams().getBool(MtasSolrComponentList.PARAM_MTAS_LIST,
          false)) {
        searchList.finishStage(rb);
      }
      if (rb.req.getParams().getBool(MtasSolrComponentKwic.PARAM_MTAS_KWIC,
          false)) {
        searchKwic.finishStage(rb);
      }
      mtasSolrResultMerge.merge(rb);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.component.SearchComponent#distributedProcess(org.
   * apache.solr.handler.component.ResponseBuilder)
   */
  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    // System.out.println(Thread.currentThread().getId() + " - "
    // + rb.req.getParams().getBool("isShard", false) + " DISTIRBUTEDPROCESS "
    // + rb.stage + " " + rb.req.getParamString());
    // distributed processes
    if (rb.req.getParams().getBool(PARAM_MTAS, false)) {
      if (rb.stage == STAGE_TERMVECTOR_MISSING_TOP) {
        ComponentFields mtasFields = getMtasFields(rb);
        searchTermvector.distributedProcessMissingTop(rb, mtasFields);
      } else if (rb.stage == STAGE_TERMVECTOR_MISSING_KEY) {
        ComponentFields mtasFields = getMtasFields(rb);
        searchTermvector.distributedProcessMissingKey(rb, mtasFields);
      } else if (rb.stage == STAGE_TERMVECTOR_FINISH) {
        ComponentFields mtasFields = getMtasFields(rb);
        searchTermvector.distributedProcessFinish(rb, mtasFields);
      } else if (rb.stage == STAGE_LIST) {
        ComponentFields mtasFields = getMtasFields(rb);
        searchList.distributedProcess(rb, mtasFields);
      } else if (rb.stage == STAGE_PREFIX) {
        ComponentFields mtasFields = getMtasFields(rb);
        searchPrefix.distributedProcess(rb, mtasFields);
      } else if (rb.stage == STAGE_STATS) {
        ComponentFields mtasFields = getMtasFields(rb);
        searchStats.distributedProcess(rb, mtasFields);
      } else if (rb.stage == STAGE_FACET) {
        ComponentFields mtasFields = getMtasFields(rb);
        searchFacet.distributedProcess(rb, mtasFields);
      } else if (rb.stage == STAGE_GROUP) {
        ComponentFields mtasFields = getMtasFields(rb);
        searchGroup.distributedProcess(rb, mtasFields);
      }
      // compute new stage and return if not finished
      if (rb.stage >= ResponseBuilder.STAGE_EXECUTE_QUERY
          && rb.stage <= ResponseBuilder.STAGE_GET_FIELDS) {
        if (rb.stage < STAGE_TERMVECTOR_MISSING_TOP
            && rb.req.getParams().getBool(
                MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
          return STAGE_TERMVECTOR_MISSING_TOP;
        } else if (rb.stage < STAGE_TERMVECTOR_MISSING_KEY
            && rb.req.getParams().getBool(
                MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
          return STAGE_TERMVECTOR_MISSING_KEY;
        } else if (rb.stage < STAGE_TERMVECTOR_FINISH
            && rb.req.getParams().getBool(
                MtasSolrComponentTermvector.PARAM_MTAS_TERMVECTOR, false)) {
          return STAGE_TERMVECTOR_FINISH;
        } else if (rb.stage < STAGE_LIST && rb.req.getParams()
            .getBool(MtasSolrComponentList.PARAM_MTAS_LIST, false)) {
          return STAGE_LIST;
        } else if (rb.stage < STAGE_PREFIX && rb.req.getParams()
            .getBool(MtasSolrComponentPrefix.PARAM_MTAS_PREFIX, false)) {
          return STAGE_PREFIX;
        } else if (rb.stage < STAGE_STATS && rb.req.getParams()
            .getBool(MtasSolrComponentStats.PARAM_MTAS_STATS, false)) {
          return STAGE_STATS;
        } else if (rb.stage < STAGE_FACET && rb.req.getParams()
            .getBool(MtasSolrComponentFacet.PARAM_MTAS_FACET, false)) {
          return STAGE_FACET;
        } else if (rb.stage < STAGE_GROUP && rb.req.getParams()
            .getBool(MtasSolrComponentGroup.PARAM_MTAS_GROUP, false)) {
          return STAGE_GROUP;
        }
      }

    }
    return ResponseBuilder.STAGE_DONE;
  }

  /**
   * Gets the mtas fields.
   *
   * @param rb
   *          the rb
   * @return the mtas fields
   */

  /**
   * Gets the mtas fields.
   *
   * @param rb
   *          the rb
   * @return the mtas fields
   */
  private ComponentFields getMtasFields(ResponseBuilder rb) {
    return (ComponentFields) rb.req.getContext().get(ComponentFields.class);
  }

}
