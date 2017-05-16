package mtas.solr.handler.component.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;

import mtas.analysis.token.MtasToken;
import mtas.codec.util.CodecUtil;
import mtas.search.spans.util.MtasSpanQuery;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentFields;
import mtas.codec.util.CodecComponent.ComponentKwic;
import mtas.codec.util.CodecComponent.KwicHit;
import mtas.codec.util.CodecComponent.KwicToken;
import mtas.solr.handler.component.MtasSolrSearchComponent;

/**
 * The Class MtasSolrComponentKwic.
 */
public class MtasSolrComponentKwic implements MtasSolrComponent<ComponentKwic> {

  /** The Constant PARAM_MTAS_KWIC. */
  public static final String PARAM_MTAS_KWIC = MtasSolrSearchComponent.PARAM_MTAS
      + ".kwic";

  /** The Constant NAME_MTAS_KWIC_FIELD. */
  public static final String NAME_MTAS_KWIC_FIELD = "field";

  /** The Constant NAME_MTAS_KWIC_QUERY_TYPE. */
  public static final String NAME_MTAS_KWIC_QUERY_TYPE = "query.type";

  /** The Constant NAME_MTAS_KWIC_QUERY_VALUE. */
  public static final String NAME_MTAS_KWIC_QUERY_VALUE = "query.value";

  /** The Constant NAME_MTAS_KWIC_QUERY_PREFIX. */
  public static final String NAME_MTAS_KWIC_QUERY_PREFIX = "query.prefix";

  /** The Constant NAME_MTAS_KWIC_QUERY_IGNORE. */
  public static final String NAME_MTAS_KWIC_QUERY_IGNORE = "query.ignore";

  /** The Constant NAME_MTAS_KWIC_QUERY_MAXIMUM_IGNORE_LENGTH. */
  public static final String NAME_MTAS_KWIC_QUERY_MAXIMUM_IGNORE_LENGTH = "query.maximumIgnoreLength";

  /** The Constant NAME_MTAS_KWIC_QUERY_VARIABLE. */
  public static final String NAME_MTAS_KWIC_QUERY_VARIABLE = "query.variable";

  /** The Constant SUBNAME_MTAS_KWIC_QUERY_VARIABLE_NAME. */
  public static final String SUBNAME_MTAS_KWIC_QUERY_VARIABLE_NAME = "name";

  /** The Constant SUBNAME_MTAS_KWIC_QUERY_VARIABLE_VALUE. */
  public static final String SUBNAME_MTAS_KWIC_QUERY_VARIABLE_VALUE = "value";

  /** The Constant NAME_MTAS_KWIC_KEY. */
  public static final String NAME_MTAS_KWIC_KEY = "key";

  /** The Constant NAME_MTAS_KWIC_PREFIX. */
  public static final String NAME_MTAS_KWIC_PREFIX = "prefix";

  /** The Constant NAME_MTAS_KWIC_NUMBER. */
  public static final String NAME_MTAS_KWIC_NUMBER = "number";

  /** The Constant NAME_MTAS_KWIC_START. */
  public static final String NAME_MTAS_KWIC_START = "start";

  /** The Constant NAME_MTAS_KWIC_LEFT. */
  public static final String NAME_MTAS_KWIC_LEFT = "left";

  /** The Constant NAME_MTAS_KWIC_RIGHT. */
  public static final String NAME_MTAS_KWIC_RIGHT = "right";

  /** The Constant NAME_MTAS_KWIC_OUTPUT. */
  public static final String NAME_MTAS_KWIC_OUTPUT = "output";

  /**
   * Instantiates a new mtas solr component kwic.
   *
   * @param searchComponent the search component
   */
  public MtasSolrComponentKwic(MtasSolrSearchComponent searchComponent) {
  }

  /**
   * Gets the positive integer.
   *
   * @param number the number
   * @return the positive integer
   */
  private int getPositiveInteger(String number) {
    try {
      return Math.max(0, Integer.parseInt(number));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#prepare(org.apache.solr.handler.component.ResponseBuilder, mtas.codec.util.CodecComponent.ComponentFields)
   */
  public void prepare(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    Set<String> ids = MtasSolrResultUtil
        .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_KWIC);
    if (!ids.isEmpty()) {
      int tmpCounter = 0;
      String[] fields = new String[ids.size()];
      String[] queryTypes = new String[ids.size()];
      String[] queryValues = new String[ids.size()];
      String[] queryPrefixes = new String[ids.size()];
      String[] queryIgnores = new String[ids.size()];
      String[] queryMaximumIgnoreLengths = new String[ids.size()];
      HashMap<String, String[]>[] queryVariables = new HashMap[ids.size()];
      String[] keys = new String[ids.size()];
      String[] prefixes = new String[ids.size()];
      String[] numbers = new String[ids.size()];
      String[] starts = new String[ids.size()];
      String[] lefts = new String[ids.size()];
      String[] rights = new String[ids.size()];
      String[] outputs = new String[ids.size()];
      for (String id : ids) {
        fields[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_FIELD, null);
        queryTypes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_QUERY_TYPE, null);
        queryValues[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_QUERY_VALUE,
            null);
        queryPrefixes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_QUERY_PREFIX,
            null);
        queryIgnores[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_QUERY_IGNORE,
            null);
        queryMaximumIgnoreLengths[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "."
                + NAME_MTAS_KWIC_QUERY_MAXIMUM_IGNORE_LENGTH, null);
        Set<String> vIds = MtasSolrResultUtil.getIdsFromParameters(
            rb.req.getParams(),
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_QUERY_VARIABLE);
        queryVariables[tmpCounter] = new HashMap<>();
        if (!vIds.isEmpty()) {
          HashMap<String, ArrayList<String>> tmpVariables = new HashMap<>();
          for (String vId : vIds) {
            String name = rb.req.getParams().get(
                PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_QUERY_VARIABLE
                    + "." + vId + "." + SUBNAME_MTAS_KWIC_QUERY_VARIABLE_NAME,
                null);
            if (name != null) {
              if (!tmpVariables.containsKey(name)) {
                tmpVariables.put(name, new ArrayList<String>());
              }
              String value = rb.req.getParams()
                  .get(PARAM_MTAS_KWIC + "." + id + "."
                      + NAME_MTAS_KWIC_QUERY_VARIABLE + "." + vId + "."
                      + SUBNAME_MTAS_KWIC_QUERY_VARIABLE_VALUE, null);
              if (value != null) {
                ArrayList<String> list = new ArrayList<>();
                String[] subList = value.split("(?<!\\\\),");
                for (int i = 0; i < subList.length; i++) {
                  list.add(
                      subList[i].replace("\\,", ",").replace("\\\\", "\\"));
                }
                tmpVariables.get(name).addAll(list);
              }
            }
          }
          for (Entry<String, ArrayList<String>> entry : tmpVariables
              .entrySet()) {
            queryVariables[tmpCounter].put(entry.getKey(),
                entry.getValue().toArray(new String[entry.getValue().size()]));
          }
        }
        keys[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_KEY,
                String.valueOf(tmpCounter))
            .trim();
        prefixes[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_PREFIX, null);
        numbers[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_NUMBER, null);
        starts[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_START, null);
        lefts[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_LEFT, null);
        rights[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_RIGHT, null);
        starts[tmpCounter] = rb.req.getParams()
            .get(PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_START, null);
        outputs[tmpCounter] = rb.req.getParams().get(
            PARAM_MTAS_KWIC + "." + id + "." + NAME_MTAS_KWIC_OUTPUT, null);
        tmpCounter++;
      }
      String uniqueKeyField = rb.req.getSchema().getUniqueKeyField().getName();
      mtasFields.doKwic = true;
      rb.setNeedDocList(true);
      for (String field : fields) {
        if (field == null || field.isEmpty()) {
          throw new IOException("no (valid) field in mtas kwic");
        } else if (!mtasFields.list.containsKey(field)) {
          mtasFields.list.put(field, new ComponentField(uniqueKeyField));
        }
      }
      MtasSolrResultUtil.compareAndCheck(keys, fields, NAME_MTAS_KWIC_KEY,
          NAME_MTAS_KWIC_FIELD, true);
      MtasSolrResultUtil.compareAndCheck(queryValues, fields,
          NAME_MTAS_KWIC_QUERY_VALUE, NAME_MTAS_KWIC_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(queryTypes, fields,
          NAME_MTAS_KWIC_QUERY_TYPE, NAME_MTAS_KWIC_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(queryPrefixes, fields,
          NAME_MTAS_KWIC_QUERY_PREFIX, NAME_MTAS_KWIC_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(queryIgnores, fields,
          NAME_MTAS_KWIC_QUERY_IGNORE, NAME_MTAS_KWIC_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(queryMaximumIgnoreLengths, fields,
          NAME_MTAS_KWIC_QUERY_MAXIMUM_IGNORE_LENGTH, NAME_MTAS_KWIC_FIELD,
          false);
      MtasSolrResultUtil.compareAndCheck(prefixes, fields,
          NAME_MTAS_KWIC_PREFIX, NAME_MTAS_KWIC_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(numbers, fields, NAME_MTAS_KWIC_NUMBER,
          NAME_MTAS_KWIC_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(starts, fields, NAME_MTAS_KWIC_START,
          NAME_MTAS_KWIC_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(lefts, fields, NAME_MTAS_KWIC_LEFT,
          NAME_MTAS_KWIC_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(rights, fields, NAME_MTAS_KWIC_RIGHT,
          NAME_MTAS_KWIC_FIELD, false);
      MtasSolrResultUtil.compareAndCheck(outputs, fields, NAME_MTAS_KWIC_OUTPUT,
          NAME_MTAS_KWIC_FIELD, false);
      for (int i = 0; i < fields.length; i++) {
        ComponentField cf = mtasFields.list.get(fields[i]);
        Integer maximumIgnoreLength = (queryMaximumIgnoreLengths[i] == null)
            ? null : Integer.parseInt(queryMaximumIgnoreLengths[i]);
        MtasSpanQuery q = MtasSolrResultUtil.constructQuery(queryValues[i],
            queryTypes[i], queryPrefixes[i], queryVariables[i], fields[i],
            queryIgnores[i], maximumIgnoreLength);
        // minimize number of queries
        if (cf.spanQueryList.contains(q)) {
          q = cf.spanQueryList.get(cf.spanQueryList.indexOf(q));
        } else {
          cf.spanQueryList.add(q);
        }
        String key = (keys[i] == null) || (keys[i].isEmpty())
            ? String.valueOf(i) + ":" + fields[i] + ":" + queryValues[i]
            : keys[i].trim();
        String prefix = prefixes[i];
        Integer number = (numbers[i] != null) ? getPositiveInteger(numbers[i])
            : null;
        int start = getPositiveInteger(starts[i]);
        int left = getPositiveInteger(lefts[i]);
        int right = getPositiveInteger(rights[i]);
        String output = outputs[i];
        mtasFields.list.get(fields[i]).kwicList.add(new ComponentKwic(q, key,
            prefix, number, start, left, right, output));
      }
    }
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#create(mtas.codec.util.CodecComponent.BasicComponent, java.lang.Boolean)
   */
  public SimpleOrderedMap<Object> create(ComponentKwic kwic, Boolean encode) {
    SimpleOrderedMap<Object> mtasKwicResponse = new SimpleOrderedMap<>();
    mtasKwicResponse.add("key", kwic.key);
    ArrayList<NamedList<Object>> mtasKwicItemResponses = new ArrayList<>();
    if (kwic.output.equals(ComponentKwic.KWIC_OUTPUT_HIT)) {
      for (int docId : kwic.hits.keySet()) {
        NamedList<Object> mtasKwicItemResponse = new SimpleOrderedMap<>();
        List<KwicHit> list = kwic.hits.get(docId);
        List<NamedList<Object>> mtasKwicItemResponseItems = new ArrayList<>();
        for (KwicHit h : list) {
          NamedList<Object> mtasKwicItemResponseItem = new SimpleOrderedMap<>();
          SortedMap<Integer, List<List<String>>> hitData = new TreeMap<>();
          SortedMap<Integer, List<List<String>>> leftData = null;
          SortedMap<Integer, List<List<String>>> rightData = null;
          if (kwic.left > 0) {
            leftData = new TreeMap<>();
          }
          if (kwic.right > 0) {
            rightData = new TreeMap<>();
          }
          for (int position = Math.max(0,
              h.startPosition - kwic.left); position <= (h.endPosition
                  + kwic.right); position++) {
            if (h.hits.containsKey(position)) {
              List<List<String>> hitDataItem = new ArrayList<>();
              for (String term : h.hits.get(position)) {
                List<String> hitDataSubItem = new ArrayList<>();
                hitDataSubItem.add(CodecUtil.termPrefix(term));
                hitDataSubItem.add(CodecUtil.termValue(term));
                hitDataItem.add(hitDataSubItem);
              }
              if (position < h.startPosition) {
                if (leftData != null) {
                  leftData.put(position, hitDataItem);
                }
              } else if (position > h.endPosition) {
                if (rightData != null) {
                  rightData.put(position, hitDataItem);
                }
              } else {
                hitData.put(position, hitDataItem);
              }
            }
          }
          if (kwic.left > 0) {
            mtasKwicItemResponseItem.add("left", leftData);
          }
          mtasKwicItemResponseItem.add("hit", hitData);
          if (kwic.right > 0) {
            mtasKwicItemResponseItem.add("right", rightData);
          }
          mtasKwicItemResponseItems.add(mtasKwicItemResponseItem);
        }
        mtasKwicItemResponse.add("documentKey", kwic.uniqueKey.get(docId));
        mtasKwicItemResponse.add("documentTotal", kwic.subTotal.get(docId));
        mtasKwicItemResponse.add("documentMinPosition",
            kwic.minPosition.get(docId));
        mtasKwicItemResponse.add("documentMaxPosition",
            kwic.maxPosition.get(docId));
        mtasKwicItemResponse.add("list", mtasKwicItemResponseItems);
        mtasKwicItemResponses.add(mtasKwicItemResponse);
      }
    } else if (kwic.output.equals(ComponentKwic.KWIC_OUTPUT_TOKEN)) {
      for (int docId : kwic.tokens.keySet()) {
        NamedList<Object> mtasKwicItemResponse = new SimpleOrderedMap<>();
        List<KwicToken> list = kwic.tokens.get(docId);
        List<NamedList<Object>> mtasKwicItemResponseItems = new ArrayList<>();
        for (KwicToken k : list) {
          NamedList<Object> mtasKwicItemResponseItem = new SimpleOrderedMap<>();
          mtasKwicItemResponseItem.add("startPosition", k.startPosition);
          mtasKwicItemResponseItem.add("endPosition", k.endPosition);
          ArrayList<NamedList<Object>> mtasKwicItemResponseItemTokens = new ArrayList<>();
          for (MtasToken token : k.tokens) {
            NamedList<Object> mtasKwicItemResponseItemToken = new SimpleOrderedMap<>();
            if (token.getId() != null) {
              mtasKwicItemResponseItemToken.add("mtasId", token.getId());
            }
            mtasKwicItemResponseItemToken.add("prefix", token.getPrefix());
            mtasKwicItemResponseItemToken.add("value", token.getPostfix());
            if (token.getPositionStart() != null) {
              mtasKwicItemResponseItemToken.add("positionStart",
                  token.getPositionStart());
              mtasKwicItemResponseItemToken.add("positionEnd",
                  token.getPositionEnd());
            }
            if (token.getPositions() != null) {
              mtasKwicItemResponseItemToken.add("positions",
                  token.getPositions());
            }
            if (token.getParentId() != null) {
              mtasKwicItemResponseItemToken.add("parentMtasId",
                  token.getParentId());
            }
            if (token.getPayload() != null) {
              mtasKwicItemResponseItemToken.add("payload", token.getPayload());
            }
            if (token.getOffsetStart() != null) {
              mtasKwicItemResponseItemToken.add("offsetStart",
                  token.getOffsetStart());
              mtasKwicItemResponseItemToken.add("offsetEnd",
                  token.getOffsetEnd());
            }
            if (token.getRealOffsetStart() != null) {
              mtasKwicItemResponseItemToken.add("realOffsetStart",
                  token.getRealOffsetStart());
              mtasKwicItemResponseItemToken.add("realOffsetEnd",
                  token.getRealOffsetEnd());
            }
            mtasKwicItemResponseItemTokens.add(mtasKwicItemResponseItemToken);
          }
          mtasKwicItemResponseItem.add("tokens",
              mtasKwicItemResponseItemTokens);
          mtasKwicItemResponseItems.add(mtasKwicItemResponseItem);
        }
        mtasKwicItemResponse.add("documentKey", kwic.uniqueKey.get(docId));
        mtasKwicItemResponse.add("documentTotal", kwic.subTotal.get(docId));
        mtasKwicItemResponse.add("documentMinPosition",
            kwic.minPosition.get(docId));
        mtasKwicItemResponse.add("documentMaxPosition",
            kwic.maxPosition.get(docId));
        mtasKwicItemResponse.add("list", mtasKwicItemResponseItems);
        mtasKwicItemResponses.add(mtasKwicItemResponse);
      }
    }
    mtasKwicResponse.add("list", mtasKwicItemResponses);
    return mtasKwicResponse;
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#modifyRequest(org.apache.solr.handler.component.ResponseBuilder, org.apache.solr.handler.component.SearchComponent, org.apache.solr.handler.component.ShardRequest)
   */
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,
      ShardRequest sreq) {
    if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)) {
      if (sreq.params.getBool(PARAM_MTAS_KWIC, false)
          && (sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
        // do nothing
      } else {
        Set<String> keys = MtasSolrResultUtil
            .getIdsFromParameters(rb.req.getParams(), PARAM_MTAS_KWIC);
        sreq.params.remove(PARAM_MTAS_KWIC);
        for (String key : keys) {
          sreq.params
              .remove(PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_FIELD);
          sreq.params.remove(
              PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_QUERY_TYPE);
          sreq.params.remove(
              PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_QUERY_VALUE);
          sreq.params.remove(
              PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_QUERY_PREFIX);
          sreq.params.remove(
              PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_QUERY_IGNORE);
          sreq.params.remove(PARAM_MTAS_KWIC + "." + key + "."
              + NAME_MTAS_KWIC_QUERY_MAXIMUM_IGNORE_LENGTH);
          sreq.params
              .remove(PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_KEY);
          sreq.params.remove(
              PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_PREFIX);
          sreq.params.remove(
              PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_NUMBER);
          sreq.params
              .remove(PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_LEFT);
          sreq.params
              .remove(PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_RIGHT);
          sreq.params.remove(
              PARAM_MTAS_KWIC + "." + key + "." + NAME_MTAS_KWIC_OUTPUT);
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#finishStage(org.apache.solr.handler.component.ResponseBuilder)
   */
  public void finishStage(ResponseBuilder rb) {
    if (rb.req.getParams().getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
        && rb.stage >= ResponseBuilder.STAGE_EXECUTE_QUERY
        && rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
      for (ShardRequest sreq : rb.finished) {
        if (sreq.params.getBool(MtasSolrSearchComponent.PARAM_MTAS, false)
            && sreq.params.getBool(PARAM_MTAS_KWIC, false)) {
          // nothing to do
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see mtas.solr.handler.component.util.MtasSolrComponent#distributedProcess(org.apache.solr.handler.component.ResponseBuilder, mtas.codec.util.CodecComponent.ComponentFields)
   */
  public void distributedProcess(ResponseBuilder rb, ComponentFields mtasFields)
      throws IOException {
    // nothing to do
  }

}
