package mtas.analysis.parser;

import mtas.analysis.token.MtasToken;
import mtas.analysis.token.MtasTokenIdFactory;
import mtas.analysis.token.MtasTokenString;
import mtas.analysis.util.Configuration;
import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasParserException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.util.BytesRef;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.singletonList;

public abstract class MtasBasicParser extends MtasParser {
  private static final Log log = LogFactory.getLog(MtasBasicParser.class);
  static final String MAPPING_TYPE_REF = "ref";
  static final String MAPPING_TYPE_RELATION = "relation";
  static final String MAPPING_TYPE_RELATION_ANNOTATION = "relationAnnotation";
  static final String MAPPING_TYPE_GROUP = "group";
  static final String MAPPING_TYPE_GROUP_ANNOTATION = "groupAnnotation";
  static final String MAPPING_TYPE_WORD = "word";
  static final String MAPPING_TYPE_WORD_ANNOTATION = "wordAnnotation";
  static final String ITEM_TYPE_STRING = "string";
  static final String ITEM_TYPE_NAME = "name";
  static final String ITEM_TYPE_NAME_ANCESTOR = "ancestorName";
  static final String ITEM_TYPE_NAME_ANCESTOR_GROUP = "ancestorGroupName";
  static final String ITEM_TYPE_NAME_ANCESTOR_GROUP_ANNOTATION = "ancestorGroupAnnotationName";
  static final String ITEM_TYPE_NAME_ANCESTOR_WORD = "ancestorWordName";
  static final String ITEM_TYPE_NAME_ANCESTOR_WORD_ANNOTATION = "ancestorWordAnnotationName";
  static final String ITEM_TYPE_NAME_ANCESTOR_RELATION = "ancestorRelationName";
  static final String ITEM_TYPE_NAME_ANCESTOR_RELATION_ANNOTATION = "ancestorRelationAnnotationName";
  static final String ITEM_TYPE_ATTRIBUTE = "attribute";
  static final String ITEM_TYPE_ATTRIBUTE_ANCESTOR = "ancestorAttribute";
  static final String ITEM_TYPE_ATTRIBUTE_ANCESTOR_GROUP = "ancestorGroupAttribute";
  static final String ITEM_TYPE_ATTRIBUTE_ANCESTOR_GROUP_ANNOTATION = "ancestorGroupAnnotationAttribute";
  static final String ITEM_TYPE_ATTRIBUTE_ANCESTOR_WORD = "ancestorWordAttribute";
  static final String ITEM_TYPE_ATTRIBUTE_ANCESTOR_WORD_ANNOTATION = "ancestorWordAnnotationAttribute";
  static final String ITEM_TYPE_ATTRIBUTE_ANCESTOR_RELATION = "ancestorRelationAttribute";
  static final String ITEM_TYPE_ATTRIBUTE_ANCESTOR_RELATION_ANNOTATION =
    "ancestorRelationAnnotationAttribute";
  static final String ITEM_TYPE_TEXT = "text";
  static final String ITEM_TYPE_TEXT_SPLIT = "textSplit";
  static final String ITEM_TYPE_UNKNOWN_ANCESTOR = "unknownAncestor";
  static final String ITEM_TYPE_ANCESTOR = "ancestor";
  static final String ITEM_TYPE_ANCESTOR_GROUP = "ancestorGroup";
  static final String ITEM_TYPE_ANCESTOR_GROUP_ANNOTATION = "ancestorGroupAnnotation";
  static final String ITEM_TYPE_ANCESTOR_WORD = "ancestorWord";
  static final String ITEM_TYPE_ANCESTOR_WORD_ANNOTATION = "ancestorWordAnnotation";
  static final String ITEM_TYPE_ANCESTOR_RELATION = "ancestorRelation";
  static final String ITEM_TYPE_ANCESTOR_RELATION_ANNOTATION = "ancestorRelationAnnotation";
  static final String ITEM_TYPE_VARIABLE_FROM_ATTRIBUTE = "variableFromAttribute";
  static final String VARIABLE_SUBTYPE_VALUE = "value";
  static final String VARIABLE_SUBTYPE_VALUE_ITEM = "item";
  static final String MAPPING_SUBTYPE_TOKEN = "token";
  static final String MAPPING_SUBTYPE_TOKEN_PRE = "pre";
  static final String MAPPING_SUBTYPE_TOKEN_POST = "post";
  static final String MAPPING_SUBTYPE_PAYLOAD = "payload";
  static final String MAPPING_SUBTYPE_CONDITION = "condition";
  static final String MAPPING_FILTER_UPPERCASE = "uppercase";
  static final String MAPPING_FILTER_LOWERCASE = "lowercase";
  static final String MAPPING_FILTER_ASCII = "ascii";
  static final String MAPPING_FILTER_SPLIT = "split";
  static final String UPDATE_TYPE_OFFSET = "offsetUpdate";
  static final String UPDATE_TYPE_POSITION = "positionUpdate";
  static final String UPDATE_TYPE_VARIABLE = "variableUpdate";
  static final String UPDATE_TYPE_LOCAL_REF_OFFSET_START = "localRefOffsetStartUpdate";
  static final String UPDATE_TYPE_LOCAL_REF_OFFSET_END = "localRefOffsetEndUpdate";
  static final String UPDATE_TYPE_LOCAL_REF_POSITION_START = "localRefPositionStartUpdate";
  static final String UPDATE_TYPE_LOCAL_REF_POSITION_END = "localRefPositionEndUpdate";
  static final String MAPPING_VALUE_VALUE = "value";
  static final String MAPPING_VALUE_TYPE = "type";
  static final String MAPPING_VALUE_NAME = "name";
  static final String MAPPING_VALUE_NAMESPACE = "namespace";
  static final String MAPPING_VALUE_PREFIX = "prefix";
  static final String MAPPING_VALUE_FILTER = "filter";
  static final String MAPPING_VALUE_DISTANCE = "distance";
  static final String MAPPING_VALUE_SOURCE = "source";
  static final String MAPPING_VALUE_ANCESTOR = "ancestor";
  static final String MAPPING_VALUE_SPLIT = "split";
  static final String MAPPING_VALUE_NUMBER = "number";
  static final String MAPPING_VALUE_CONDITION = "condition";
  static final String MAPPING_VALUE_TEXT = "text";
  static final String MAPPING_VALUE_NOT = "not";

  private Base64.Encoder enc = Base64.getEncoder();

  private Base64.Decoder dec = Base64.getDecoder();

  MtasBasicParser(Configuration config) {
    super(config);
  }

  Map<String, List<MtasParserObject>> createCurrentList() {
    Map<String, List<MtasParserObject>> currentList = new HashMap<>();
    currentList.put(MAPPING_TYPE_RELATION, new ArrayList<>());
    currentList.put(MAPPING_TYPE_RELATION_ANNOTATION, new ArrayList<>());
    currentList.put(MAPPING_TYPE_REF, new ArrayList<>());
    currentList.put(MAPPING_TYPE_GROUP, new ArrayList<>());
    currentList.put(MAPPING_TYPE_GROUP_ANNOTATION, new ArrayList<>());
    currentList.put(MAPPING_TYPE_WORD, new ArrayList<>());
    currentList.put(MAPPING_TYPE_WORD_ANNOTATION, new ArrayList<>());
    return currentList;
  }

  Map<String, Map<Integer, Set<String>>> createUpdateList() {
    Map<String, Map<Integer, Set<String>>> updateList = new HashMap<>();
    updateList.put(UPDATE_TYPE_OFFSET, new HashMap<>());
    updateList.put(UPDATE_TYPE_POSITION, new HashMap<>());
    updateList.put(UPDATE_TYPE_LOCAL_REF_POSITION_START, new HashMap<>());
    updateList.put(UPDATE_TYPE_LOCAL_REF_POSITION_END, new HashMap<>());
    updateList.put(UPDATE_TYPE_LOCAL_REF_OFFSET_START, new HashMap<>());
    updateList.put(UPDATE_TYPE_LOCAL_REF_OFFSET_END, new HashMap<>());
    updateList.put(UPDATE_TYPE_VARIABLE, new HashMap<>());
    return updateList;
  }

  Map<String, Map<String, String>> createVariables() {
    return new HashMap<>();
  }

  void computeMappingsFromObject(
    MtasTokenIdFactory mtasTokenIdFactory, MtasParserObject object,
    Map<String, List<MtasParserObject>> currentList,
    Map<String, Map<Integer, Set<String>>> updateList)
      throws MtasParserException, MtasConfigException {
    MtasParserType<MtasParserMapping<?>> objectType = object.getType();
    List<MtasParserMapping<?>> mappings = objectType.getItems();
    if (!object.updateableMappingsWithPosition.isEmpty()) {
      for (int tokenId : object.updateableMappingsWithPosition) {
        updateList.get(UPDATE_TYPE_POSITION).put(tokenId, object.getRefIds());
      }
    }
    if (!object.updateableMappingsWithOffset.isEmpty()) {
      for (int tokenId : object.updateableMappingsWithOffset) {
        updateList.get(UPDATE_TYPE_OFFSET).put(tokenId, object.getRefIds());
      }
    }
    for (MtasParserMapping<?> mapping : mappings) {
      try {
        if (mapping.getTokens().isEmpty()) {
          // empty exception
        } else {
          for (int i = 0; i < mapping.getTokens().size(); i++) {
            MtasParserMappingToken mappingToken = mapping.getTokens().get(i);
            // empty exception
            if (mappingToken.preValues.isEmpty()) {
              // continue, but no token
            } else {
              // check conditions
              postcheckMappingConditions(object, mapping.getConditions(), currentList);
              boolean containsVariables = checkForVariables(mappingToken.preValues);
              if (!containsVariables) {
                containsVariables = checkForVariables(mappingToken.postValues);
              }
              // construct preValue
              String[] preValue = computeValueFromMappingValues(object,
                mappingToken.preValues, currentList, containsVariables);
              // at least preValue
              if (preValue == null || preValue.length == 0) {
                throw new MtasParserException("no preValues");
              } else {
                // no delimiter in preValue
                for (int k = 0; k < preValue.length; k++) {
                  if ((preValue[k] = preValue[k].replace(MtasToken.DELIMITER,
                    "")).isEmpty()) {
                    throw new MtasParserException("empty preValue");
                }
              }
              }

              String[] postValue = computeValueFromMappingValues(object,
                mappingToken.postValues, currentList, containsVariables);

              String[] value;
              if (postValue == null || postValue.length == 0) {
                value = preValue.clone();
                for (int k = 0; k < value.length; k++) {
                  value[k] = value[k] + MtasToken.DELIMITER;
                }
              } else if (postValue.length == 1) {
                value = preValue.clone();
                for (int k = 0; k < value.length; k++) {
                  value[k] = value[k] + MtasToken.DELIMITER + postValue[0];
                }
              } else if (preValue.length == 1) {
                value = postValue.clone();
                for (int k = 0; k < value.length; k++) {
                  value[k] = preValue[0] + MtasToken.DELIMITER + value[k];
                }
              } else {
                value = new String[preValue.length * postValue.length];
                int number = 0;
                for (String pre : preValue) {
                  for (String post : postValue) {
                    value[number] = pre + MtasToken.DELIMITER + post;
                    number++;
                }
                }
              }

              // construct payload
              BytesRef payload = computePayloadFromMappingPayload(object,
                mappingToken.payload, currentList);
              // create token and get id: from now on, we must continue, no
              // exceptions allowed...
              for (String v : value) {
                MtasTokenString token = new MtasTokenString(
                  mtasTokenIdFactory.createTokenId(), v);
                // store settings offset, realoffset and parent
                token.setProvideOffset(mappingToken.offset);
                token.setProvideRealOffset(mappingToken.realoffset);
                token.setProvideParentId(mappingToken.parent);
                String checkType = object.objectType.getType();
                // register token if it contains variables
                if (containsVariables) {
                  updateList.get(UPDATE_TYPE_VARIABLE).put(token.getId(), null);
                }
                // register id for update when parent is created
                if (!currentList.get(checkType).isEmpty()) {
                  if (currentList.get(checkType).contains(object)) {
                    int listPosition = currentList.get(checkType)
                                                  .indexOf(object);
                    if (listPosition > 0) {
                      currentList.get(checkType).get(listPosition - 1)
                                 .registerUpdateableMappingAtParent(token.getId());
                    }
                  } else {
                    currentList.get(checkType)
                               .get(currentList.get(checkType).size() - 1)
                               .registerUpdateableMappingAtParent(token.getId());
                  }
                  // if no real ancestor, register id update when group
                  // ancestor is created
                } else if (!currentList.get(MAPPING_TYPE_GROUP).isEmpty()) {
                  currentList.get(MAPPING_TYPE_GROUP)
                             .get(currentList.get(MAPPING_TYPE_GROUP).size() - 1)
                             .registerUpdateableMappingAtParent(token.getId());
                } else if (!currentList.get(MAPPING_TYPE_RELATION).isEmpty()) {
                  currentList.get(MAPPING_TYPE_RELATION)
                             .get(currentList.get(MAPPING_TYPE_RELATION).size() - 1)
                             .registerUpdateableMappingAtParent(token.getId());
                }
                // update children
                for (Integer tmpId : object.getUpdateableMappingsAsParent()) {
                  if (tokenCollection.get(tmpId) != null) {
                    tokenCollection.get(tmpId).setParentId(token.getId());
                }
                }
                object.resetUpdateableMappingsAsParent();
                // use own position
                if (mapping.position.equals(MtasParserMapping.SOURCE_OWN)) {
                  token.addPositions(object.getPositions());
                  // use position from ancestorGroup
                } else if (mapping.position.equals(MtasParserMapping.SOURCE_ANCESTOR_GROUP)
                  && (!currentList.get(MAPPING_TYPE_GROUP).isEmpty())) {
                  currentList.get(MAPPING_TYPE_GROUP)
                             .get(currentList.get(MAPPING_TYPE_GROUP).size() - 1)
                             .addUpdateableMappingWithPosition(token.getId());
                  // use position from ancestorWord
                } else if (mapping.position.equals(MtasParserMapping.SOURCE_ANCESTOR_WORD)
                  && (!currentList.get(MAPPING_TYPE_WORD).isEmpty())) {
                  currentList.get(MAPPING_TYPE_WORD)
                             .get(currentList.get(MAPPING_TYPE_WORD).size() - 1)
                             .addUpdateableMappingWithPosition(token.getId());
                  // use position from ancestorRelation
                } else if (mapping.position.equals(MtasParserMapping.SOURCE_ANCESTOR_RELATION)
                  && (!currentList.get(MAPPING_TYPE_RELATION).isEmpty())) {
                  currentList.get(MAPPING_TYPE_RELATION)
                             .get(currentList.get(MAPPING_TYPE_RELATION).size() - 1)
                             .addUpdateableMappingWithPosition(token.getId());
                  // register id to get positions later from references
                } else if (!mapping.position.equals(MtasParserMapping.SOURCE_REFS)) {
                  throw new IllegalStateException("should not happen");
                } else {
                  if (mapping.type.equals(MAPPING_TYPE_GROUP_ANNOTATION)) {
                    if (mapping.start != null && mapping.end != null) {
                      String start = object.getAttribute(mapping.start);
                      String end = object.getAttribute(mapping.end);
                      if (start != null && !start.isEmpty() && end != null && !end.isEmpty()) {
                        if (start.startsWith("#")) {
                          start = start.substring(1);
                        }
                        if (end.startsWith("#")) {
                          end = end.substring(1);
                        }
                        updateList.get(UPDATE_TYPE_LOCAL_REF_POSITION_START)
                                  .put(token.getId(), new HashSet<>(singletonList(start)));
                        updateList.get(UPDATE_TYPE_LOCAL_REF_POSITION_END)
                                  .put(token.getId(), new HashSet<>(singletonList(end)));
                        updateList.get(UPDATE_TYPE_LOCAL_REF_OFFSET_START)
                                  .put(token.getId(), new HashSet<>(singletonList(start)));
                        updateList.get(UPDATE_TYPE_LOCAL_REF_OFFSET_END)
                                  .put(token.getId(), new HashSet<>(singletonList(end)));
                    }
                  }
                } else {
                    updateList.get(UPDATE_TYPE_POSITION)
                              .put(token.getId(), object.getRefIds());
                }
                }
                // use own offset
                if (mapping.offset.equals(MtasParserMapping.SOURCE_OWN)) {
                  token.setOffset(object.getOffsetStart(), object.getOffsetEnd());
                  // use offset from ancestorGroup
                } else if (mapping.offset.equals(MtasParserMapping.SOURCE_ANCESTOR_GROUP)
                  && (!currentList.get(MAPPING_TYPE_GROUP).isEmpty())) {
                  currentList.get(MAPPING_TYPE_GROUP)
                             .get(currentList.get(MAPPING_TYPE_GROUP).size() - 1)
                             .addUpdateableMappingWithOffset(token.getId());
                  // use offset from ancestorWord
                } else if (mapping.offset.equals(MtasParserMapping.SOURCE_ANCESTOR_WORD)
                  && !currentList.get(MAPPING_TYPE_WORD).isEmpty()) {
                  currentList.get(MAPPING_TYPE_WORD)
                             .get(currentList.get(MAPPING_TYPE_WORD).size() - 1)
                             .addUpdateableMappingWithOffset(token.getId());
                  // use offset from ancestorRelation
                } else if (mapping.offset.equals(MtasParserMapping.SOURCE_ANCESTOR_RELATION)
                  && !currentList.get(MAPPING_TYPE_RELATION).isEmpty()) {
                  currentList.get(MAPPING_TYPE_RELATION)
                             .get(currentList.get(MAPPING_TYPE_RELATION).size() - 1)
                             .addUpdateableMappingWithOffset(token.getId());
                  // register id to get offset later from refs
                } else if (mapping.offset.equals(MtasParserMapping.SOURCE_REFS)) {
                  updateList.get(UPDATE_TYPE_OFFSET).put(token.getId(),
                    object.getRefIds());
                }
                // always use own realOffset
                token.setRealOffset(object.getRealOffsetStart(), object.getRealOffsetEnd());
                token.setPayload(payload);
                tokenCollection.add(token);
              }
            }
          }
        }
        // register start and end
        if (mapping.start != null && mapping.end != null) {
          String startAttribute = null;
          String endAttribute = null;
          if (mapping.start.equals("#")) {
            startAttribute = object.getId();
          } else {
            startAttribute = object.getAttribute(mapping.start);
            if (startAttribute != null && startAttribute.startsWith("#")) {
              startAttribute = startAttribute.substring(1);
            }
          }
          if (mapping.end.equals("#")) {
            endAttribute = object.getId();
          } else {
            endAttribute = object.getAttribute(mapping.end);
            if (endAttribute != null && endAttribute.startsWith("#")) {
              endAttribute = endAttribute.substring(1);
            }
          }
          if (startAttribute != null && endAttribute != null
            && !object.getPositions().isEmpty()) {
            object.setReferredStartPosition(startAttribute,
              object.getPositions().first());
            object.setReferredEndPosition(endAttribute,
              object.getPositions().last());
            object.setReferredStartOffset(startAttribute,
              object.getOffsetStart());
            object.setReferredEndOffset(endAttribute, object.getOffsetEnd());
          }
        }
      } catch (MtasParserException e) {
        log.debug("Rejected mapping " + object.getType().getName(), e);
        // ignore, no new token is created
      }
    }
    // copy remaining updateableMappings to new parent
    if (!currentList.get(objectType.getType()).isEmpty()) {
      if (currentList.get(objectType.getType()).contains(object)) {
        int listPosition = currentList.get(objectType.getType())
                                      .indexOf(object);
        if (listPosition > 0) {
          currentList.get(objectType.getType()).get(listPosition - 1)
                     .registerUpdateableMappingsAtParent(
                       object.getUpdateableMappingsAsParent());
        }
      } else {
        currentList.get(objectType.getType())
                   .get(currentList.get(objectType.getType()).size() - 1)
                   .registerUpdateableMappingsAtParent(
                     object.getUpdateableMappingsAsParent());
      }
    } else if (!currentList.get(MAPPING_TYPE_GROUP).isEmpty()) {
      currentList.get(MAPPING_TYPE_GROUP)
                 .get(currentList.get(MAPPING_TYPE_GROUP).size() - 1)
                 .registerUpdateableMappingsAtParent(
                   object.getUpdateableMappingsAsParent());
    } else if (!currentList.get(MAPPING_TYPE_RELATION).isEmpty()) {
      currentList.get(MAPPING_TYPE_RELATION)
                 .get(currentList.get(MAPPING_TYPE_RELATION).size() - 1)
                 .registerUpdateableMappingsAtParent(
                   object.getUpdateableMappingsAsParent());
    }
    updateMappingsWithLocalReferences(object, currentList, updateList);
  }

  void computeVariablesFromObject(MtasParserObject object,
                                  Map<String, Map<String, String>> variables) {
    MtasParserType<MtasParserVariable> parserType = object.getType();
    String id = object.getId();
    if (id != null) {
      for (MtasParserVariable variable : parserType.getItems()) {
        if (!variables.containsKey(variable.variable)) {
          variables.put(variable.variable, new HashMap<String, String>());
        }
        StringBuilder builder = new StringBuilder();
        for (MtasParserVariableValue variableValue : variable.values) {
          if (variableValue.type.equals("attribute")) {
            String subValue = object.getAttribute(variableValue.name);
            if (subValue != null) {
              builder.append(subValue);
            }
          }
        }
        variables.get(variable.variable).put(id, builder.toString());
      }
    }
  }

  private boolean checkForVariables(List<Map<String, String>> values) {
    if (values == null || values.isEmpty()) {
      return false;
    }
    for (Map<String, String> list : values) {
      if (MtasParserMapping.PARSER_TYPE_VARIABLE.equals(list.get("type"))) {
        return true;
      }
    }
    return false;
  }

  private void updateMappingsWithLocalReferences(MtasParserObject currentObject,
      Map<String, List<MtasParserObject>> currentList,
      Map<String, Map<Integer, Set<String>>> updateList) {
    if (currentObject.getType().type.equals(MAPPING_TYPE_GROUP)) {
      for (Integer tokenId : updateList
          .get(UPDATE_TYPE_LOCAL_REF_POSITION_START).keySet()) {
        if (updateList.get(UPDATE_TYPE_LOCAL_REF_POSITION_END)
            .containsKey(tokenId)
            && updateList.get(UPDATE_TYPE_LOCAL_REF_OFFSET_START)
                .containsKey(tokenId)
            && updateList.get(UPDATE_TYPE_LOCAL_REF_OFFSET_END)
                .containsKey(tokenId)) {
          Iterator<String> startPositionIt = updateList
              .get(UPDATE_TYPE_LOCAL_REF_POSITION_START).get(tokenId)
              .iterator();
          Iterator<String> endPositionIt = updateList
              .get(UPDATE_TYPE_LOCAL_REF_POSITION_END).get(tokenId).iterator();
          Iterator<String> startOffsetIt = updateList
              .get(UPDATE_TYPE_LOCAL_REF_OFFSET_START).get(tokenId).iterator();
          Iterator<String> endOffsetIt = updateList
              .get(UPDATE_TYPE_LOCAL_REF_OFFSET_END).get(tokenId).iterator();
          Integer startPosition = null;
          Integer endPosition = null;
          Integer startOffset = null;
          Integer endOffset = null;
          Integer newValue = null;
          while (startPositionIt.hasNext()) {
            String localKey = startPositionIt.next();
            if (currentObject.referredStartPosition.containsKey(localKey)) {
              newValue = currentObject.referredStartPosition.get(localKey);
              startPosition = (startPosition == null) ? newValue
                  : Math.min(startPosition, newValue);
            }
          }
          while (endPositionIt.hasNext()) {
            String localKey = endPositionIt.next();
            if (currentObject.referredEndPosition.containsKey(localKey)) {
              newValue = currentObject.referredEndPosition.get(localKey);
              endPosition = (endPosition == null) ? newValue
                  : Math.max(endPosition, newValue);
            }
          }
          while (startOffsetIt.hasNext()) {
            String localKey = startOffsetIt.next();
            if (currentObject.referredStartOffset.containsKey(localKey)) {
              newValue = currentObject.referredStartOffset.get(localKey);
              startOffset = (startOffset == null) ? newValue
                  : Math.min(startOffset, newValue);
            }
          }
          while (endOffsetIt.hasNext()) {
            String localKey = endOffsetIt.next();
            if (currentObject.referredEndOffset.containsKey(localKey)) {
              newValue = currentObject.referredEndOffset.get(localKey);
              endOffset = (endOffset == null) ? newValue
                  : Math.max(endOffset, newValue);
            }
          }
          if (startPosition != null && endPosition != null
              && startOffset != null && endOffset != null) {
            MtasToken token = tokenCollection.get(tokenId);
            token.addPositionRange(startPosition, endPosition);
            token.setOffset(startOffset, endOffset);
          }
        }
      }

    }
    if (!currentList.get(MAPPING_TYPE_GROUP).isEmpty()) {
      MtasParserObject parentGroup = currentList.get(MAPPING_TYPE_GROUP)
          .get(currentList.get(MAPPING_TYPE_GROUP).size() - 1);
      parentGroup.referredStartPosition
          .putAll(currentObject.referredStartPosition);
      parentGroup.referredEndPosition.putAll(currentObject.referredEndPosition);
      parentGroup.referredStartOffset.putAll(currentObject.referredStartOffset);
      parentGroup.referredEndOffset.putAll(currentObject.referredEndOffset);
    }
    currentObject.referredStartPosition.clear();
    currentObject.referredEndPosition.clear();
    currentObject.referredStartOffset.clear();
    currentObject.referredEndOffset.clear();
  }

  private String computeTypeFromMappingSource(String source)
      throws MtasParserException {
    switch (source) {
      case MtasParserMapping.SOURCE_OWN:
        return null;
      case MtasParserMapping.SOURCE_ANCESTOR_GROUP:
        return MAPPING_TYPE_GROUP;
      case MtasParserMapping.SOURCE_ANCESTOR_GROUP_ANNOTATION:
        return MAPPING_TYPE_GROUP_ANNOTATION;
      case MtasParserMapping.SOURCE_ANCESTOR_WORD:
        return MAPPING_TYPE_WORD;
      case MtasParserMapping.SOURCE_ANCESTOR_WORD_ANNOTATION:
        return MAPPING_TYPE_WORD_ANNOTATION;
      case MtasParserMapping.SOURCE_ANCESTOR_RELATION:
        return MAPPING_TYPE_RELATION;
      case MtasParserMapping.SOURCE_ANCESTOR_RELATION_ANNOTATION:
        return MAPPING_TYPE_RELATION_ANNOTATION;
      default:
        throw new MtasParserException("unknown source " + source);
    }
  }

  private MtasParserObject[] computeObjectFromMappingValue(MtasParserObject object, Map<String, String> mappingValue,
                                                           Map<String, List<MtasParserObject>> currentList)
    throws MtasParserException {
    // try to get relevant object
    if (mappingValue.get(MAPPING_VALUE_SOURCE).equals(MtasParserMapping.SOURCE_OWN)) {
      return new MtasParserObject[]{object};
    }

    Integer ancestorNumber = mappingValue.get(MAPPING_VALUE_ANCESTOR) != null
      ? Integer.parseInt(mappingValue.get(MAPPING_VALUE_ANCESTOR)) : null;
    String ancestorType = computeTypeFromMappingSource(mappingValue.get(MAPPING_VALUE_SOURCE));

    // get ancestor object
    MtasParserObject[] checkObjects = null;
    MtasParserObject checkObject;
    if (ancestorType != null) {
      int s = currentList.get(ancestorType).size();
      // check existence ancestor for conditions
      if (ancestorNumber != null) {
        if (s > 0 && ancestorNumber < s
          && (checkObject = currentList.get(ancestorType).get((s - ancestorNumber - 1))) != null) {
          checkObjects = new MtasParserObject[]{checkObject};
        }
      } else {
        checkObjects = new MtasParserObject[s];
        for (int i = s - 1; i >= 0; i--) {
          checkObjects[s - i - 1] = currentList.get(ancestorType).get(i);
        }
      }
    }
    return checkObjects;
  }

  private String[] computeValueFromMappingValues(MtasParserObject object,
      List<Map<String, String>> mappingValues,
      Map<String, List<MtasParserObject>> currentList,
      boolean containsVariables)
      throws MtasParserException, MtasConfigException {
    String[] value = { "" };
    for (Map<String, String> mappingValue : mappingValues) {
      // directly
      if (mappingValue.get(MAPPING_VALUE_SOURCE)
          .equals(MtasParserMapping.SOURCE_STRING)) {
        if (mappingValue.get("type")
            .equals(MtasParserMapping.PARSER_TYPE_STRING)) {
          String subvalue = computeFilteredPrefixedValue(
              mappingValue.get(MAPPING_VALUE_TYPE),
              mappingValue.get(MAPPING_VALUE_TEXT), null, null);
          if (subvalue != null) {
            for (int i = 0; i < value.length; i++) {
              value[i] = addAndEncodeValue(value[i], subvalue,
                  containsVariables);
            }
          }
        }
        // from objects
      } else {
        MtasParserObject[] checkObjects = computeObjectFromMappingValue(object,
            mappingValue, currentList);
        // create value
        if (checkObjects != null && checkObjects.length > 0) {
          MtasParserType checkType = checkObjects[0].getType();
          // add name to value
          if (mappingValue.get(MAPPING_VALUE_TYPE)
              .equals(MtasParserMapping.PARSER_TYPE_NAME)) {
            String subvalue = computeFilteredPrefixedValue(
                mappingValue.get(MAPPING_VALUE_TYPE), checkType.getName(),
                mappingValue.get(MAPPING_VALUE_FILTER),
                mappingValue.get(MAPPING_VALUE_PREFIX) == null
                    || mappingValue.get(MAPPING_VALUE_PREFIX).isEmpty() ? null
                        : mappingValue.get(MAPPING_VALUE_PREFIX));
            if (subvalue != null) {
              for (int i = 0; i < value.length; i++) {
                value[i] = addAndEncodeValue(value[i], subvalue,
                    containsVariables);
              }
            }
            // add attribute to value
          } else if (mappingValue.get(MAPPING_VALUE_TYPE)
              .equals(MtasParserMapping.PARSER_TYPE_ATTRIBUTE)) {
            String tmpValue = null;    
            if (mappingValue.get(MAPPING_VALUE_NAME).equals("#")) {
              tmpValue = checkObjects[0].getId();
            } else {
              String namespace = mappingValue.get(MAPPING_VALUE_NAMESPACE); 
              if(namespace==null) {
                tmpValue = checkObjects[0]
                  .getAttribute(mappingValue.get(MAPPING_VALUE_NAME));
              } else {
                tmpValue = checkObjects[0]
                    .getOtherAttribute(namespace, mappingValue.get(MAPPING_VALUE_NAME));
              }
            }
            String subvalue = computeFilteredPrefixedValue(
                mappingValue.get(MAPPING_VALUE_TYPE), tmpValue,
                mappingValue.get(MAPPING_VALUE_FILTER),
                mappingValue.get(MAPPING_VALUE_PREFIX) == null
                    || mappingValue.get(MAPPING_VALUE_PREFIX).isEmpty() ? null
                        : mappingValue.get(MAPPING_VALUE_PREFIX));
            if (subvalue != null) {
              for (int i = 0; i < value.length; i++) {
                value[i] = addAndEncodeValue(value[i], subvalue,
                    containsVariables);
              }
            }
            // value from text
          } else if (mappingValue.get("type")
              .equals(MtasParserMapping.PARSER_TYPE_TEXT)) {
            String subvalue = computeFilteredPrefixedValue(
                mappingValue.get(MAPPING_VALUE_TYPE), checkObjects[0].getText(),
                mappingValue.get(MAPPING_VALUE_FILTER),
                mappingValue.get(MAPPING_VALUE_PREFIX) == null
                    || mappingValue.get(MAPPING_VALUE_PREFIX).isEmpty() ? null
                        : mappingValue.get(MAPPING_VALUE_PREFIX));
            if (subvalue != null) {
              for (int i = 0; i < value.length; i++) {
                value[i] = addAndEncodeValue(value[i], subvalue,
                    containsVariables);
              }
            }
          } else if (mappingValue.get("type")
              .equals(MtasParserMapping.PARSER_TYPE_TEXT_SPLIT)) {
            String[] textValues = checkObjects[0].getText()
                .split(Pattern.quote(mappingValue.get(MAPPING_VALUE_SPLIT)));
            textValues = computeFilteredSplitValues(textValues,
                mappingValue.get(MAPPING_VALUE_FILTER));
            if (textValues != null && textValues.length > 0) {
              String[] nextValue = new String[value.length * textValues.length];
              boolean nullValue = false;
              int number = 0;
              for (String textValue : textValues) {
                String subvalue = computeFilteredPrefixedValue(
                  mappingValue.get(MAPPING_VALUE_TYPE), textValue,
                  mappingValue.get(MAPPING_VALUE_FILTER),
                  mappingValue.get(MAPPING_VALUE_PREFIX) == null
                    || mappingValue.get(MAPPING_VALUE_PREFIX).isEmpty()
                    ? null : mappingValue.get(MAPPING_VALUE_PREFIX));
                if (subvalue != null) {
                  for (String v : value) {
                    nextValue[number] = addAndEncodeValue(v, subvalue, containsVariables);
                    number++;
                  }
                } else if (!nullValue) {
                  for (String v : value) {
                    nextValue[number] = v;
                    number++;
                  }
                  nullValue = true;
                }
              }
              value = new String[number];
              System.arraycopy(nextValue, 0, value, 0, number);
            }
          } else if (mappingValue.get("type").equals(MtasParserMapping.PARSER_TYPE_VARIABLE)) {
            if (!containsVariables) {
              throw new MtasParserException("unexpected variable");
            }
            String variableName = mappingValue.get(MAPPING_VALUE_NAME);
            String variableValue = mappingValue.get(MAPPING_VALUE_VALUE);
            String prefix = mappingValue.get(MAPPING_VALUE_PREFIX);
            if (variableName != null && variableValue != null
              && mappingValue.get(MAPPING_VALUE_SOURCE)
                             .equals(MtasParserMapping.SOURCE_OWN)) {
              String subvalue = object.getAttribute(variableValue);
              if (subvalue != null && subvalue.startsWith("#")) {
                subvalue = subvalue.substring(1);
              }
              if (subvalue != null) {
                for (int i = 0; i < value.length; i++) {
                  if (prefix != null && !prefix.isEmpty()) {
                    value[i] = addAndEncodeValue(value[i], prefix, true);
                  }
                  value[i] = addAndEncodeVariable(value[i], variableName, subvalue, true);
                }
              }
            }
          } else {
            throw new MtasParserException(
                "unknown type " + mappingValue.get("type"));
          }
        }
      }
    }
    if (value.length == 1 && value[0].isEmpty()) {
      return new String[] {};
    } else {
      return value;
    }
  }

  private String addAndEncodeVariable(String originalValue, String newVariable,
      String newVariableName, boolean encode) {
    return addAndEncode(originalValue, newVariable, newVariableName, encode);
  }

  private String addAndEncodeValue(String originalValue, String newValue,
      boolean encode) {
    return addAndEncode(originalValue, null, newValue, encode);
  }

  private String addAndEncode(String originalValue, String newType,
                              String newValue, boolean encode) {
    if (newValue == null) {
      return originalValue;
    }
    String finalNewValue;
    if (encode) {
      if (newType == null) {
        finalNewValue = new String(
          enc.encode(newValue.getBytes(StandardCharsets.UTF_8)),
          StandardCharsets.UTF_8);
      } else {
        finalNewValue = new String(
          enc.encode(newType.getBytes(StandardCharsets.UTF_8)),
          StandardCharsets.UTF_8)
          + ":"
          + new String(
          enc.encode(newValue.getBytes(StandardCharsets.UTF_8)),
          StandardCharsets.UTF_8);
      }
    } else {
      finalNewValue = newValue;
    }
    if (originalValue == null || originalValue.isEmpty()) {
      return finalNewValue;
    } else {
      return originalValue + (encode ? " " : "") + finalNewValue;
    }
  }

  String decodeAndUpdateWithVariables(String encodedPrefix, String encodedPostfix,
                                      Map<String, Map<String, String>> variables) {
    String[] prefixSplit;
    String[] postfixSplit;
    if (encodedPrefix != null && !encodedPrefix.isEmpty()) {
      prefixSplit = encodedPrefix.split(" ");
    } else {
      prefixSplit = new String[0];
    }
    if (encodedPostfix != null && !encodedPostfix.isEmpty()) {
      postfixSplit = encodedPostfix.split(" ");
    } else {
      postfixSplit = new String[0];
    }
    try {
      String prefix = decodeAndUpdateWithVariables(prefixSplit, variables);
      String postfix = decodeAndUpdateWithVariables(postfixSplit, variables);
      return prefix + MtasToken.DELIMITER + postfix;
    } catch (MtasParserException e) {
      log.debug(e);
      return null;
    }
  }

  private String decodeAndUpdateWithVariables(String[] splitList,
      Map<String, Map<String, String>> variables) throws MtasParserException {
    StringBuilder builder = new StringBuilder();
    for (String split : splitList) {
      if (split.contains(":")) {
        String[] subSplit = split.split(":");
        if (subSplit.length == 2) {
          String decodedVariableName = new String(dec.decode(subSplit[0]),
              StandardCharsets.UTF_8);
          String decodedVariableValue = new String(dec.decode(subSplit[1]),
              StandardCharsets.UTF_8);
          if (variables.containsKey(decodedVariableName)) {
            if (variables.get(decodedVariableName)
                .containsKey(decodedVariableValue)) {
              String valueFromVariable = variables.get(decodedVariableName)
                  .get(decodedVariableValue);
              builder.append(valueFromVariable);
            } else {
              throw new MtasParserException("id " + decodedVariableValue
                  + " not found in " + decodedVariableName);
            }
          } else {
            throw new MtasParserException(
                "variable " + decodedVariableName + " unknown");
          }
        }
      } else {
        try {
          builder.append(new String(dec.decode(split), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
          log.info(e);
        }
      }
    }
    return builder.toString();
  }

  private BytesRef computePayloadFromMappingPayload(MtasParserObject object,
      List<Map<String, String>> mappingPayloads,
      Map<String, List<MtasParserObject>> currentList)
      throws MtasParserException {
    BytesRef payload = null;
    for (Map<String, String> mappingPayload : mappingPayloads) {
      if (mappingPayload.get(MAPPING_VALUE_SOURCE)
          .equals(MtasParserMapping.SOURCE_STRING)) {
        if (mappingPayload.get(MAPPING_VALUE_TYPE)
            .equals(MtasParserMapping.PARSER_TYPE_STRING)
            && mappingPayload.get(MAPPING_VALUE_TEXT) != null) {
          BytesRef subpayload = computeMaximumFilteredPayload(
              mappingPayload.get(MAPPING_VALUE_TEXT), payload, null);
          payload = (subpayload != null) ? subpayload : payload;
        }
        // from objects
      } else {
        MtasParserObject[] checkObjects = computeObjectFromMappingValue(object,
            mappingPayload, currentList);
        // do checks and updates
        if (checkObjects != null) {
          // payload from attribute
          if (mappingPayload.get("type")
              .equals(MtasParserMapping.PARSER_TYPE_ATTRIBUTE)) {
            BytesRef subpayload = computeMaximumFilteredPayload(
                checkObjects[0].getAttribute(mappingPayload.get("name")),
                payload, mappingPayload.get(MAPPING_VALUE_FILTER));
            payload = (subpayload != null) ? subpayload : payload;
            // payload from text
          } else if (mappingPayload.get("type")
              .equals(MtasParserMapping.PARSER_TYPE_TEXT)) {
            BytesRef subpayload = computeMaximumFilteredPayload(
                object.getText(), payload,
                mappingPayload.get(MAPPING_VALUE_FILTER));
            payload = (subpayload != null) ? subpayload : payload;
          }
        }
      }
    }
    return payload;
  }

  Boolean prevalidateObject(MtasParserObject object,
      Map<String, List<MtasParserObject>> currentList) {
    MtasParserType objectType = object.getType();
    List<MtasParserMapping<?>> mappings = objectType.getItems();
    if (mappings.isEmpty()) {
      return true;
    }
    for (MtasParserMapping<?> mapping : mappings) {
      try {
        precheckMappingConditions(object, mapping.getConditions(), currentList);
        return true;
      } catch (MtasParserException e) {
        log.debug(e);
      }
    }
    return false;
  }

  private void precheckMappingConditions(MtasParserObject object,
                                         List<Map<String, String>> mappingConditions,
                                         Map<String, List<MtasParserObject>> currentList)
      throws MtasParserException {
    for (Map<String, String> mappingCondition : mappingConditions) {
      // condition existence ancestor
      if (mappingCondition.get("type")
          .equals(MtasParserMapping.PARSER_TYPE_EXISTENCE)) {
        int number = 0;
        try {
          number = Integer.parseInt(mappingCondition.get("number"));
        } catch (Exception e) {
          log.debug(e);
        }
        String type = computeTypeFromMappingSource(
            mappingCondition.get(MAPPING_VALUE_SOURCE));
        if (number != currentList.get(type).size()) {
          throw new MtasParserException(
              "condition mapping is " + number + " ancestors of " + type
                  + " (but " + currentList.get(type).size() + " found)");
        }
        // condition unknown ancestors
      } else if (mappingCondition.get("type")
          .equals(MtasParserMapping.PARSER_TYPE_UNKNOWN_ANCESTOR)) {
        int number = 0;
        try {
          number = Integer.parseInt(mappingCondition.get("number"));
        } catch (Exception e) {
          log.debug(e);
        }
        if (number != object.getUnknownAncestorNumber()) {
          throw new MtasParserException(
              "condition mapping is " + number + " unknown ancestors (but "
                  + object.getUnknownAncestorNumber() + " found)");
        }
      } else {
        MtasParserObject[] checkObjects = computeObjectFromMappingValue(object,
            mappingCondition, currentList);
        Boolean notCondition = (mappingCondition.get("not") != null);

        // do checks
        if (checkObjects != null) {
          for (MtasParserObject checkObject : checkObjects) {
            MtasParserType checkType = checkObject.getType();
            // condition on name
            if (mappingCondition.get("type")
                                .equals(MtasParserMapping.PARSER_TYPE_NAME)) {
              if (notCondition && mappingCondition.get(MAPPING_VALUE_CONDITION)
                                                  .equals(checkType.getName())) {
                throw new MtasParserException("condition NOT "
                  + mappingCondition.get(MAPPING_VALUE_CONDITION)
                  + " on name not matched (is " + checkType.getName() + ")");
              } else if (!notCondition && mappingCondition
                .get(MAPPING_VALUE_CONDITION).equals(checkType.getName())) {
                break;
              } else if (!notCondition && !mappingCondition
                .get(MAPPING_VALUE_CONDITION).equals(checkType.getName())) {
                throw new MtasParserException("condition "
                  + mappingCondition.get(MAPPING_VALUE_CONDITION)
                  + " on name not matched (is " + checkType.getName() + ")");
              }
              // condition on attribute
            } else if (mappingCondition.get("type")
                                       .equals(MtasParserMapping.PARSER_TYPE_ATTRIBUTE)) {
              String attributeCondition = mappingCondition
                .get(MAPPING_VALUE_CONDITION);
              String namespace = mappingCondition.get(MAPPING_VALUE_NAMESPACE);
              String attributeValue;
              if (namespace == null) {
                attributeValue = checkObject.getAttribute(mappingCondition.get(MAPPING_VALUE_NAME));
              } else {
                attributeValue = checkObject.getOtherAttribute(namespace, mappingCondition.get(MAPPING_VALUE_NAME));
              }
              if ((attributeCondition == null) && (attributeValue == null)) {
                if (!notCondition) {
                  throw new MtasParserException("attribute "
                    + mappingCondition.get("name") + " not available");
                }
              } else if ((attributeCondition != null)
                && (attributeValue == null)) {
                if (!notCondition) {
                  throw new MtasParserException(
                    "condition " + attributeCondition + " on attribute "
                      + mappingCondition.get("name")
                      + " not matched (is null)");
                }
              } else if (attributeCondition != null) {
                if (!notCondition
                  && !attributeCondition.equals(attributeValue)) {
                  throw new MtasParserException(
                    "condition " + attributeCondition + " on attribute "
                      + mappingCondition.get("name") + " not matched (is "
                      + attributeValue + ")");
                } else if (!notCondition
                  && attributeCondition.equals(attributeValue)) {
                  break;
                } else if (notCondition
                  && attributeCondition.equals(attributeValue)) {
                  throw new MtasParserException(
                    "condition NOT " + attributeCondition + " on attribute "
                      + mappingCondition.get("name") + " not matched (is "
                      + attributeValue + ")");
                }
              }
              // condition on text
            } else if (mappingCondition.get("type")
                                       .equals(MtasParserMapping.PARSER_TYPE_TEXT)
              && object.getType().precheckText()) {
              String textCondition = mappingCondition
                .get(MAPPING_VALUE_CONDITION);
              String textValue = object.getText();
              if ((textCondition == null)
                && ((textValue == null) || textValue.equals(""))) {
                if (!notCondition) {
                  throw new MtasParserException("no text available");
                }
              } else if ((textCondition != null) && (textValue == null)) {
                if (!notCondition) {
                  throw new MtasParserException("condition " + textCondition
                    + " on text not matched (is null)");
                }
              } else if (textCondition != null) {
                if (!notCondition && !textCondition.equals(textValue)) {
                  throw new MtasParserException("condition " + textCondition
                    + " on text not matched (is " + textValue + ")");
                } else if (notCondition && textCondition.equals(textValue)) {
                  throw new MtasParserException("condition NOT " + textCondition
                    + " on text not matched (is " + textValue + ")");
                }
              }
            }
          }
        } else if (!notCondition) {
          throw new MtasParserException(
              "no object found to match condition" + mappingCondition);
        }
      }
    }
  }

  private void postcheckMappingConditions(MtasParserObject object,
      List<Map<String, String>> mappingConditions,
      Map<String, List<MtasParserObject>> currentList)
      throws MtasParserException {
    precheckMappingConditions(object, mappingConditions, currentList);
    for (Map<String, String> mappingCondition : mappingConditions) {
      // condition on text
      if (mappingCondition.get("type")
          .equals(MtasParserMapping.PARSER_TYPE_TEXT)) {
        MtasParserObject[] checkObjects = computeObjectFromMappingValue(object,
            mappingCondition, currentList);
        if (checkObjects != null) {
          String textCondition = mappingCondition.get(MAPPING_VALUE_CONDITION);
          String textValue = object.getText();
          Boolean notCondition = false;
          if (mappingCondition.get("not") != null) {
            notCondition = true;
          }
          if ((textCondition == null)
              && ((textValue == null) || textValue.isEmpty())) {
            if (!notCondition) {
              throw new MtasParserException("no text available");
            }
          } else if ((textCondition != null) && (textValue == null)) {
            if (!notCondition) {
              throw new MtasParserException("condition " + textCondition
                  + " on text not matched (is null)");
            }
          } else if (textCondition != null) {
            if (!notCondition && !textCondition.equals(textValue)) {
              throw new MtasParserException("condition " + textCondition
                  + " on text not matched (is " + textValue + ")");
            } else if (notCondition && textCondition.equals(textValue)) {
              throw new MtasParserException("condition NOT " + textCondition
                  + " on text not matched (is " + textValue + ")");
            }
          }
        }
      }
    }
  }

  private String[] computeFilteredSplitValues(String[] values, String filter)
      throws MtasConfigException {
    if (filter != null) {
      String[] filters = filter.split(",");
      boolean[] valuesFilter = new boolean[values.length];
      boolean doSplitFilter = false;
      for (String item : filters) {
        if (item.trim().matches(
            "^" + Pattern.quote(MAPPING_FILTER_SPLIT) + "\\([0-9\\-]+\\)$")) {
          doSplitFilter = true;
          Pattern splitContent = Pattern
              .compile("^" + Pattern.quote(MAPPING_FILTER_SPLIT)
                  + "\\(([0-9]+)(-([0-9]+))?\\)$");
          Matcher splitContentMatcher = splitContent.matcher(item.trim());
          while (splitContentMatcher.find()) {
            if (splitContentMatcher.group(3) == null) {
              int i = Integer.parseInt(splitContentMatcher.group(1));
              if (i >= 0 && i < values.length) {
                valuesFilter[i] = true;
              }
            } else {
              int i1 = Integer.parseInt(splitContentMatcher.group(1));
              int i2 = Integer.parseInt(splitContentMatcher.group(3));
              for (int i = Math.max(0, i1); i < Math.min(values.length, i2); i++) {
                valuesFilter[i] = true;
              }
            }
          }
        }
      }
      if (doSplitFilter) {
        int number = 0;
        for (boolean b : valuesFilter) {
          if (b) {
            number++;
          }
        }
        if (number > 0) {
          String[] newValues = new String[number];
          number = 0;
          for (int i = 0; i < valuesFilter.length; i++) {
            if (valuesFilter[i]) {
              newValues[number] = values[i];
              number++;
            }
          }
          return newValues;
        } else {
          return new String[] {};
        }
      }
    }
    return values;
  }

  private String computeFilteredPrefixedValue(String type, String value,
      String filter, String prefix) throws MtasConfigException {
    String localValue = value;
    // do magic with filter
    if (filter != null) {
      String[] filters = filter.split(",");
      for (String item : filters) {
        if (item.trim().equals(MAPPING_FILTER_UPPERCASE)) {
          localValue = localValue == null ? null : localValue.toUpperCase();
        } else if (item.trim().equals(MAPPING_FILTER_LOWERCASE)) {
          localValue = localValue == null ? null : localValue.toLowerCase();
        } else if (item.trim().equals(MAPPING_FILTER_ASCII)) {
          if (localValue != null) {
            char[] old = localValue.toCharArray();
            char[] ascii = new char[4 * old.length];
            ASCIIFoldingFilter.foldToASCII(old, 0, ascii, 0,
                localValue.length());
            localValue = new String(ascii);
          }
        } else if (item.trim()
            .matches(Pattern.quote(MAPPING_FILTER_SPLIT) + "\\([0-9\\-]+\\)")) {
          if (!type.equals(MtasParserMapping.PARSER_TYPE_TEXT_SPLIT)) {
            throw new MtasConfigException(
                "split filter not allowed for " + type);
          }
        } else {
          throw new MtasConfigException(
              "unknown filter " + item + " for value " + localValue);
        }
      }
    }
    if (localValue != null && prefix != null) {
      localValue = prefix + localValue;
    }
    return localValue;
  }

  private BytesRef computeMaximumFilteredPayload(String value, BytesRef payload,
      String filter) {
    // do magic with filter
    if (value != null) {
      if (payload != null) {
        Float payloadFloat = PayloadHelper.decodeFloat(payload.bytes,
            payload.offset);
        Float valueFloat = Float.parseFloat(value);
        return new BytesRef(
            PayloadHelper.encodeFloat(Math.max(payloadFloat, valueFloat)));
      } else {
        return new BytesRef(PayloadHelper.encodeFloat(Float.parseFloat(value)));
      }
    } else {
      return payload;
    }
  }

  static class MtasParserType<T> {
    private String type;
    private String name;
    boolean precheckText;
    private String refAttributeName;
    protected ArrayList<T> items = new ArrayList<>();

    MtasParserType(String type, String name, boolean precheckText) {
      this.type = type;
      this.name = name;
      this.precheckText = precheckText;
    }

    MtasParserType(String type, String name, boolean precheckText,
        String refAttributeName) {
      this(type, name, precheckText);
      this.refAttributeName = refAttributeName;
    }

    String getRefAttributeName() {
      return refAttributeName;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    boolean precheckText() {
      return precheckText;
    }

    void addItem(T item) {
      items.add(item);
    }

    public List<T> getItems() {
      return items;
    }
  }

  private static class MtasParserVariableValue {
    public String type;
    public String name;

    MtasParserVariableValue(String type, String name) {
      this.type = type;
      this.name = name;
    }
  }

  private static class MtasParserMappingToken {
    public String type;
    public Boolean offset;
    public Boolean realoffset;
    public Boolean parent;
    List<Map<String, String>> preValues;
    List<Map<String, String>> postValues;
    public List<Map<String, String>> payload;

    public MtasParserMappingToken(String tokenType) {
      type = tokenType;
      offset = true;
      realoffset = true;
      parent = true;
      preValues = new ArrayList<>();
      postValues = new ArrayList<>();
      payload = new ArrayList<>();
    }

    public void setOffset(Boolean tokenOffset) {
      offset = tokenOffset;
    }

    void setRealOffset(Boolean tokenRealOffset) {
      realoffset = tokenRealOffset;
    }

    public void setParent(Boolean tokenParent) {
      parent = tokenParent;
    }
  }

  static class MtasParserVariable {
    public String name;
    public String variable;
    protected ArrayList<MtasParserVariableValue> values;

    MtasParserVariable(String name, String value) {
      this.name = name;
      this.variable = value;
      values = new ArrayList<>();
    }

    void processConfig(Configuration config)
        throws MtasConfigException {
      for (int k = 0; k < config.numChildren(); k++) {
        if (config.child(k).getName().equals(VARIABLE_SUBTYPE_VALUE)) {

          for (int m = 0; m < config.child(k).numChildren(); m++) {
            if (config.child(k).child(m).getName()
                      .equals(VARIABLE_SUBTYPE_VALUE_ITEM)) {
              String valueType = config.child(k).child(m).getAttr("type");
              String nameType = config.child(k).child(m).getAttr("name");
              if ((valueType != null) && valueType.equals("attribute")
                  && nameType != null) {
                MtasParserVariableValue variableValue = new MtasParserVariableValue(
                    valueType, nameType);
                values.add(variableValue);
              }
            }
          }
        } else {
          throw new MtasConfigException(
            "unknown variable subtype " + config.child(k).getName()
              + " in variable " + config.getAttr("name"));
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("variable " + variable + " from " + name);
      for (int i = 0; i < values.size(); i++) {
        builder.append("\n\tvalue " + i);
        builder.append(" - " + values.get(i).type);
      }
      return builder.toString();
    }
  }

  abstract class MtasParserMapping<T extends MtasParserMapping<T>> {
    protected abstract T self();

    static final String SOURCE_OWN = "own";
    static final String SOURCE_REFS = "refs";
    static final String SOURCE_ANCESTOR_GROUP = "ancestorGroup";
    static final String SOURCE_ANCESTOR_GROUP_ANNOTATION = "ancestorGroupAnnotation";
    static final String SOURCE_ANCESTOR_WORD = "ancestorWord";
    static final String SOURCE_ANCESTOR_WORD_ANNOTATION = "ancestorWordAnnotation";
    static final String SOURCE_ANCESTOR_RELATION = "ancestorRelation";
    static final String SOURCE_ANCESTOR_RELATION_ANNOTATION = "ancestorRelationAnnotation";
    static final String SOURCE_STRING = "string";
    static final String PARSER_TYPE_VARIABLE = "variable";
    static final String PARSER_TYPE_STRING = "string";
    static final String PARSER_TYPE_NAME = "name";
    static final String PARSER_TYPE_ATTRIBUTE = "attribute";
    static final String PARSER_TYPE_TEXT = "text";
    static final String PARSER_TYPE_TEXT_SPLIT = "textSplit";
    static final String PARSER_TYPE_EXISTENCE = "existence";
    static final String PARSER_TYPE_UNKNOWN_ANCESTOR = "unknownAncestor";
    String type;
    String offset;
    String realOffset;
    String position;
    String start;
    String end;
    List<MtasParserMappingToken> tokens;
    List<Map<String, String>> conditions;

    public MtasParserMapping() {
      type = null;
      offset = null;
      realOffset = null;
      position = null;
      tokens = new ArrayList<>();
      conditions = new ArrayList<>();
      start = null;
      end = null;
    }

    public void processConfig(Configuration config)
        throws MtasConfigException {
      setStartEnd(config.getAttr("start"), config.getAttr("end"));
      for (int k = 0; k < config.numChildren(); k++) {
        if (config.child(k).getName().equals(MAPPING_SUBTYPE_TOKEN)) {
          String tokenType = config.child(k).getAttr("type");
          if ((tokenType != null) && tokenType.equals("string")) {
            MtasParserMappingToken mappingToken = new MtasParserMappingToken(
                tokenType);
            tokens.add(mappingToken);
            // check attributes
            for (String tokenAttributeName : config.child(k).attrNames()) {
              String attributeValue = config.child(k).getAttr(tokenAttributeName);
              switch (tokenAttributeName) {
                case TOKEN_OFFSET:
                  if (!attributeValue.equals("true")
                    && !attributeValue.equals("1")) {
                    mappingToken.setOffset(false);
                  } else {
                    mappingToken.setOffset(true);
                  }
                  break;
                case TOKEN_REALOFFSET:
                  if (!attributeValue.equals("true")
                    && !attributeValue.equals("1")) {
                    mappingToken.setRealOffset(false);
                  } else {
                    mappingToken.setRealOffset(true);
                  }
                  break;
                case TOKEN_PARENT:
                  if (!attributeValue.equals("true")
                    && !attributeValue.equals("1")) {
                    mappingToken.setParent(false);
                  } else {
                    mappingToken.setParent(true);
                  }
                  break;
              }
            }
            for (int m = 0; m < config.child(k).numChildren(); m++) {
              if (config.child(k).child(m).getName()
                        .equals(MAPPING_SUBTYPE_TOKEN_PRE)
                || config.child(k).child(m).getName()
                         .equals(MAPPING_SUBTYPE_TOKEN_POST)) {
                Configuration items = config.child(k).child(m);
                for (int l = 0; l < items.numChildren(); l++) {
                  if (items.child(l).getName().equals("item")) {
                    String itemType = items.child(l).getAttr(MAPPING_VALUE_TYPE);
                    String nameAttribute = items.child(l).getAttr(MAPPING_VALUE_NAME);
                    String namespaceAttribute = items.child(l).getAttr(MAPPING_VALUE_NAMESPACE);
                    String prefixAttribute = items.child(l).getAttr(MAPPING_VALUE_PREFIX);
                    String filterAttribute = items.child(l).getAttr(MAPPING_VALUE_FILTER);
                    String distanceAttribute = items.child(l).getAttr(MAPPING_VALUE_DISTANCE);
                    String valueAttribute = items.child(l).getAttr(MAPPING_VALUE_VALUE);
                    switch (itemType) {
                      case ITEM_TYPE_STRING:
                        addString(mappingToken, items.getName(), valueAttribute);
                        break;
                      case ITEM_TYPE_NAME:
                        addName(mappingToken, items.getName(), prefixAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE:
                        addAttribute(mappingToken, items.getName(), nameAttribute, namespaceAttribute,
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_TEXT:
                        addText(mappingToken, items.getName(), prefixAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_TEXT_SPLIT:
                        addTextSplit(mappingToken, items.getName(), valueAttribute,
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_NAME_ANCESTOR:
                        addAncestorName(computeAncestorSourceType(type),
                          mappingToken, items.getName(),
                          computeDistance(distanceAttribute), prefixAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_NAME_ANCESTOR_GROUP:
                        addAncestorName(SOURCE_ANCESTOR_GROUP, mappingToken,
                          items.getName(), computeDistance(distanceAttribute),
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_NAME_ANCESTOR_GROUP_ANNOTATION:
                        addAncestorName(SOURCE_ANCESTOR_GROUP_ANNOTATION,
                          mappingToken, items.getName(),
                          computeDistance(distanceAttribute), prefixAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_NAME_ANCESTOR_WORD:
                        addAncestorName(SOURCE_ANCESTOR_WORD, mappingToken,
                          items.getName(), computeDistance(distanceAttribute),
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_NAME_ANCESTOR_WORD_ANNOTATION:
                        addAncestorName(SOURCE_ANCESTOR_WORD_ANNOTATION,
                          mappingToken, items.getName(),
                          computeDistance(distanceAttribute), prefixAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_NAME_ANCESTOR_RELATION:
                        addAncestorName(SOURCE_ANCESTOR_RELATION, mappingToken,
                          items.getName(), computeDistance(distanceAttribute),
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_NAME_ANCESTOR_RELATION_ANNOTATION:
                        addAncestorName(SOURCE_ANCESTOR_RELATION_ANNOTATION,
                          mappingToken, items.getName(),
                          computeDistance(distanceAttribute), prefixAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_GROUP:
                        addAncestorAttribute(SOURCE_ANCESTOR_GROUP, mappingToken,
                          items.getName(), computeDistance(distanceAttribute),
                          nameAttribute, prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_GROUP_ANNOTATION:
                        addAncestorAttribute(SOURCE_ANCESTOR_GROUP_ANNOTATION,
                          mappingToken, items.getName(),
                          computeDistance(distanceAttribute), nameAttribute,
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_WORD:
                        addAncestorAttribute(SOURCE_ANCESTOR_WORD, mappingToken,
                          items.getName(), computeDistance(distanceAttribute),
                          nameAttribute, prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_WORD_ANNOTATION:
                        addAncestorAttribute(SOURCE_ANCESTOR_WORD_ANNOTATION,
                          mappingToken, items.getName(),
                          computeDistance(distanceAttribute), nameAttribute,
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_RELATION:
                        addAncestorAttribute(SOURCE_ANCESTOR_RELATION,
                          mappingToken, items.getName(),
                          computeDistance(distanceAttribute), nameAttribute,
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_RELATION_ANNOTATION:
                        addAncestorAttribute(SOURCE_ANCESTOR_RELATION_ANNOTATION,
                          mappingToken, items.getName(),
                          computeDistance(distanceAttribute), nameAttribute,
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR:
                        addAncestorAttribute(computeAncestorSourceType(this.type),
                          mappingToken, items.getName(),
                          computeDistance(distanceAttribute), nameAttribute,
                          prefixAttribute, filterAttribute);
                        break;
                      case ITEM_TYPE_VARIABLE_FROM_ATTRIBUTE:
                        addVariableFromAttribute(mappingToken, items.getName(),
                          nameAttribute, prefixAttribute, valueAttribute);
                        break;
                      default:
                        throw new MtasConfigException(String.format(
                          "unknown itemType %s for %s in mapping %s", itemType,
                          items.getName(), config.getAttr("name")));
                    }
                  }
                }
              } else if (config.child(k).child(m).getName()
                               .equals(MAPPING_SUBTYPE_PAYLOAD)) {
                Configuration items = config.child(k).child(m);
                for (int l = 0; l < items.numChildren(); l++) {
                  if (items.child(l).getName().equals("item")) {
                    String itemType = items.child(l).getAttr("type");
                    String valueAttribute = items.child(l).getAttr(MAPPING_VALUE_VALUE);
                    String nameAttribute = items.child(l).getAttr(MAPPING_VALUE_NAME);
                    String filterAttribute = items.child(l).getAttr(MAPPING_VALUE_FILTER);
                    String distanceAttribute = items.child(l).getAttr(MAPPING_VALUE_DISTANCE);
                    switch (itemType) {
                      case ITEM_TYPE_STRING:
                        payloadString(mappingToken, valueAttribute);
                        break;
                      case ITEM_TYPE_TEXT:
                        payloadText(mappingToken, filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE:
                        payloadAttribute(mappingToken, nameAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR:
                        payloadAncestorAttribute(mappingToken,
                          computeAncestorSourceType(type),
                          computeDistance(distanceAttribute), nameAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_GROUP:
                        payloadAncestorAttribute(mappingToken,
                          SOURCE_ANCESTOR_GROUP,
                          computeDistance(distanceAttribute), nameAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_GROUP_ANNOTATION:
                        payloadAncestorAttribute(mappingToken,
                          SOURCE_ANCESTOR_GROUP_ANNOTATION,
                          computeDistance(distanceAttribute), nameAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_WORD:
                        payloadAncestorAttribute(mappingToken,
                          SOURCE_ANCESTOR_WORD,
                          computeDistance(distanceAttribute), nameAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_WORD_ANNOTATION:
                        payloadAncestorAttribute(mappingToken,
                          SOURCE_ANCESTOR_WORD_ANNOTATION,
                          computeDistance(distanceAttribute), nameAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_RELATION:
                        payloadAncestorAttribute(mappingToken,
                          SOURCE_ANCESTOR_RELATION,
                          computeDistance(distanceAttribute), nameAttribute,
                          filterAttribute);
                        break;
                      case ITEM_TYPE_ATTRIBUTE_ANCESTOR_RELATION_ANNOTATION:
                        payloadAncestorAttribute(mappingToken,
                          SOURCE_ANCESTOR_RELATION_ANNOTATION,
                          computeDistance(distanceAttribute), nameAttribute,
                          filterAttribute);
                        break;
                      default:
                        throw new MtasConfigException(String.format(
                          "unknown itemType %s for %s in mapping %s", itemType,
                          items.getName(), config.getAttr("name")));
                    }
                  }
                }
              }
            }
          }
        } else if (config.child(k).getName()
                         .equals(MAPPING_SUBTYPE_CONDITION)) {
          Configuration items = config.child(k);
          for (int l = 0; l < items.numChildren(); l++) {
            if (items.child(l).getName().equals("item")) {
              String itemType = items.child(l).getAttr("type");
              String nameAttribute = items.child(l).getAttr(MAPPING_VALUE_NAME);
              String namespaceAttribute = items.child(l).getAttr(MAPPING_VALUE_NAMESPACE);
              String conditionAttribute = items.child(l).getAttr(MAPPING_VALUE_CONDITION);
              String filterAttribute = items.child(l).getAttr(MAPPING_VALUE_FILTER);
              String numberAttribute = items.child(l).getAttr(MAPPING_VALUE_NUMBER);
              String distanceAttribute = items.child(l).getAttr(MAPPING_VALUE_DISTANCE);
              String notAttribute = items.child(l).getAttr("not");
              if ((notAttribute != null) && !notAttribute.equals("true")
                  && !notAttribute.equals("1")) {
                notAttribute = null;
              }
              switch (itemType) {
                case ITEM_TYPE_ATTRIBUTE:
                  conditionAttribute(nameAttribute, namespaceAttribute, conditionAttribute,
                    filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_NAME:
                  conditionName(conditionAttribute, notAttribute);
                  break;
                case ITEM_TYPE_TEXT:
                  conditionText(conditionAttribute, filterAttribute,
                    notAttribute);
                  break;
                case ITEM_TYPE_UNKNOWN_ANCESTOR:
                  conditionUnknownAncestor(computeNumber(numberAttribute));
                  break;
                case ITEM_TYPE_ANCESTOR:
                  conditionAncestor(computeAncestorSourceType(type),
                    computeNumber(numberAttribute));
                  break;
                case ITEM_TYPE_ANCESTOR_GROUP:
                  conditionAncestor(SOURCE_ANCESTOR_GROUP,
                    computeNumber(numberAttribute));
                  break;
                case ITEM_TYPE_ANCESTOR_GROUP_ANNOTATION:
                  conditionAncestor(SOURCE_ANCESTOR_GROUP_ANNOTATION,
                    computeNumber(numberAttribute));
                  break;
                case ITEM_TYPE_ANCESTOR_WORD:
                  conditionAncestor(SOURCE_ANCESTOR_WORD,
                    computeNumber(numberAttribute));
                  break;
                case ITEM_TYPE_ANCESTOR_WORD_ANNOTATION:
                  conditionAncestor(SOURCE_ANCESTOR_WORD_ANNOTATION,
                    computeNumber(numberAttribute));
                  break;
                case ITEM_TYPE_ANCESTOR_RELATION:
                  conditionAncestor(SOURCE_ANCESTOR_RELATION,
                    computeNumber(numberAttribute));
                  break;
                case ITEM_TYPE_ANCESTOR_RELATION_ANNOTATION:
                  conditionAncestor(SOURCE_ANCESTOR_RELATION_ANNOTATION,
                    computeNumber(numberAttribute));
                  break;
                case ITEM_TYPE_ATTRIBUTE_ANCESTOR:
                  conditionAncestorAttribute(computeAncestorSourceType(type),
                    computeDistance(distanceAttribute), nameAttribute,
                    conditionAttribute, filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_ATTRIBUTE_ANCESTOR_GROUP:
                  conditionAncestorAttribute(SOURCE_ANCESTOR_GROUP,
                    computeDistance(distanceAttribute), nameAttribute,
                    conditionAttribute, filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_ATTRIBUTE_ANCESTOR_GROUP_ANNOTATION:
                  conditionAncestorAttribute(SOURCE_ANCESTOR_GROUP_ANNOTATION,
                    computeDistance(distanceAttribute), nameAttribute,
                    conditionAttribute, filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_ATTRIBUTE_ANCESTOR_WORD:
                  conditionAncestorAttribute(SOURCE_ANCESTOR_WORD,
                    computeDistance(distanceAttribute), nameAttribute,
                    conditionAttribute, filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_ATTRIBUTE_ANCESTOR_WORD_ANNOTATION:
                  conditionAncestorAttribute(SOURCE_ANCESTOR_WORD_ANNOTATION,
                    computeDistance(distanceAttribute), nameAttribute,
                    conditionAttribute, filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_ATTRIBUTE_ANCESTOR_RELATION:
                  conditionAncestorAttribute(SOURCE_ANCESTOR_RELATION,
                    computeDistance(distanceAttribute), nameAttribute,
                    conditionAttribute, filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_ATTRIBUTE_ANCESTOR_RELATION_ANNOTATION:
                  conditionAncestorAttribute(SOURCE_ANCESTOR_RELATION_ANNOTATION,
                    computeDistance(distanceAttribute), nameAttribute,
                    conditionAttribute, filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_NAME_ANCESTOR:
                  conditionAncestorName(computeAncestorSourceType(type),
                    computeDistance(distanceAttribute), conditionAttribute,
                    filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_NAME_ANCESTOR_GROUP:
                  conditionAncestorName(SOURCE_ANCESTOR_GROUP,
                    computeDistance(distanceAttribute), conditionAttribute,
                    filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_NAME_ANCESTOR_GROUP_ANNOTATION:
                  conditionAncestorName(SOURCE_ANCESTOR_GROUP_ANNOTATION,
                    computeDistance(distanceAttribute), conditionAttribute,
                    filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_NAME_ANCESTOR_WORD:
                  conditionAncestorName(SOURCE_ANCESTOR_WORD,
                    computeDistance(distanceAttribute), conditionAttribute,
                    filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_NAME_ANCESTOR_WORD_ANNOTATION:
                  conditionAncestorName(SOURCE_ANCESTOR_WORD_ANNOTATION,
                    computeDistance(distanceAttribute), conditionAttribute,
                    filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_NAME_ANCESTOR_RELATION:
                  conditionAncestorName(SOURCE_ANCESTOR_RELATION,
                    computeDistance(distanceAttribute), conditionAttribute,
                    filterAttribute, notAttribute);
                  break;
                case ITEM_TYPE_NAME_ANCESTOR_RELATION_ANNOTATION:
                  conditionAncestorName(SOURCE_ANCESTOR_RELATION_ANNOTATION,
                    computeDistance(distanceAttribute), conditionAttribute,
                    filterAttribute, notAttribute);
                  break;
                default:
                  throw new MtasConfigException(
                    String.format("unknown itemType %s for %s in mapping %s",
                      itemType, config.child(k).getName(),
                      config.getAttr("name")));
              }
            }
          }
        } else {
          throw new MtasConfigException(
              String.format("unknown mapping subType %s in mapping %s",
                config.child(k).getName(), config.getAttr("name")));
        }
      }
    }

    protected void setStartEnd(String start, String end) {
      if (start != null && !start.isEmpty() && end != null && !end.isEmpty()) {
        this.start = start;
        this.end = end;
      }
    }

    private void conditionUnknownAncestor(String number) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put("type", PARSER_TYPE_UNKNOWN_ANCESTOR);
      mapConstructionItem.put("number", number);
      conditions.add(mapConstructionItem);
    }

    private void addString(MtasParserMappingToken mappingToken, String type,
        String text) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_STRING);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_STRING);
      mapConstructionItem.put(MAPPING_VALUE_TEXT, text);
      if (type.equals(MAPPING_SUBTYPE_TOKEN_PRE)) {
        mappingToken.preValues.add(mapConstructionItem);
      } else if (type.equals(MAPPING_SUBTYPE_TOKEN_POST)) {
        mappingToken.postValues.add(mapConstructionItem);
      }
    }

    private void payloadString(MtasParserMappingToken mappingToken,
        String text) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_STRING);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_STRING);
      mapConstructionItem.put(MAPPING_VALUE_TEXT, text);
      mappingToken.payload.add(mapConstructionItem);
    }

    private void addName(MtasParserMappingToken mappingToken, String type,
        String prefix, String filter) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_NAME);
      mapConstructionItem.put(MAPPING_VALUE_PREFIX, prefix);
      mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
      if (type.equals(MAPPING_SUBTYPE_TOKEN_PRE)) {
        mappingToken.preValues.add(mapConstructionItem);
      } else if (type.equals(MAPPING_SUBTYPE_TOKEN_POST)) {
        mappingToken.postValues.add(mapConstructionItem);
      }
    }

    private void conditionName(String condition, String not) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_NAME);
      mapConstructionItem.put(MAPPING_VALUE_CONDITION, condition);
      mapConstructionItem.put(MAPPING_VALUE_NOT, not);
      conditions.add(mapConstructionItem);
    }

    private void addText(MtasParserMappingToken mappingToken, String type,
        String prefix, String filter) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_TEXT);
      mapConstructionItem.put(MAPPING_VALUE_PREFIX, prefix);
      mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
      if (type.equals(MAPPING_SUBTYPE_TOKEN_PRE)) {
        mappingToken.preValues.add(mapConstructionItem);
      } else if (type.equals(MAPPING_SUBTYPE_TOKEN_POST)) {
        mappingToken.postValues.add(mapConstructionItem);
      }
    }

    private void addTextSplit(MtasParserMappingToken mappingToken, String type,
        String split, String prefix, String filter) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_TEXT_SPLIT);
      mapConstructionItem.put(MAPPING_VALUE_SPLIT, split);
      mapConstructionItem.put(MAPPING_VALUE_PREFIX, prefix);
      mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
      if (type.equals(MAPPING_SUBTYPE_TOKEN_PRE)) {
        mappingToken.preValues.add(mapConstructionItem);
      } else if (type.equals(MAPPING_SUBTYPE_TOKEN_POST)) {
        mappingToken.postValues.add(mapConstructionItem);
      }
    }

    private void conditionText(String condition, String filter, String not) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_TEXT);
      mapConstructionItem.put(MAPPING_VALUE_CONDITION, condition);
      mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
      mapConstructionItem.put(MAPPING_VALUE_NOT, not);
      conditions.add(mapConstructionItem);
    }

    private void payloadText(MtasParserMappingToken mappingToken,
        String filter) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_TEXT);
      mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
      mappingToken.payload.add(mapConstructionItem);
    }

    private void addAttribute(MtasParserMappingToken mappingToken, String type,
        String name, String namespace, String prefix, String filter) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_ATTRIBUTE);
      mapConstructionItem.put(MAPPING_VALUE_NAME, name);
      mapConstructionItem.put(MAPPING_VALUE_NAMESPACE, namespace);
      mapConstructionItem.put(MAPPING_VALUE_PREFIX, prefix);
      mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
      if (name != null) {
        if (type.equals(MAPPING_SUBTYPE_TOKEN_PRE)) {
          mappingToken.preValues.add(mapConstructionItem);
        } else if (type.equals(MAPPING_SUBTYPE_TOKEN_POST)) {
          mappingToken.postValues.add(mapConstructionItem);
        }
      }
    }

    private void addVariableFromAttribute(MtasParserMappingToken mappingToken,
        String type, String name, String prefix, String value) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_VARIABLE);
      mapConstructionItem.put(MAPPING_VALUE_NAME, name);
      mapConstructionItem.put(MAPPING_VALUE_PREFIX, prefix);
      mapConstructionItem.put(MAPPING_VALUE_VALUE, value);
      if (name != null && value != null) {
        if (type.equals(MAPPING_SUBTYPE_TOKEN_PRE)) {
          mappingToken.preValues.add(mapConstructionItem);
        } else if (type.equals(MAPPING_SUBTYPE_TOKEN_POST)) {
          mappingToken.postValues.add(mapConstructionItem);
        }
      }
    }

    private void conditionAttribute(String name, String namespace, String condition,
        String filter, String not) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_ATTRIBUTE);
      mapConstructionItem.put(MAPPING_VALUE_NAME, name);
      mapConstructionItem.put(MAPPING_VALUE_NAMESPACE, namespace);
      mapConstructionItem.put(MAPPING_VALUE_CONDITION, condition);
      mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
      mapConstructionItem.put(MAPPING_VALUE_NOT, not);
      if (name != null) {
        conditions.add(mapConstructionItem);
      }
    }

    private void payloadAttribute(MtasParserMappingToken mappingToken,
        String name, String filter) {
      HashMap<String, String> mapConstructionItem = new HashMap<>();
      mapConstructionItem.put(MAPPING_VALUE_SOURCE, SOURCE_OWN);
      mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_ATTRIBUTE);
      mapConstructionItem.put(MAPPING_VALUE_NAME, name);
      mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
      mappingToken.payload.add(mapConstructionItem);
    }

    void conditionAncestor(String ancestorType, String number) {
      if (ancestorType.equals(SOURCE_ANCESTOR_GROUP)
          || ancestorType.equals(SOURCE_ANCESTOR_GROUP_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION_ANNOTATION)) {
        HashMap<String, String> mapConstructionItem = new HashMap<>();
        mapConstructionItem.put(MAPPING_VALUE_SOURCE, ancestorType);
        mapConstructionItem.put(MAPPING_VALUE_NUMBER, number);
        mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_EXISTENCE);
        conditions.add(mapConstructionItem);
      }
    }

    private void addAncestorName(String ancestorType,
        MtasParserMappingToken mappingToken, String type, String distance,
        String prefix, String filter) {
      if (ancestorType.equals(SOURCE_ANCESTOR_GROUP)
          || ancestorType.equals(SOURCE_ANCESTOR_GROUP_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION_ANNOTATION)) {
        HashMap<String, String> mapConstructionItem = new HashMap<>();
        mapConstructionItem.put(MAPPING_VALUE_SOURCE, ancestorType);
        mapConstructionItem.put(MAPPING_VALUE_ANCESTOR, distance);
        mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_NAME);
        mapConstructionItem.put(MAPPING_VALUE_PREFIX, prefix);
        mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
        if (type.equals(MAPPING_SUBTYPE_TOKEN_PRE)) {
          mappingToken.preValues.add(mapConstructionItem);
        } else if (type.equals(MAPPING_SUBTYPE_TOKEN_POST)) {
          mappingToken.postValues.add(mapConstructionItem);
        }
      }
    }

    void conditionAncestorName(String ancestorType, String distance,
                               String condition, String filter, String not) {
      if (ancestorType.equals(SOURCE_ANCESTOR_GROUP)
          || ancestorType.equals(SOURCE_ANCESTOR_GROUP_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION_ANNOTATION)) {
        HashMap<String, String> mapConstructionItem = new HashMap<>();
        mapConstructionItem.put(MAPPING_VALUE_SOURCE, ancestorType);
        mapConstructionItem.put(MAPPING_VALUE_ANCESTOR, distance);
        mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_NAME);
        mapConstructionItem.put(MAPPING_VALUE_CONDITION, condition);
        mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
        mapConstructionItem.put(MAPPING_VALUE_NOT, not);
        conditions.add(mapConstructionItem);
      }
    }

    void addAncestorAttribute(String ancestorType,
                              MtasParserMappingToken mappingToken, String type, String distance,
                              String name, String prefix, String filter) {
      if (ancestorType.equals(SOURCE_ANCESTOR_GROUP)
          || ancestorType.equals(SOURCE_ANCESTOR_GROUP_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION_ANNOTATION)) {
        HashMap<String, String> mapConstructionItem = new HashMap<>();
        mapConstructionItem.put(MAPPING_VALUE_SOURCE, ancestorType);
        mapConstructionItem.put(MAPPING_VALUE_ANCESTOR, distance);
        mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_ATTRIBUTE);
        mapConstructionItem.put(MAPPING_VALUE_NAME, name);
        mapConstructionItem.put(MAPPING_VALUE_PREFIX, prefix);
        mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
        if (name != null) {
          if (type.equals(MAPPING_SUBTYPE_TOKEN_PRE)) {
            mappingToken.preValues.add(mapConstructionItem);
          } else if (type.equals(MAPPING_SUBTYPE_TOKEN_POST)) {
            mappingToken.postValues.add(mapConstructionItem);
          }
        }
      }
    }

    void conditionAncestorAttribute(String ancestorType, String distance,
                                    String name, String condition, String filter, String not) {
      if (ancestorType.equals(SOURCE_ANCESTOR_GROUP)
          || ancestorType.equals(SOURCE_ANCESTOR_GROUP_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION_ANNOTATION)) {
        HashMap<String, String> mapConstructionItem = new HashMap<>();
        mapConstructionItem.put(MAPPING_VALUE_SOURCE, ancestorType);
        mapConstructionItem.put(MAPPING_VALUE_ANCESTOR, distance);
        mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_ATTRIBUTE);
        mapConstructionItem.put(MAPPING_VALUE_NAME, name);
        mapConstructionItem.put(MAPPING_VALUE_CONDITION, condition);
        mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
        mapConstructionItem.put(MAPPING_VALUE_NOT, not);
        if (name != null) {
          conditions.add(mapConstructionItem);
        }
      }
    }

    private void payloadAncestorAttribute(MtasParserMappingToken mappingToken,
        String ancestorType, String distance, String name, String filter) {
      if (ancestorType.equals(SOURCE_ANCESTOR_GROUP)
          || ancestorType.equals(SOURCE_ANCESTOR_GROUP_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD)
          || ancestorType.equals(SOURCE_ANCESTOR_WORD_ANNOTATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION)
          || ancestorType.equals(SOURCE_ANCESTOR_RELATION_ANNOTATION)) {
        HashMap<String, String> mapConstructionItem = new HashMap<>();
        mapConstructionItem.put(MAPPING_VALUE_SOURCE, ancestorType);
        mapConstructionItem.put(MAPPING_VALUE_ANCESTOR, distance);
        mapConstructionItem.put(MAPPING_VALUE_TYPE, PARSER_TYPE_ATTRIBUTE);
        mapConstructionItem.put(MAPPING_VALUE_NAME, name);
        mapConstructionItem.put(MAPPING_VALUE_FILTER, filter);
        if (name != null) {
          mappingToken.payload.add(mapConstructionItem);
        }
      }
    }

    private String computeAncestorSourceType(String type)
        throws MtasConfigException {
      switch (type) {
        case MAPPING_TYPE_GROUP:
          return SOURCE_ANCESTOR_GROUP;
        case MAPPING_TYPE_GROUP_ANNOTATION:
          return SOURCE_ANCESTOR_GROUP_ANNOTATION;
        case MAPPING_TYPE_WORD:
          return SOURCE_ANCESTOR_WORD;
        case MAPPING_TYPE_WORD_ANNOTATION:
          return SOURCE_ANCESTOR_WORD_ANNOTATION;
        case MAPPING_TYPE_RELATION:
          return SOURCE_ANCESTOR_RELATION;
        case MAPPING_TYPE_RELATION_ANNOTATION:
          return SOURCE_ANCESTOR_RELATION_ANNOTATION;
        default:
          throw new MtasConfigException("unknown type " + type);
      }
    }

    private String computeDistance(String distance) {
      Integer i = 0;
      if (distance != null) {
        Integer d = Integer.parseInt(distance);
        if (d >= i) {
          return distance;
        } else {
          return i.toString();
        }
      }
      return null;
    }

    private String computeNumber(String number) {
      return computeDistance(number);
    }

    public List<MtasParserMappingToken> getTokens() {
      return tokens;
    }

    public List<Map<String, String>> getConditions() {
      return conditions;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("mapping - type:" + type + ", offset:" + offset
          + ", realOffset:" + realOffset + ", position:" + position);
      for (int i = 0; i < conditions.size(); i++) {
        builder.append("\n\tcondition " + i + ": ");
        for (Entry<String, String> entry : conditions.get(i).entrySet()) {
          builder.append(entry.getKey() + ":" + entry.getValue() + ",");
        }
      }
      for (int i = 0; i < tokens.size(); i++) {
        builder.append("\n\ttoken " + i);
        builder.append(" - " + tokens.get(i).type);
        builder.append(" [offset:" + tokens.get(i).offset);
        builder.append(",realoffset:" + tokens.get(i).realoffset);
        builder.append(",parent:" + tokens.get(i).parent + "]");
        for (int j = 0; j < tokens.get(i).preValues.size(); j++) {
          builder.append("\n\t- pre " + j + ": ");
          for (Entry<String, String> entry : tokens.get(i).preValues.get(j)
              .entrySet()) {
            builder.append(entry.getKey() + ":" + entry.getValue() + ",");
          }
        }
        for (int j = 0; j < tokens.get(i).postValues.size(); j++) {
          builder.append("\n\t- post " + j + ": ");
          for (Entry<String, String> entry : tokens.get(i).postValues.get(j)
              .entrySet()) {
            builder.append(entry.getKey() + ":" + entry.getValue() + ",");
          }
        }
        for (int j = 0; j < tokens.get(i).payload.size(); j++) {
          builder.append("\n\t- payload " + j + ": ");
          for (Entry<String, String> entry : tokens.get(i).payload.get(j)
              .entrySet()) {
            builder.append(entry.getKey() + ":" + entry.getValue() + ",");
          }
        }
      }
      return builder.toString();
    }

  }

  final class MtasParserObject {
    MtasParserType objectType;
    private Integer objectRealOffsetStart = null;
    private Integer objectRealOffsetEnd = null;
    private Integer objectOffsetStart = null;
    private Integer objectOffsetEnd = null;
    private String objectText = null;
    String objectId = null;
    private Integer objectUnknownAncestorNumber = null;
    HashMap<String, String> objectAttributes = null;
    HashMap<String, HashMap<String, String>> objectOtherAttributes = null;
    private SortedSet<Integer> objectPositions = new TreeSet<>();
    private Set<String> refIds = new HashSet<>();
    private Set<Integer> updateableMappingsAsParent = new HashSet<>();
    Set<Integer> updateableMappingsWithPosition = new HashSet<>();
    private Set<String> updateableIdsWithOffset = new HashSet<>();
    Set<Integer> updateableMappingsWithOffset = new HashSet<>();
    Map<String, Integer> referredStartPosition = new HashMap<>();
    Map<String, Integer> referredEndPosition = new HashMap<>();
    Map<String, Integer> referredStartOffset = new HashMap<>();
    Map<String, Integer> referredEndOffset = new HashMap<>();

    MtasParserObject(MtasParserType type) {
      objectType = type;
      objectAttributes = new HashMap<>();
      objectOtherAttributes = new HashMap<>();
    }

    void registerUpdateableMappingAtParent(Integer mappingId) {
      updateableMappingsAsParent.add(mappingId);
    }

    void registerUpdateableMappingsAtParent(Set<Integer> mappingIds) {
      updateableMappingsAsParent.addAll(mappingIds);
    }

    Set<Integer> getUpdateableMappingsAsParent() {
      return updateableMappingsAsParent;
    }

    void resetUpdateableMappingsAsParent() {
      updateableMappingsAsParent.clear();
    }

    void addUpdateableMappingWithPosition(Integer mappingId) {
      updateableMappingsWithPosition.add(mappingId);
    }

    void addUpdateableIdWithOffset(String id) {
      updateableIdsWithOffset.add(id);
    }

    void addUpdateableMappingWithOffset(Integer mappingId) {
      updateableMappingsWithOffset.add(mappingId);
    }

    void updateMappings(Map<String, Set<Integer>> idPositions,
                        Map<String, Integer[]> idOffsets) {
      for (Integer mappingId : updateableMappingsWithPosition) {
        tokenCollection.get(mappingId).addPositions(objectPositions);
      }
      for (Integer mappingId : updateableMappingsWithOffset) {
        tokenCollection.get(mappingId).addOffset(objectOffsetStart,
            objectOffsetEnd);
      }
      for (String id : updateableIdsWithOffset) {
        if (idOffsets.containsKey(id)) {
          Integer[] currentOffset = idOffsets.get(id);
          if (currentOffset == null || currentOffset.length == 0) {
            idOffsets.put(id,
                new Integer[] { objectOffsetStart, objectOffsetEnd });
          }
        }
      }
    }

    String getAttribute(String name) {
      if (name != null) {
        return objectAttributes.get(name);
      } else {
        return null;
      }
    }

    String getOtherAttribute(String other, String name) {
      if(other==null) {
        return getAttribute(name);
      } else {
        if(objectOtherAttributes.containsKey(other)) {
          if (name != null) {
            return objectOtherAttributes.get(other).get(name);
          } else {
            return null;
          }
        } else {
          return null;
        }        
      }  
    }

    public String getId() {
      return objectId;
    }

    MtasParserType getType() {
      return objectType;
    }

    public void setText(String text) {
      objectText = text;
    }

    void addText(String text) {
      if (objectText == null) {
        objectText = text;
      } else {
        objectText += text;
      }
    }

    public String getText() {
      return objectText;
    }

    void setUnknownAncestorNumber(Integer i) {
      objectUnknownAncestorNumber = i;
    }

    Integer getUnknownAncestorNumber() {
      return objectUnknownAncestorNumber;
    }

    void setRealOffsetStart(Integer start) {
      objectRealOffsetStart = start;
    }

    Integer getRealOffsetStart() {
      return objectRealOffsetStart;
    }

    void setRealOffsetEnd(Integer end) {
      objectRealOffsetEnd = end;
    }

    Integer getRealOffsetEnd() {
      return objectRealOffsetEnd;
    }

    void setOffsetStart(Integer start) {
      objectOffsetStart = start;
    }

    void addOffsetStart(Integer start) {
      if ((start != null)
          && ((objectOffsetStart == null) || (start < objectOffsetStart))) {
        objectOffsetStart = start;
      }
    }

    void addOffsetEnd(Integer end) {
      if ((end != null)
          && ((objectOffsetEnd == null) || (end > objectOffsetEnd))) {
        objectOffsetEnd = end;
      }
    }

    public Integer getOffsetStart() {
      return objectOffsetStart;
    }

    void setOffsetEnd(Integer end) {
      objectOffsetEnd = end;
    }

    public Integer getOffsetEnd() {
      return objectOffsetEnd;
    }

    public Integer[] getOffset() {
      if (objectOffsetStart != null) {
        return new Integer[] { objectOffsetStart, objectOffsetEnd };
      } else {
        return new Integer[0];
      }
    }

    void addPosition(Integer position) {
      objectPositions.add(position);
    }

    void addPositions(Set<Integer> positions) {
      objectPositions.addAll(positions);
    }

    public SortedSet<Integer> getPositions() {
      return objectPositions;
    }

    void addRefId(String id) {
      if (id != null) {
        refIds.add(id);
      }
    }

    Set<String> getRefIds() {
      return refIds;
    }

    void setReferredStartPosition(String id, Integer position) {
      referredStartPosition.put(id, position);
    }

    void setReferredEndPosition(String id, Integer position) {
      referredEndPosition.put(id, position);
    }

    void setReferredStartOffset(String id, Integer offset) {
      referredStartOffset.put(id, offset);
    }

    void setReferredEndOffset(String id, Integer offset) {
      referredEndOffset.put(id, offset);
    }
  }
}
