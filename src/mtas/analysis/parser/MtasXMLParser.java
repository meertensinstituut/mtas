package mtas.analysis.parser;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import mtas.analysis.token.MtasToken;
import mtas.analysis.token.MtasTokenCollection;
import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasParserException;
import mtas.analysis.util.MtasConfiguration;

/**
 * The Class MtasXMLParser.
 */
abstract class MtasXMLParser extends MtasBasicParser {

  /** The namespace uri. */
  protected String namespaceURI = null;

  /** The namespace ur i_id. */
  protected String namespaceURI_id = null;

  /** The root tag. */
  protected String rootTag = null;

  /** The content tag. */
  protected String contentTag = null;

  /** The allow non content. */
  protected boolean allowNonContent = false;

  /** The relation key map. */
  private HashMap<String, TreeSet<String>> relationKeyMap = new HashMap<String, TreeSet<String>>();

  /** The q names. */
  private HashMap<String, QName> qNames = new HashMap<String, QName>();

  /** The relation types. */
  private HashMap<QName, MtasParserType> relationTypes = new HashMap<QName, MtasParserType>();

  /** The relation annotation types. */
  private HashMap<QName, MtasParserType> relationAnnotationTypes = new HashMap<QName, MtasParserType>();

  /** The ref types. */
  private HashMap<QName, MtasParserType> refTypes = new HashMap<QName, MtasParserType>();

  /** The group types. */
  private HashMap<QName, MtasParserType> groupTypes = new HashMap<QName, MtasParserType>();

  /** The group annotation types. */
  private HashMap<QName, MtasParserType> groupAnnotationTypes = new HashMap<QName, MtasParserType>();

  /** The word types. */
  private HashMap<QName, MtasParserType> wordTypes = new HashMap<QName, MtasParserType>();

  /** The word annotation types. */
  private HashMap<QName, MtasParserType> wordAnnotationTypes = new HashMap<QName, MtasParserType>();

  /** The xml references. */
  private static String XML_REFERENCES = "references";

  /** The xml reference. */
  private static String XML_REFERENCE = "reference";

  /** The xml mappings. */
  private static String XML_MAPPINGS = "mappings";

  /** The xml mapping. */
  private static String XML_MAPPING = "mapping";

  /** The xml mapping type. */
  private static String XML_MAPPING_TYPE = "type";

  /** The xml mapping name. */
  private static String XML_MAPPING_NAME = "name";

  /**
   * Instantiates a new mtas xml parser.
   *
   * @param config the config
   */
  public MtasXMLParser(MtasConfiguration config) {
    super(config);
    try {
      initParser();
    } catch (MtasConfigException e) {
      e.printStackTrace();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.analysis.parser.MtasParser#printConfig()
   */
  @Override
  public String printConfig() {
    String text = "";
    text += "=== CONFIGURATION ===\n";
    text += "type: " + groupTypes.size() + " x group\n";
    text += printConfigTypes(groupTypes);
    text += "type: " + groupAnnotationTypes.size() + " x groupAnnotation";
    text += printConfigTypes(groupAnnotationTypes);
    text += "type: " + wordTypes.size() + " x word\n";
    text += printConfigTypes(wordTypes);
    text += "type: " + wordAnnotationTypes.size() + " x wordAnnotation";
    text += printConfigTypes(wordAnnotationTypes);
    text += "type: " + relationTypes.size() + " x relation\n";
    text += printConfigTypes(relationTypes);
    text += "type: " + relationAnnotationTypes.size()
        + " x relationAnnotation\n";
    text += printConfigTypes(relationAnnotationTypes);
    text += "type: " + refTypes.size() + " x references\n";
    text += printConfigTypes(refTypes);
    text += "=== CONFIGURATION ===\n";
    return text;
  }

  /**
   * Prints the config types.
   *
   * @param types the types
   * @return the string
   */
  private String printConfigTypes(HashMap<QName, MtasParserType> types) {
    String text = "";
    for (Entry<QName, MtasParserType> entry : types.entrySet()) {
      text += "- " + entry.getKey().getLocalPart() + ": "
          + entry.getValue().mappings.size() + " mapping(s)\n";
      for (int i = 0; i < entry.getValue().mappings.size(); i++) {
        text += "\t" + entry.getValue().mappings.get(i) + "\n";
      }
    }
    return text;
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.analysis.parser.MtasParser#initParser()
   */
  @Override
  protected void initParser() throws MtasConfigException {
    super.initParser();
    if (config != null) {
      // find namespaceURI
      for (int i = 0; i < config.children.size(); i++) {
        MtasConfiguration current = config.children.get(i);
        if (current.name.equals("namespaceURI")) {
          namespaceURI = current.attributes.get("value");
        }
      }
      // loop again
      for (int i = 0; i < config.children.size(); i++) {
        MtasConfiguration current = config.children.get(i);
        if (current.name.equals(XML_REFERENCES)) {
          for (int j = 0; j < current.children.size(); j++) {
            if (current.children.get(j).name.equals(XML_REFERENCE)) {
              MtasConfiguration reference = current.children.get(j);
              String name = reference.attributes.get("name");
              String ref = reference.attributes.get("ref");
              if ((name != null) && (ref != null)) {
                MtasParserType t = new MtasParserType(MAPPING_TYPE_REF, name,
                    false, ref);
                refTypes.put(getQName(t.getName()), t);
              }
            }
          }
        } else if (current.name.equals(XML_MAPPINGS)) {
          for (int j = 0; j < current.children.size(); j++) {
            if (current.children.get(j).name.equals(XML_MAPPING)) {
              MtasConfiguration mapping = current.children.get(j);
              String typeMapping = mapping.attributes.get(XML_MAPPING_TYPE);
              String nameMapping = mapping.attributes.get(XML_MAPPING_NAME);
              if ((typeMapping != null) && (nameMapping != null)) {
                if (typeMapping.equals(MAPPING_TYPE_RELATION)) {
                  MtasXMLParserMappingRelation m = new MtasXMLParserMappingRelation();
                  m.processConfig(mapping);
                  QName qn = getQName(nameMapping);
                  if (relationTypes.containsKey(qn)) {
                    relationTypes.get(qn).addMapping(m);
                  } else {
                    MtasParserType t = new MtasParserType(typeMapping,
                        nameMapping, false);
                    t.addMapping(m);
                    relationTypes.put(qn, t);
                  }
                } else if (typeMapping
                    .equals(MAPPING_TYPE_RELATION_ANNOTATION)) {
                  MtasXMLParserMappingRelationAnnotation m = new MtasXMLParserMappingRelationAnnotation();
                  m.processConfig(mapping);
                  QName qn = getQName(nameMapping);
                  if (relationAnnotationTypes.containsKey(qn)) {
                    relationAnnotationTypes.get(qn).addMapping(m);
                  } else {
                    MtasParserType t = new MtasParserType(typeMapping,
                        nameMapping, false);
                    t.addMapping(m);
                    relationAnnotationTypes.put(qn, t);
                  }
                } else if (typeMapping.equals(MAPPING_TYPE_WORD)) {
                  MtasXMLParserMappingWord m = new MtasXMLParserMappingWord();
                  m.processConfig(mapping);
                  QName qn = getQName(nameMapping);
                  if (wordTypes.containsKey(qn)) {
                    wordTypes.get(qn).addMapping(m);
                  } else {
                    MtasParserType t = new MtasParserType(typeMapping,
                        nameMapping, false);
                    t.addMapping(m);
                    wordTypes.put(qn, t);
                  }
                } else if (typeMapping.equals(MAPPING_TYPE_WORD_ANNOTATION)) {
                  MtasXMLParserMappingWordAnnotation m = new MtasXMLParserMappingWordAnnotation();
                  m.processConfig(mapping);
                  QName qn = getQName(nameMapping);
                  if (wordAnnotationTypes.containsKey(qn)) {
                    wordAnnotationTypes.get(qn).addMapping(m);
                  } else {
                    MtasParserType t = new MtasParserType(typeMapping,
                        nameMapping, false);
                    t.addMapping(m);
                    wordAnnotationTypes.put(qn, t);
                  }
                } else if (typeMapping.equals(MAPPING_TYPE_GROUP)) {
                  MtasXMLParserMappingGroup m = new MtasXMLParserMappingGroup();
                  m.processConfig(mapping);
                  QName qn = getQName(nameMapping);
                  if (groupTypes.containsKey(qn)) {
                    groupTypes.get(qn).addMapping(m);
                  } else {
                    MtasParserType t = new MtasParserType(typeMapping,
                        nameMapping, false);
                    t.addMapping(m);
                    groupTypes.put(qn, t);
                  }
                } else if (typeMapping.equals(MAPPING_TYPE_GROUP_ANNOTATION)) {
                  MtasXMLParserMappingGroupAnnotation m = new MtasXMLParserMappingGroupAnnotation();
                  m.processConfig(mapping);
                  QName qn = getQName(nameMapping);
                  if (groupAnnotationTypes.containsKey(qn)) {
                    groupAnnotationTypes.get(qn).addMapping(m);
                  } else {
                    MtasParserType t = new MtasParserType(typeMapping,
                        nameMapping, false);
                    t.addMapping(m);
                    groupAnnotationTypes.put(qn, t);
                  }
                } else {
                  throw new MtasConfigException(
                      "unknown mapping type " + typeMapping);
                }
              }
            }
          }
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.analysis.parser.MtasParser#createTokenCollection(java.io.Reader)
   */
  @Override
  public MtasTokenCollection createTokenCollection(Reader reader)
      throws MtasParserException, MtasConfigException {
    Boolean hasRoot = rootTag == null ? true : false;
    Boolean parsingContent = contentTag == null ? true : false;
    String textContent = null;
    Integer unknownAncestors = 0;
    Integer lastOffset = 0;

    AtomicInteger position = new AtomicInteger(0);
    HashMap<String, TreeSet<Integer>> idPositions = new HashMap<String, TreeSet<Integer>>();
    HashMap<String, Integer[]> idOffsets = new HashMap<String, Integer[]>();

    HashMap<String, HashMap<Integer, HashSet<String>>> updateList = new HashMap<String, HashMap<Integer, HashSet<String>>>();
    updateList.put(UPDATE_TYPE_OFFSET, new HashMap<Integer, HashSet<String>>());
    updateList.put(UPDATE_TYPE_POSITION,
        new HashMap<Integer, HashSet<String>>());

    HashMap<String, ArrayList<MtasParserObject>> currentList = new HashMap<String, ArrayList<MtasParserObject>>();
    currentList.put(MAPPING_TYPE_RELATION, new ArrayList<MtasParserObject>());
    currentList.put(MAPPING_TYPE_RELATION_ANNOTATION,
        new ArrayList<MtasParserObject>());
    currentList.put(MAPPING_TYPE_REF, new ArrayList<MtasParserObject>());
    currentList.put(MAPPING_TYPE_GROUP, new ArrayList<MtasParserObject>());
    currentList.put(MAPPING_TYPE_GROUP_ANNOTATION,
        new ArrayList<MtasParserObject>());
    currentList.put(MAPPING_TYPE_WORD, new ArrayList<MtasParserObject>());
    currentList.put(MAPPING_TYPE_WORD_ANNOTATION,
        new ArrayList<MtasParserObject>());

    tokenCollection = new MtasTokenCollection();
    MtasToken.resetId();
    XMLInputFactory factory = XMLInputFactory.newInstance();
    try {
      XMLStreamReader streamReader = factory.createXMLStreamReader(reader);
      QName qname;
      try {
        int event = streamReader.getEventType();
        MtasParserType currentType, tmpCurrentType;
        MtasParserObject currentObject = null;
        while (true) {
          switch (event) {
          case XMLStreamConstants.START_DOCUMENT:
            String encodingScheme = streamReader.getCharacterEncodingScheme();
            if (encodingScheme == null) {
              throw new MtasParserException("No encodingScheme found");
            } else if (!encodingScheme.toLowerCase().equals("utf-8")) {
              throw new MtasParserException(
                  "XML not UTF-8 encoded but '" + encodingScheme + "'");
            }
            break;
          case XMLStreamConstants.END_DOCUMENT:
            break;
          case XMLStreamConstants.SPACE:
            // set offset (end of start-element)
            lastOffset = streamReader.getLocation().getCharacterOffset();
            break;
          case XMLStreamConstants.START_ELEMENT:
            // get data
            qname = streamReader.getName();
            // check for rootTag
            if (!hasRoot) {
              if (qname.equals(getQName(rootTag))) {
                hasRoot = true;
              } else {
                throw new MtasParserException("No " + rootTag);
              }
              // parse content
            } else if (parsingContent) {
              // check for relation : not within word, not within
              // groupAnnotation
              if ((currentList.get(MAPPING_TYPE_WORD).size() == 0)
                  && (currentList.get(MAPPING_TYPE_GROUP_ANNOTATION)
                      .size() == 0)
                  && (tmpCurrentType = relationTypes.get(qname)) != null) {
                currentObject = new MtasParserObject(tmpCurrentType);
                collectAttributes(currentObject, streamReader);
                currentObject.setUnknownAncestorNumber(unknownAncestors);
                currentObject.setRealOffsetStart(lastOffset);
                if (!prevalidateObject(currentObject, currentList)) {
                  unknownAncestors++;
                } else {
                  currentType = tmpCurrentType;
                  currentList.get(MAPPING_TYPE_RELATION).add(currentObject);
                  unknownAncestors = 0;
                }
                // check for relation annotation: not within word, but within
                // relation
              } else if ((currentList.get(MAPPING_TYPE_WORD).size() == 0)
                  && (currentList.get(MAPPING_TYPE_RELATION).size() > 0)
                  && (tmpCurrentType = relationAnnotationTypes
                      .get(qname)) != null) {
                currentObject = new MtasParserObject(tmpCurrentType);
                collectAttributes(currentObject, streamReader);
                currentObject.setUnknownAncestorNumber(unknownAncestors);
                currentObject.setRealOffsetStart(lastOffset);
                if (!prevalidateObject(currentObject, currentList)) {
                  unknownAncestors++;
                } else {
                  currentType = tmpCurrentType;
                  currentList.get(MAPPING_TYPE_RELATION_ANNOTATION)
                      .add(currentObject);
                  unknownAncestors = 0;
                }
                // check for group: not within word, not within relation, not
                // within groupAnnotation
              } else if ((currentList.get(MAPPING_TYPE_WORD).size() == 0)
                  && (currentList.get(MAPPING_TYPE_RELATION).size() == 0)
                  && (currentList.get(MAPPING_TYPE_GROUP_ANNOTATION)
                      .size() == 0)
                  && (tmpCurrentType = groupTypes.get(qname)) != null) {
                currentObject = new MtasParserObject(tmpCurrentType);
                collectAttributes(currentObject, streamReader);
                currentObject.setUnknownAncestorNumber(unknownAncestors);
                currentObject.setRealOffsetStart(lastOffset);
                if (!prevalidateObject(currentObject, currentList)) {
                  unknownAncestors++;
                } else {
                  currentType = tmpCurrentType;
                  currentList.get(MAPPING_TYPE_GROUP).add(currentObject);
                  unknownAncestors = 0;
                }
                // check for group annotation: not within word, not within
                // relation, but within group
              } else if ((currentList.get(MAPPING_TYPE_WORD).size() == 0)
                  && (currentList.get(MAPPING_TYPE_RELATION).size() == 0)
                  && (currentList.get(MAPPING_TYPE_GROUP).size() > 0)
                  && (tmpCurrentType = groupAnnotationTypes
                      .get(qname)) != null) {
                currentObject = new MtasParserObject(tmpCurrentType);
                collectAttributes(currentObject, streamReader);
                currentObject.setUnknownAncestorNumber(unknownAncestors);
                currentObject.setRealOffsetStart(lastOffset);
                if (!prevalidateObject(currentObject, currentList)) {
                  unknownAncestors++;
                } else {
                  currentType = tmpCurrentType;
                  currentList.get(MAPPING_TYPE_GROUP_ANNOTATION)
                      .add(currentObject);
                  unknownAncestors = 0;
                }
                // check for word: not within relation, not within
                // groupAnnotation, not within word, not within wordAnnotation
              } else if ((currentList.get(MAPPING_TYPE_RELATION).size() == 0)
                  && (currentList.get(MAPPING_TYPE_GROUP_ANNOTATION)
                      .size() == 0)
                  && (currentList.get(MAPPING_TYPE_WORD).size() == 0)
                  && (currentList.get(MAPPING_TYPE_WORD_ANNOTATION).size() == 0)
                  && (tmpCurrentType = wordTypes.get(qname)) != null) {
                currentObject = new MtasParserObject(tmpCurrentType);
                collectAttributes(currentObject, streamReader);
                currentObject.setUnknownAncestorNumber(unknownAncestors);
                currentObject.setOffsetStart(lastOffset);
                currentObject.setRealOffsetStart(lastOffset);
                if (!prevalidateObject(currentObject, currentList)) {
                  unknownAncestors++;
                } else {
                  currentType = tmpCurrentType;
                  currentObject.addPosition(position.getAndIncrement());
                  currentList.get(MAPPING_TYPE_WORD).add(currentObject);
                  unknownAncestors = 0;
                }
                // check for word annotation: not within relation, not within
                // groupAnnotation, but within word
              } else if ((currentList.get(MAPPING_TYPE_RELATION).size() == 0)
                  && (currentList.get(MAPPING_TYPE_GROUP_ANNOTATION)
                      .size() == 0)
                  && (currentList.get(MAPPING_TYPE_WORD).size() > 0)
                  && (tmpCurrentType = wordAnnotationTypes
                      .get(qname)) != null) {
                currentObject = new MtasParserObject(tmpCurrentType);
                collectAttributes(currentObject, streamReader);
                currentObject.addPositions(currentList.get(MAPPING_TYPE_WORD)
                    .get((currentList.get(MAPPING_TYPE_WORD).size() - 1))
                    .getPositions());
                currentObject.setUnknownAncestorNumber(unknownAncestors);
                currentObject.setRealOffsetStart(lastOffset);
                if (!prevalidateObject(currentObject, currentList)) {
                  unknownAncestors++;
                } else {
                  currentType = tmpCurrentType;
                  currentList.get(MAPPING_TYPE_WORD_ANNOTATION)
                      .add(currentObject);
                  unknownAncestors = 0;
                }
                // check for references: within relation
              } else if ((currentList.get(MAPPING_TYPE_RELATION).size() > 0)
                  && (tmpCurrentType = refTypes.get(qname)) != null) {
                currentObject = new MtasParserObject(tmpCurrentType);
                collectAttributes(currentObject, streamReader);
                currentObject.setUnknownAncestorNumber(unknownAncestors);
                currentObject.setRealOffsetStart(lastOffset);
                if (!prevalidateObject(currentObject, currentList)) {
                  unknownAncestors++;
                } else {
                  currentType = tmpCurrentType;
                  currentList.get(MAPPING_TYPE_REF).add(currentObject);
                  unknownAncestors = 0;
                  // add reference to ancestor relations
                  for (MtasParserObject currentRelation : currentList
                      .get(MAPPING_TYPE_RELATION)) {
                    currentRelation.addRefId(currentObject
                        .getAttribute(currentType.getRefAttributeName()));
                    // register mapping for relation (for recursive relations)
                    TreeSet<String> keyMapList;
                    if (currentRelation.getId() != null) {
                      if (relationKeyMap.containsKey(currentRelation.getId())) {
                        keyMapList = relationKeyMap
                            .get(currentRelation.getId());
                      } else {
                        keyMapList = new TreeSet<String>();
                        relationKeyMap.put(currentRelation.getId(), keyMapList);
                      }
                      keyMapList.add(currentObject
                          .getAttribute(currentType.getRefAttributeName()));
                    }
                  }
                }
              } else {
                unknownAncestors++;
              }
              // check for start content
            } else if (qname.equals(getQName(contentTag))) {
              parsingContent = true;
              // unexpected
            } else if (!allowNonContent) {
              throw new MtasParserException(
                  "Unexpected " + qname.getLocalPart() + " in document");
            }
            // set offset (end of start-element)
            lastOffset = streamReader.getLocation().getCharacterOffset();
            break;
          case XMLStreamConstants.END_ELEMENT:
            // set offset (end of end-element)
            lastOffset = streamReader.getLocation().getCharacterOffset();
            // get data
            qname = streamReader.getName();
            // parse content
            if (parsingContent) {
              if (unknownAncestors > 0) {
                unknownAncestors--;
                // check for reference: because otherwise currentList should
                // contain no references
              } else if (currentList.get(MAPPING_TYPE_REF).size() > 0) {
                if ((currentType = refTypes.get(qname)) != null) {
                  currentObject = currentList.get(MAPPING_TYPE_REF)
                      .remove(currentList.get(MAPPING_TYPE_REF).size() - 1);
                  assert currentObject.getType()
                      .equals(currentType) : "object expected to be "
                          + currentObject.getType().getName() + ", not "
                          + currentType.getName();
                  assert unknownAncestors == 0 : "error in administration "
                      + currentObject.getType().getName();
                  // ignore text and realOffset: not relevant
                  idPositions.put(currentObject.getId(),
                      currentObject.getPositions());
                  idOffsets.put(currentObject.getId(),
                      currentObject.getOffset());
                  currentObject.updateMappings(idPositions, idOffsets);
                  unknownAncestors = currentObject.getUnknownAncestorNumber();
                  // todo: necessary???
                  computeMappingsFromObject(currentObject, currentList,
                      updateList);
                } else {
                  // this shouldn't happen
                }
                // check for wordAnnotation: because otherwise currentList
                // should contain no wordAnnotations
              } else if (currentList.get(MAPPING_TYPE_WORD_ANNOTATION)
                  .size() > 0) {
                if ((currentType = wordAnnotationTypes.get(qname)) != null) {
                  currentObject = currentList.get(MAPPING_TYPE_WORD_ANNOTATION)
                      .remove(
                          currentList.get(MAPPING_TYPE_WORD_ANNOTATION).size()
                              - 1);
                  assert currentObject.getType()
                      .equals(currentType) : "object expected to be "
                          + currentObject.getType().getName() + ", not "
                          + currentType.getName();
                  assert unknownAncestors == 0 : "error in administration "
                      + currentObject.getType().getName();
                  currentObject.setRealOffsetEnd(lastOffset);
                  idPositions.put(currentObject.getId(),
                      currentObject.getPositions());
                  idOffsets.put(currentObject.getId(),
                      currentObject.getOffset());
                  // offset always null, so update later with word (should be
                  // possible)
                  if ((currentObject.getId() != null)
                      && (currentList.get(MAPPING_TYPE_WORD).size() > 0)) {
                    currentList.get(MAPPING_TYPE_WORD)
                        .get((currentList.get(MAPPING_TYPE_WORD).size() - 1))
                        .addUpdateableIdWithOffset(currentObject.getId());
                  }
                  currentObject.updateMappings(idPositions, idOffsets);
                  unknownAncestors = currentObject.getUnknownAncestorNumber();
                  computeMappingsFromObject(currentObject, currentList,
                      updateList);
                } else {
                  // this shouldn't happen
                }
                // check for word: because otherwise currentList should contain
                // no words
              } else if (currentList.get(MAPPING_TYPE_WORD).size() > 0) {
                if ((currentType = wordTypes.get(qname)) != null) {
                  currentObject = currentList.get(MAPPING_TYPE_WORD)
                      .remove(currentList.get(MAPPING_TYPE_WORD).size() - 1);
                  assert currentObject.getType()
                      .equals(currentType) : "object expected to be "
                          + currentObject.getType().getName() + ", not "
                          + currentType.getName();
                  assert unknownAncestors == 0 : "error in administration "
                      + currentObject.getType().getName();
                  currentObject.setOffsetEnd(lastOffset);
                  currentObject.setRealOffsetEnd(lastOffset);
                  // update ancestor groups with position and offset
                  for (MtasParserObject currentGroup : currentList
                      .get(MAPPING_TYPE_GROUP)) {
                    currentGroup.addPositions(currentObject.getPositions());
                    currentGroup.addOffsetStart(currentObject.getOffsetStart());
                    currentGroup.addOffsetEnd(currentObject.getOffsetEnd());
                  }
                  idPositions.put(currentObject.getId(),
                      currentObject.getPositions());
                  idOffsets.put(currentObject.getId(),
                      currentObject.getOffset());
                  currentObject.updateMappings(idPositions, idOffsets);
                  unknownAncestors = currentObject.getUnknownAncestorNumber();
                  computeMappingsFromObject(currentObject, currentList,
                      updateList);
                } else {
                  // this shouldn't happen
                }
                // check for group annotation: because otherwise currentList
                // should contain no groupAnnotations
              } else if (currentList.get(MAPPING_TYPE_GROUP_ANNOTATION)
                  .size() > 0) {
                if ((currentType = groupAnnotationTypes.get(qname)) != null) {
                  currentObject = currentList.get(MAPPING_TYPE_GROUP_ANNOTATION)
                      .remove(
                          currentList.get(MAPPING_TYPE_GROUP_ANNOTATION).size()
                              - 1);
                  assert currentObject.getType()
                      .equals(currentType) : "object expected to be "
                          + currentObject.getType().getName() + ", not "
                          + currentType.getName();
                  assert unknownAncestors == 0 : "error in administration "
                      + currentObject.getType().getName();
                  currentObject.setRealOffsetEnd(lastOffset);
                  idPositions.put(currentObject.getId(),
                      currentObject.getPositions());
                  idOffsets.put(currentObject.getId(),
                      currentObject.getOffset());
                  currentObject.updateMappings(idPositions, idOffsets);
                  unknownAncestors = currentObject.getUnknownAncestorNumber();
                  computeMappingsFromObject(currentObject, currentList,
                      updateList);
                } else {
                  // this shouldn't happen
                }
                // check for relation annotation
              } else if (currentList.get(MAPPING_TYPE_RELATION_ANNOTATION)
                  .size() > 0) {
                if ((currentType = relationAnnotationTypes
                    .get(qname)) != null) {
                  currentObject = currentList
                      .get(MAPPING_TYPE_RELATION_ANNOTATION).remove(currentList
                          .get(MAPPING_TYPE_RELATION_ANNOTATION).size() - 1);
                  assert currentObject.getType()
                      .equals(currentType) : "object expected to be "
                          + currentObject.getType().getName() + ", not "
                          + currentType.getName();
                  assert unknownAncestors == 0 : "error in administration "
                      + currentObject.getType().getName();
                  currentObject.setRealOffsetEnd(lastOffset);
                  idPositions.put(currentObject.getId(),
                      currentObject.getPositions());
                  idOffsets.put(currentObject.getId(),
                      currentObject.getOffset());
                  currentObject.updateMappings(idPositions, idOffsets);
                  unknownAncestors = currentObject.getUnknownAncestorNumber();
                  computeMappingsFromObject(currentObject, currentList,
                      updateList);
                } else {
                  // this shouldn't happen
                }
                // check for relation
              } else if (currentList.get(MAPPING_TYPE_RELATION).size() > 0) {
                if ((currentType = relationTypes.get(qname)) != null) {
                  currentObject = currentList.get(MAPPING_TYPE_RELATION).remove(
                      currentList.get(MAPPING_TYPE_RELATION).size() - 1);
                  assert currentObject.getType()
                      .equals(currentType) : "object expected to be "
                          + currentObject.getType().getName() + ", not "
                          + currentType.getName();
                  assert unknownAncestors == 0 : "error in administration "
                      + currentObject.getType().getName();
                  // ignore text: should not occur
                  currentObject.setRealOffsetEnd(lastOffset);
                  idPositions.put(currentObject.getId(),
                      currentObject.getPositions());
                  idOffsets.put(currentObject.getId(),
                      currentObject.getOffset());
                  currentObject.updateMappings(idPositions, idOffsets);
                  unknownAncestors = currentObject.getUnknownAncestorNumber();
                  computeMappingsFromObject(currentObject, currentList,
                      updateList);
                } else {
                  // this shouldn't happen
                }
                // check for group
              } else if (currentList.get(MAPPING_TYPE_GROUP).size() > 0) {
                if ((currentType = groupTypes.get(qname)) != null) {
                  currentObject = currentList.get(MAPPING_TYPE_GROUP)
                      .remove(currentList.get(MAPPING_TYPE_GROUP).size() - 1);
                  assert currentObject.getType()
                      .equals(currentType) : "object expected to be "
                          + currentObject.getType().getName() + ", not "
                          + currentType.getName();
                  assert unknownAncestors == 0 : "error in administration "
                      + currentObject.getType().getName();
                  // ignore text: should not occur
                  currentObject.setRealOffsetEnd(lastOffset);
                  idPositions.put(currentObject.getId(),
                      currentObject.getPositions());
                  idOffsets.put(currentObject.getId(),
                      currentObject.getOffset());
                  currentObject.updateMappings(idPositions, idOffsets);
                  unknownAncestors = currentObject.getUnknownAncestorNumber();
                  computeMappingsFromObject(currentObject, currentList,
                      updateList);
                } else {
                  unknownAncestors--;
                }
              } else if (qname.equals(getQName("text"))) {
                parsingContent = false;
                assert unknownAncestors == 0 : "error in administration unknownAncestors";
                assert currentList.get(MAPPING_TYPE_REF)
                    .size() == 0 : "error in administration references";
                assert currentList.get(MAPPING_TYPE_GROUP)
                    .size() == 0 : "error in administration groups";
                assert currentList.get(MAPPING_TYPE_GROUP_ANNOTATION)
                    .size() == 0 : "error in administration groupAnnotations";
                assert currentList.get(MAPPING_TYPE_WORD)
                    .size() == 0 : "error in administration words";
                assert currentList.get(MAPPING_TYPE_WORD_ANNOTATION)
                    .size() == 0 : "error in administration wordAnnotations";
                assert currentList.get(MAPPING_TYPE_RELATION)
                    .size() == 0 : "error in administration relations";
                assert currentList.get(MAPPING_TYPE_RELATION_ANNOTATION)
                    .size() == 0 : "error in administration relationAnnotations";
              }
            }
            // forget text
            textContent = null;
            break;
          case XMLStreamConstants.CHARACTERS:
            // set offset (end of start-element)
            lastOffset = streamReader.getLocation().getCharacterOffset();
            // check for text
            if (streamReader.hasText()) {
              textContent = streamReader.getText();
            }
            if (currentObject != null) {
              currentObject.addText(textContent);
            }
            break;
          }
          if (!streamReader.hasNext()) {
            break;
          }
          event = streamReader.next();
        }
      } finally {
        streamReader.close();
      }
      // final checks
      assert unknownAncestors == 0 : "error in administration unknownAncestors";
      assert hasRoot : "no " + rootTag;
    } catch (XMLStreamException e) {
      throw new MtasParserException("No valid XML: " + e.getMessage());
    }
    // update tokens with offset
    for (Entry<Integer, HashSet<String>> updateItem : updateList
        .get(UPDATE_TYPE_OFFSET).entrySet()) {
      HashSet<String> refIdList = new HashSet<String>();
      for (String refId : updateItem.getValue()) {
        if (idPositions.containsKey(refId)) {
          refIdList.add(refId);
        }
        if (relationKeyMap.containsKey(refId)) {
          refIdList.addAll(recursiveCollect(refId, relationKeyMap, 10));
        }
      }
      for (String refId : refIdList) {
        Integer[] refOffset = idOffsets.get(refId);
        Integer tokenId = updateItem.getKey();
        if (tokenId != null) {
          if (refOffset != null) {
            MtasToken<?> token = tokenCollection.get(tokenId);
            token.addOffset(refOffset[0], refOffset[1]);
          } else {

          }
        }
      }
    }
    // update tokens with position
    for (Entry<Integer, HashSet<String>> updateItem : updateList
        .get(UPDATE_TYPE_POSITION).entrySet()) {
      HashSet<String> refIdList = new HashSet<String>();
      for (String refId : updateItem.getValue()) {
        if (idPositions.containsKey(refId)) {
          refIdList.add(refId);
        }
        if (relationKeyMap.containsKey(refId)) {
          refIdList.addAll(recursiveCollect(refId, relationKeyMap, 10));
        }
      }
      for (String refId : refIdList) {
        TreeSet<Integer> refPositions = idPositions.get(refId);
        Integer tokenId = updateItem.getKey();
        if (tokenId != null) {
          if (refPositions != null) {
            MtasToken<?> token = tokenCollection.get(tokenId);
            token.addPositions(refPositions);
          }
        }
      }
    }
    // final check
    tokenCollection.check(autorepair, makeunique);
    return tokenCollection;
  }

  /**
   * Recursive collect.
   *
   * @param refId the ref id
   * @param relationKeyMap the relation key map
   * @param maxRecursion the max recursion
   * @return the collection<? extends string>
   */
  private Collection<? extends String> recursiveCollect(String refId,
      HashMap<String, TreeSet<String>> relationKeyMap, int maxRecursion) {
    HashSet<String> list = new HashSet<String>();
    if (maxRecursion > 0 && relationKeyMap.containsKey(refId)) {
      TreeSet<String> subList = relationKeyMap.get(refId);
      for (String subRefId : subList) {
        list.add(subRefId);
        list.addAll(
            recursiveCollect(subRefId, relationKeyMap, maxRecursion - 1));
      }
    }
    return list;
  }

  /**
   * Gets the q name.
   *
   * @param key the key
   * @return the q name
   */
  private QName getQName(String key) {
    QName qname;
    if ((qname = qNames.get(key)) == null) {
      qname = new QName(namespaceURI, key);
      qNames.put(key, qname);
    }
    return qname;
  }

  /**
   * Collect attributes.
   *
   * @param currentObject the current object
   * @param streamReader the stream reader
   */
  public void collectAttributes(MtasParserObject currentObject,
      XMLStreamReader streamReader) {
    String attributeNamespaceURI;
    currentObject.objectAttributes.clear();
    currentObject.objectId = streamReader.getAttributeValue(namespaceURI_id,
        "id");
    for (int i = 0; i < streamReader.getAttributeCount(); i++) {
      attributeNamespaceURI = streamReader.getAttributeNamespace(i);
      if (attributeNamespaceURI == null || attributeNamespaceURI.equals("")) {
        attributeNamespaceURI = streamReader.getNamespaceURI();
      }
      if (namespaceURI == null || attributeNamespaceURI.equals(namespaceURI)) {
        currentObject.objectAttributes.put(
            streamReader.getAttributeLocalName(i),
            streamReader.getAttributeValue(i));
      }
    }
  }

  /**
   * The Class MtasXMLParserMappingRelation.
   */
  private class MtasXMLParserMappingRelation
      extends MtasParserMapping<MtasXMLParserMappingRelation> {

    /**
     * Instantiates a new mtas xml parser mapping relation.
     */
    public MtasXMLParserMappingRelation() {
      super();
      this.position = SOURCE_REFS;
      this.realOffset = SOURCE_OWN;
      this.offset = SOURCE_REFS;
      this.type = MAPPING_TYPE_RELATION;
    }

    /*
     * (non-Javadoc)
     * 
     * @see mtas.analysis.parser.MtasBasicParser.MtasParserMapping#self()
     */
    @Override
    protected MtasXMLParserMappingRelation self() {
      return this;
    }
  }

  /**
   * The Class MtasXMLParserMappingRelationAnnotation.
   */
  private class MtasXMLParserMappingRelationAnnotation
      extends MtasParserMapping<MtasXMLParserMappingRelationAnnotation> {

    /**
     * Instantiates a new mtas xml parser mapping relation annotation.
     */
    public MtasXMLParserMappingRelationAnnotation() {
      super();
      this.position = SOURCE_ANCESTOR_RELATION;
      this.realOffset = SOURCE_OWN;
      this.offset = SOURCE_ANCESTOR_RELATION;
      this.type = MAPPING_TYPE_RELATION_ANNOTATION;
    }

    /*
     * (non-Javadoc)
     * 
     * @see mtas.analysis.parser.MtasBasicParser.MtasParserMapping#self()
     */
    @Override
    protected MtasXMLParserMappingRelationAnnotation self() {
      return this;
    }

  }

  /**
   * The Class MtasXMLParserMappingGroup.
   */
  private class MtasXMLParserMappingGroup
      extends MtasParserMapping<MtasXMLParserMappingGroup> {

    /**
     * Instantiates a new mtas xml parser mapping group.
     */
    public MtasXMLParserMappingGroup() {
      super();
      this.position = SOURCE_OWN;
      this.realOffset = SOURCE_OWN;
      this.offset = SOURCE_OWN;
      this.type = MAPPING_TYPE_GROUP;
    }

    /*
     * (non-Javadoc)
     * 
     * @see mtas.analysis.parser.MtasBasicParser.MtasParserMapping#self()
     */
    @Override
    protected MtasXMLParserMappingGroup self() {
      return this;
    }
  }

  /**
   * The Class MtasXMLParserMappingGroupAnnotation.
   */
  private class MtasXMLParserMappingGroupAnnotation
      extends MtasParserMapping<MtasXMLParserMappingGroupAnnotation> {

    /**
     * Instantiates a new mtas xml parser mapping group annotation.
     */
    public MtasXMLParserMappingGroupAnnotation() {
      super();
      this.position = SOURCE_ANCESTOR_GROUP;
      this.realOffset = SOURCE_OWN;
      this.offset = SOURCE_ANCESTOR_GROUP;
      this.type = MAPPING_TYPE_GROUP_ANNOTATION;
    }

    /*
     * (non-Javadoc)
     * 
     * @see mtas.analysis.parser.MtasBasicParser.MtasParserMapping#self()
     */
    @Override
    protected MtasXMLParserMappingGroupAnnotation self() {
      return this;
    }

  }

  /**
   * The Class MtasXMLParserMappingWord.
   */
  private class MtasXMLParserMappingWord
      extends MtasParserMapping<MtasXMLParserMappingWord> {

    /**
     * Instantiates a new mtas xml parser mapping word.
     */
    public MtasXMLParserMappingWord() {
      super();
      this.position = SOURCE_OWN;
      this.realOffset = SOURCE_OWN;
      this.offset = SOURCE_OWN;
      this.type = MAPPING_TYPE_WORD;
    }

    /*
     * (non-Javadoc)
     * 
     * @see mtas.analysis.parser.MtasBasicParser.MtasParserMapping#self()
     */
    @Override
    protected MtasXMLParserMappingWord self() {
      return this;
    }
  }

  /**
   * The Class MtasXMLParserMappingWordAnnotation.
   */
  private class MtasXMLParserMappingWordAnnotation
      extends MtasParserMapping<MtasXMLParserMappingWordAnnotation> {

    /**
     * Instantiates a new mtas xml parser mapping word annotation.
     */
    public MtasXMLParserMappingWordAnnotation() {
      super();
      this.position = SOURCE_OWN;
      this.realOffset = SOURCE_OWN;
      this.offset = SOURCE_ANCESTOR_WORD;
      this.type = MAPPING_TYPE_WORD_ANNOTATION;
    }

    /*
     * (non-Javadoc)
     * 
     * @see mtas.analysis.parser.MtasBasicParser.MtasParserMapping#self()
     */
    @Override
    protected MtasXMLParserMappingWordAnnotation self() {
      return this;
    }
  }

}
