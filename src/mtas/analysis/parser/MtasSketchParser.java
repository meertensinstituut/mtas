package mtas.analysis.parser;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mtas.analysis.token.MtasToken;
import mtas.analysis.token.MtasTokenCollection;
import mtas.analysis.util.MtasBufferedReader;
import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasConfiguration;
import mtas.analysis.util.MtasParserException;

/**
 * The Class MtasSketchParser.
 */
final public class MtasSketchParser extends MtasBasicParser {

  /** The Constant MAPPING_TYPE_WORD_ANNOTATION. */
  protected final static String MAPPING_TYPE_WORD_ANNOTATION = "wordAnnotation";

  /** The autorepair. */
  private Boolean autorepair = true;

  /** The word type. */
  private MtasParserType wordType = null;

  /** The word annotation types. */
  private HashMap<Integer, MtasParserType> wordAnnotationTypes = new HashMap<Integer, MtasParserType>();

  /** The group types. */
  private HashMap<String, MtasParserType> groupTypes = new HashMap<String, MtasParserType>();

  /**
   * Instantiates a new mtas sketch parser.
   *
   * @param config
   *          the config
   */
  public MtasSketchParser(MtasConfiguration config) {
    super(config);
    try {
      initParser();
      // System.out.print(printConfig());
    } catch (MtasConfigException e) {
      e.printStackTrace();
    }
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

      // always word, no mappings
      wordType = new MtasParserType(MAPPING_TYPE_WORD, null);

      for (int i = 0; i < config.children.size(); i++) {
        MtasConfiguration current = config.children.get(i);
        if (current.name.equals("mappings")) {
          for (int j = 0; j < current.children.size(); j++) {
            if (current.children.get(j).name.equals("mapping")) {
              MtasConfiguration mapping = current.children.get(j);
              String typeMapping = mapping.attributes.get("type");
              String nameMapping = mapping.attributes.get("name");
              if ((typeMapping != null)) {
                if (typeMapping.equals(MAPPING_TYPE_WORD)) {
                  MtasSketchParserMappingWord m = new MtasSketchParserMappingWord();
                  m.processConfig(mapping);
                  wordType.addMapping(m);
                } else if (typeMapping.equals(MAPPING_TYPE_WORD_ANNOTATION)
                    && (nameMapping != null)) {
                  MtasSketchParserMappingWordAnnotation m = new MtasSketchParserMappingWordAnnotation();
                  m.processConfig(mapping);
                  if (wordAnnotationTypes.containsKey(nameMapping)) {
                    wordAnnotationTypes.get(nameMapping).addMapping(m);
                  } else {
                    MtasParserType t = new MtasParserType(typeMapping,
                        nameMapping);
                    t.addMapping(m);
                    wordAnnotationTypes.put(Integer.parseInt(nameMapping), t);
                  }
                } else if (typeMapping.equals(MAPPING_TYPE_GROUP)
                    && (nameMapping != null)) {
                  MtasSketchParserMappingGroup m = new MtasSketchParserMappingGroup();
                  m.processConfig(mapping);
                  if (groupTypes.containsKey(nameMapping)) {
                    groupTypes.get(nameMapping).addMapping(m);
                  } else {
                    MtasParserType t = new MtasParserType(typeMapping,
                        nameMapping);
                    t.addMapping(m);
                    groupTypes.put(nameMapping, t);
                  }
                } else {
                  throw new MtasConfigException("unknown mapping type "
                      + typeMapping + " or missing name");
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
    AtomicInteger position = new AtomicInteger(0);
    Integer unknownAncestors = 0;

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
    try (MtasBufferedReader br = new MtasBufferedReader(reader)) {
      String line;
      int currentOffset, previousOffset = br.getPosition();
      MtasParserType tmpCurrentType;
      MtasParserObject currentObject;
      Pattern groupPattern = Pattern.compile("^<([^\\/>]+)\\/>$");
      Pattern groupStartPattern = Pattern
          .compile("^<([^>\\/\\s][^>\\s]*)(|\\s[^>]+)>$");
      Pattern groupEndPattern = Pattern.compile("^<\\/([^>\\s]+)>$");
      Pattern attributePattern = Pattern.compile("([^\\s]+)=\"([^\"]*)\"");
      while ((line = br.readLine()) != null) {
        currentOffset = br.getPosition();
        // group
        if (line.trim().matches("^<[^>]*>$")) {
          Matcher matcherGroupStart = groupStartPattern.matcher(line.trim());
          Matcher matcherGroupEnd = groupEndPattern.matcher(line.trim());
          Matcher matcherGroup = groupPattern.matcher(line.trim());
          if (matcherGroup.find()) {
            // full group, ignore
          } else if (matcherGroupStart.find()) {
            // start group
            // System.out.println("Start "+matcherGroupStart.group(1)+" -
            // "+matcherGroupStart.group(2));
            if ((currentList.get(MAPPING_TYPE_WORD).size() == 0)
                && (currentList.get(MAPPING_TYPE_RELATION).size() == 0)
                && (currentList.get(MAPPING_TYPE_GROUP_ANNOTATION).size() == 0)
                && (tmpCurrentType = groupTypes
                    .get(matcherGroupStart.group(1))) != null) {
              currentObject = new MtasParserObject(tmpCurrentType);
              currentObject.setUnknownAncestorNumber(unknownAncestors);
              currentObject.setRealOffsetStart(previousOffset);
              String attributeText = matcherGroupStart.group(2).trim();
              if (!attributeText.equals("")) {
                Matcher matcherAttribute = attributePattern
                    .matcher(attributeText);
                currentObject.objectAttributes = new HashMap<String, String>();
                while (matcherAttribute.find()) {
                  currentObject.objectAttributes.put(matcherAttribute.group(1),
                      matcherAttribute.group(2));
                }
              }
              if (!prevalidateObject(currentObject, currentList)) {
                unknownAncestors++;
              } else {
                currentList.get(MAPPING_TYPE_GROUP).add(currentObject);
                unknownAncestors = 0;
              }
            }
          } else if (matcherGroupEnd.find()) {
            // end group
            if (currentList.get(MAPPING_TYPE_GROUP).size() > 0) {
              if ((tmpCurrentType = groupTypes
                  .get(matcherGroupEnd.group(1))) != null) {
                currentObject = currentList.get(MAPPING_TYPE_GROUP)
                    .remove(currentList.get(MAPPING_TYPE_GROUP).size() - 1);
                assert unknownAncestors == 0 : "error in administration "
                    + currentObject.getType().getName();
                // ignore text: should not occur
                currentObject.setRealOffsetEnd(currentOffset - 1);
                idPositions.put(currentObject.getId(),
                    currentObject.getPositions());
                idOffsets.put(currentObject.getId(), currentObject.getOffset());
                currentObject.updateMappings(idPositions, idOffsets);
                unknownAncestors = currentObject.getUnknownAncestorNumber();
                computeMappingsFromObject(currentObject, currentList,
                    updateList);
              }
            }
          }
        } else {
          if ((currentList.get(MAPPING_TYPE_RELATION).size() == 0)
              && (currentList.get(MAPPING_TYPE_GROUP_ANNOTATION).size() == 0)
              && (currentList.get(MAPPING_TYPE_WORD).size() == 0)
              && (currentList.get(MAPPING_TYPE_WORD_ANNOTATION).size() == 0)
              && (wordType != null)) {
            // start word
            currentObject = new MtasParserObject(wordType);
            currentObject.setOffsetStart(previousOffset);
            currentObject.setRealOffsetStart(previousOffset);
            currentObject.setUnknownAncestorNumber(unknownAncestors);
            if (!prevalidateObject(currentObject, currentList)) {
              unknownAncestors++;
            } else {
              int p = position.getAndIncrement();
              currentObject.addPosition(p);
              currentList.get(MAPPING_TYPE_WORD).add(currentObject);
              unknownAncestors = 0;
            }
            if ((currentList.get(MAPPING_TYPE_RELATION).size() == 0)
                && (currentList.get(MAPPING_TYPE_GROUP_ANNOTATION).size() == 0)
                && (currentList.get(MAPPING_TYPE_WORD).size() > 0)) {
              // start and finish word annotations
              String[] items = line.split("\t");
              for (int i = 0; i < items.length; i++) {
                if ((tmpCurrentType = wordAnnotationTypes.get(i)) != null) {
                  // start word annotation
                  currentObject = new MtasParserObject(tmpCurrentType);
                  currentObject.setRealOffsetStart(previousOffset);
                  currentObject.addPositions(currentList.get(MAPPING_TYPE_WORD)
                      .get((currentList.get(MAPPING_TYPE_WORD).size() - 1))
                      .getPositions());
                  currentObject.setUnknownAncestorNumber(unknownAncestors);
                  if (!prevalidateObject(currentObject, currentList)) {
                    unknownAncestors++;
                  } else {
                    currentList.get(MAPPING_TYPE_WORD_ANNOTATION)
                        .add(currentObject);
                    unknownAncestors = 0;
                  }
                  // finish word annotation
                  if (unknownAncestors > 0) {
                    unknownAncestors--;
                  } else {
                    currentObject = currentList
                        .get(MAPPING_TYPE_WORD_ANNOTATION).remove(
                            currentList.get(MAPPING_TYPE_WORD_ANNOTATION).size()
                                - 1);
                    assert unknownAncestors == 0 : "error in administration "
                        + currentObject.getType().getName();
                    currentObject.setText(items[i]);
                    currentObject.setRealOffsetEnd(currentOffset - 1);
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
                  }
                }
              }
            }
            // finish word
            if (unknownAncestors > 0) {
              unknownAncestors--;
            } else {
              currentObject = currentList.get(MAPPING_TYPE_WORD)
                  .remove(currentList.get(MAPPING_TYPE_WORD).size() - 1);
              assert unknownAncestors == 0 : "error in administration "
                  + currentObject.getType().getName();
              currentObject.setText(null);
              currentObject.setOffsetEnd(currentOffset - 1);
              currentObject.setRealOffsetEnd(currentOffset - 1);
              // update ancestor groups with position and offset
              for (MtasParserObject currentGroup : currentList
                  .get(MAPPING_TYPE_GROUP)) {
                currentGroup.addPositions(currentObject.getPositions());
                currentGroup.addOffsetStart(currentObject.getOffsetStart());
                currentGroup.addOffsetEnd(currentObject.getOffsetEnd());
              }
              idPositions.put(currentObject.getId(),
                  currentObject.getPositions());
              idOffsets.put(currentObject.getId(), currentObject.getOffset());
              currentObject.updateMappings(idPositions, idOffsets);
              unknownAncestors = currentObject.getUnknownAncestorNumber();
              computeMappingsFromObject(currentObject, currentList, updateList);
            }
          }
        }
        previousOffset = br.getPosition();
      }
    } catch (IOException e) {
      throw new MtasParserException(e.getMessage());
    }
    // update tokens with offset
    for (Entry<Integer, HashSet<String>> updateItem : updateList
        .get(UPDATE_TYPE_OFFSET).entrySet()) {
      for (String refId : updateItem.getValue()) {
        Integer[] refOffset = idOffsets.get(refId);
        if (refOffset != null) {
          tokenCollection.get(updateItem.getKey()).addOffset(refOffset[0],
              refOffset[1]);
        }
      }
    }
    // update tokens with position
    for (Entry<Integer, HashSet<String>> updateItem : updateList
        .get(UPDATE_TYPE_POSITION).entrySet()) {
      for (String refId : updateItem.getValue()) {
        MtasToken<?> token = tokenCollection.get(updateItem.getKey());
        token.addPositions(idPositions.get(refId));
      }
    }
    // final check
    tokenCollection.check(autorepair);
    return tokenCollection;
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
    text += "type: " + wordAnnotationTypes.size() + " x wordAnnotation";
    text += printConfigTypes(wordAnnotationTypes);
    text += "=== CONFIGURATION ===\n";
    return text;
  }

  /**
   * Prints the config types.
   *
   * @param types
   *          the types
   * @return the string
   */
  private String printConfigTypes(HashMap<?, MtasParserType> types) {
    String text = "";
    for (Entry<?, MtasParserType> entry : types.entrySet()) {
      text += "- " + entry.getKey() + ": " + entry.getValue().mappings.size()
          + " mapping(s)\n";
      for (int i = 0; i < entry.getValue().mappings.size(); i++) {
        text += "\t" + entry.getValue().mappings.get(i) + "\n";
      }
    }
    return text;
  }

  private class MtasSketchParserMappingWord
      extends MtasParserMapping<MtasSketchParserMappingWord> {
    public MtasSketchParserMappingWord() {
      super();
      this.position = SOURCE_OWN;
      this.realOffset = SOURCE_OWN;
      this.offset = SOURCE_OWN;
      this.type = MAPPING_TYPE_WORD;
    }
    
    @Override
    protected MtasSketchParserMappingWord self() {
      return this;
    }
  }

  /**
   * The Class MtasSketchParserMappingWordAnnotation.
   */
  private class MtasSketchParserMappingWordAnnotation
      extends MtasParserMapping<MtasSketchParserMappingWordAnnotation> {

    /**
     * Instantiates a new mtas sketch parser mapping word annotation.
     */
    public MtasSketchParserMappingWordAnnotation() {
      super();
      this.position = SOURCE_OWN;
      this.realOffset = SOURCE_OWN;
      this.offset = SOURCE_ANCESTOR_WORD;
      this.type = MAPPING_TYPE_WORD_ANNOTATION;
    }

    /*
     * (non-Javadoc)
     * 
     * @see mtas.analysis.parser.MtasParser.MtasParserMapping#self()
     */
    @Override
    protected MtasSketchParserMappingWordAnnotation self() {
      return this;
    }
  }

  /**
   * The Class MtasSketchParserMappingGroup.
   */
  private class MtasSketchParserMappingGroup
      extends MtasParserMapping<MtasSketchParserMappingGroup> {

    /**
     * Instantiates a new mtas sketch parser mapping group.
     */
    public MtasSketchParserMappingGroup() {
      super();
      this.position = SOURCE_OWN;
      this.realOffset = SOURCE_OWN;
      this.offset = SOURCE_OWN;
      this.type = MAPPING_TYPE_GROUP;
    }

    /*
     * (non-Javadoc)
     * 
     * @see mtas.analysis.parser.MtasFoliaParser.MtasFoliaParserMapping#self()
     */
    @Override
    protected MtasSketchParserMappingGroup self() {
      return this;
    }
  }

}
