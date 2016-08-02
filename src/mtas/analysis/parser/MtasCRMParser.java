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

public class MtasCRMParser extends MtasBasicParser {

  /** The word type. */
  private MtasParserType wordType = null;

  /** The word annotation types. */
  private HashMap<String, MtasParserType> wordAnnotationTypes = new HashMap<String, MtasParserType>();

  private HashMap<String, MtasCRMParserFunction> functions = new HashMap<String, MtasCRMParserFunction>();

  public MtasCRMParser(MtasConfiguration config) {
    super(config);
    try {
      initParser();
      // System.out.print(printConfig());
    } catch (MtasConfigException e) {
      e.printStackTrace();
    }
  }

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
                  MtasCRMParserMappingWordAnnotation m = new MtasCRMParserMappingWordAnnotation();
                  m.processConfig(mapping);
                  wordType.addMapping(m);
                } else if (typeMapping.equals(MAPPING_TYPE_WORD_ANNOTATION)
                    && (nameMapping != null)) {
                  MtasCRMParserMappingWordAnnotation m = new MtasCRMParserMappingWordAnnotation();
                  m.processConfig(mapping);
                  if (wordAnnotationTypes.containsKey(nameMapping)) {
                    wordAnnotationTypes.get(nameMapping).addMapping(m);
                  } else {
                    MtasParserType t = new MtasParserType(typeMapping,
                        nameMapping);
                    t.addMapping(m);
                    wordAnnotationTypes.put(nameMapping, t);
                  }
                } else {
                  throw new MtasConfigException("unknown mapping type "
                      + typeMapping + " or missing name");
                }
              }
            }
          }
        } else if (current.name.equals("functions")) {
          for (int j = 0; j < current.children.size(); j++) {
            if (current.children.get(j).name.equals("function")) {
              MtasConfiguration function = current.children.get(j);
              String nameFunction = function.attributes.get("name");
              String splitFunction = function.attributes.get("split");
              if (nameFunction != null) {
                MtasCRMParserFunction mtasCRMParserFunction = new MtasCRMParserFunction(
                    splitFunction);
                functions.put(nameFunction, mtasCRMParserFunction);
                MtasConfiguration subCurrent = current.children.get(j);
                for (int k = 0; k < subCurrent.children.size(); k++) {
                  if (subCurrent.children.get(k).name.equals("condition")) {
                    MtasConfiguration subSubCurrent = subCurrent.children
                        .get(k);
                    if (subSubCurrent.attributes.containsKey("value")) {
                      String[] valuesCondition = subSubCurrent.attributes
                          .get("value").split(Pattern.quote(","));
                      ArrayList<MtasCRMParserFunctionOutput> valueOutputList = new ArrayList<MtasCRMParserFunctionOutput>();
                      for (int l = 0; l < subSubCurrent.children.size(); l++) {
                        if (subSubCurrent.children.get(l).name
                            .equals("output")) {
                          String valueOutput = subSubCurrent.children
                              .get(l).attributes.get("value");
                          String nameOutput = subSubCurrent.children
                              .get(l).attributes.get("name");
                          if (nameOutput != null) {
                            MtasCRMParserFunctionOutput o = new MtasCRMParserFunctionOutput(
                                nameOutput, valueOutput);
                            valueOutputList.add(o);
                          }
                        }
                      }
                      if (valueOutputList.size() > 0) {
                        for (String valueCondition : valuesCondition) {
                          mtasCRMParserFunction.output.put(valueCondition,
                              valueOutputList);
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

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
      MtasParserObject currentObject;
      Pattern headerPattern = Pattern.compile("^@ @ @(.*)$");
      Pattern regularPattern = Pattern.compile(
          "^([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+)$");
      Matcher matcherHeader, matcherRegular;
      while ((line = br.readLine()) != null) {
        currentOffset = br.getPosition();
        matcherHeader = headerPattern.matcher(line.trim());
        if (matcherHeader.matches()) {
          // System.out.println(line);
        } else {
          matcherRegular = regularPattern.matcher(line.trim());
          if (matcherRegular.matches()) {
            // regular line
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
                  && (currentList.get(MAPPING_TYPE_GROUP_ANNOTATION)
                      .size() == 0)
                  && (currentList.get(MAPPING_TYPE_WORD).size() > 0)) {
                // compute word annotations
                for (int i = 0; i < 8; i++) {
                  ArrayList<MtasCRMParserFunctionOutput> functionOutputList = new ArrayList<MtasCRMParserFunctionOutput>();
                  processWordAnnotation(String.valueOf(i),
                      matcherRegular.group((i + 1)), previousOffset,
                      currentOffset, functionOutputList, unknownAncestors,
                      currentList, updateList, idPositions, idOffsets);
                  for (MtasCRMParserFunctionOutput functionOutput : functionOutputList) {
                    processWordAnnotation(functionOutput.name,
                        functionOutput.value, previousOffset, currentOffset,
                        functionOutputList, unknownAncestors, currentList,
                        updateList, idPositions, idOffsets);
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
                computeMappingsFromObject(currentObject, currentList,
                    updateList);
              }
            }
          } else {
            //System.out.println("PROBLEM: " + line);
          }
        }
        previousOffset = br.getPosition();
      }      
    } catch (IOException e) {
      throw new MtasParserException(e.getMessage());
    }
    // final check
    tokenCollection.check(autorepair);
    return tokenCollection;

  }

  private void processWordAnnotation(String name, String text,
      Integer previousOffset, Integer currentOffset,
      ArrayList<MtasCRMParserFunctionOutput> functionOutputList,
      Integer unknownAncestors,
      HashMap<String, ArrayList<MtasParserObject>> currentList,
      HashMap<String, HashMap<Integer, HashSet<String>>> updateList,
      HashMap<String, TreeSet<Integer>> idPositions,
      HashMap<String, Integer[]> idOffsets)
      throws MtasParserException, MtasConfigException {
    MtasParserType tmpCurrentType;
    MtasParserObject currentObject;    
    if ((tmpCurrentType = wordAnnotationTypes.get(name)) != null) {
      // start word annotation
      currentObject = new MtasParserObject(tmpCurrentType);
      currentObject.setRealOffsetStart(previousOffset);
      currentObject.addPositions(currentList.get(MAPPING_TYPE_WORD)
          .get((currentList.get(MAPPING_TYPE_WORD).size() - 1)).getPositions());
      currentObject.setUnknownAncestorNumber(unknownAncestors);
      if (!prevalidateObject(currentObject, currentList)) {
        unknownAncestors++;
      } else {
        currentList.get(MAPPING_TYPE_WORD_ANNOTATION).add(currentObject);
        unknownAncestors = 0;
      }
      // finish word annotation
      if (unknownAncestors > 0) {
        unknownAncestors--;
      } else {
        currentObject = currentList.get(MAPPING_TYPE_WORD_ANNOTATION)
            .remove(currentList.get(MAPPING_TYPE_WORD_ANNOTATION).size() - 1);
        assert unknownAncestors == 0 : "error in administration "
            + currentObject.getType().getName();
        if (functions.containsKey(name) && text!=null) {
          MtasCRMParserFunction function = functions.get(name);
          String[] value;
          if (function.split != null) {
            value = text.split(Pattern.quote(function.split));
          } else {
            value = new String[] { text };
          }
          for (int c = 0; c < value.length; c++) {
            if (function.output.containsKey(value[c])) {
              functionOutputList.addAll(function.output.get(value[c]));
            }
          }
        }
        currentObject.setText(text);
        currentObject.setRealOffsetEnd(currentOffset - 1);
        idPositions.put(currentObject.getId(), currentObject.getPositions());
        idOffsets.put(currentObject.getId(), currentObject.getOffset());
        // offset always null, so update later with word (should
        // be
        // possible)
        if ((currentObject.getId() != null)
            && (currentList.get(MAPPING_TYPE_WORD).size() > 0)) {
          currentList.get(MAPPING_TYPE_WORD)
              .get((currentList.get(MAPPING_TYPE_WORD).size() - 1))
              .addUpdateableIdWithOffset(currentObject.getId());
        }
        currentObject.updateMappings(idPositions, idOffsets);
        unknownAncestors = currentObject.getUnknownAncestorNumber();
        computeMappingsFromObject(currentObject, currentList, updateList);
      }
    }
  }

  @Override
  public String printConfig() {
    String text = "";
    text += "=== CONFIGURATION ===\n";
    text += "type: " + wordAnnotationTypes.size() + " x wordAnnotation";
    text += printConfigTypes(wordAnnotationTypes);
    text += "=== CONFIGURATION ===\n";
    return text;
  }

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

  private class MtasCRMParserFunction {

    public String type;
    public String split;
    public HashMap<String, ArrayList<MtasCRMParserFunctionOutput>> output;

    public MtasCRMParserFunction(String split) {
      this.split = split;
      output = new HashMap<String, ArrayList<MtasCRMParserFunctionOutput>>();
    }

  }

  private class MtasCRMParserFunctionOutput {
    public String name;
    public String value;

    public MtasCRMParserFunctionOutput(String name, String value) {
      this.name = name;
      this.value = value;
    }
  }

  private class MtasCRMParserMappingWordAnnotation
      extends MtasParserMapping<MtasCRMParserMappingWordAnnotation> {

    /**
     * Instantiates a new mtas sketch parser mapping word annotation.
     */
    public MtasCRMParserMappingWordAnnotation() {
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
    protected MtasCRMParserMappingWordAnnotation self() {
      return this;
    }
  }

}
