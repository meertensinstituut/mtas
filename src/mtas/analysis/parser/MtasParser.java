package mtas.analysis.parser;

import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

import mtas.analysis.parser.MtasBasicParser.MtasParserType;
import mtas.analysis.token.MtasTokenCollection;
import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasConfiguration;
import mtas.analysis.util.MtasParserException;

/**
 * The Class MtasParser.
 */
abstract public class MtasParser {

  /** The token collection. */
  protected MtasTokenCollection tokenCollection;

  /** The config. */
  protected MtasConfiguration config;

  /** The autorepair. */
  protected Boolean autorepair = false;

  /** The makeunique. */
  protected Boolean makeunique = false;

  /**
   * Inits the parser.
   *
   * @throws MtasConfigException the mtas config exception
   */
  protected void initParser() throws MtasConfigException {
    if (config != null) {
      // find namespaceURI
      for (int i = 0; i < config.children.size(); i++) {
        MtasConfiguration current = config.children.get(i);
        if (current.name.equals("autorepair")) {
          autorepair = current.attributes.get("value").equals("true");
        }
        if (current.name.equals("makeunique")) {
          makeunique = current.attributes.get("value").equals("true");
        }
      }
    }
  }

  /**
   * Creates the token collection.
   *
   * @param reader the reader
   * @return the mtas token collection
   * @throws MtasParserException the mtas parser exception
   * @throws MtasConfigException the mtas config exception
   */
  public abstract MtasTokenCollection createTokenCollection(Reader reader)
      throws MtasParserException, MtasConfigException;

  /**
   * Prints the config.
   *
   * @return the string
   */
  public abstract String printConfig();

  /**
   * The Class MtasParserObject.
   */
  protected class MtasParserObject {

    /** The object type. */
    MtasParserType objectType;

    /** The object real offset start. */
    private Integer objectRealOffsetStart = null;

    /** The object real offset end. */
    private Integer objectRealOffsetEnd = null;

    /** The object offset start. */
    private Integer objectOffsetStart = null;

    /** The object offset end. */
    private Integer objectOffsetEnd = null;

    /** The object text. */
    private String objectText = null;

    /** The object id. */
    protected String objectId = null;

    /** The object unknown ancestor number. */
    private Integer objectUnknownAncestorNumber = null;

    /** The object attributes. */
    protected HashMap<String, String> objectAttributes = null;

    /** The object positions. */
    private TreeSet<Integer> objectPositions = new TreeSet<Integer>();

    /** The ref ids. */
    private HashSet<String> refIds = new HashSet<String>();

    /** The updateable mappings as parent. */
    private HashSet<Integer> updateableMappingsAsParent = new HashSet<Integer>();

    /** The updateable ids with position. */
    private HashSet<String> updateableIdsWithPosition = new HashSet<String>();

    /** The updateable mappings with position. */
    HashSet<Integer> updateableMappingsWithPosition = new HashSet<Integer>();

    /** The updateable ids with offset. */
    private HashSet<String> updateableIdsWithOffset = new HashSet<String>();

    /** The updateable mappings with offset. */
    HashSet<Integer> updateableMappingsWithOffset = new HashSet<Integer>();

    /**
     * Instantiates a new mtas parser object.
     *
     * @param type the type
     */
    MtasParserObject(MtasParserType type) {
      objectType = type;
      objectAttributes = new HashMap<String, String>();
    }

    /**
     * Register updateable mapping at parent.
     *
     * @param mappingId the mapping id
     */
    public void registerUpdateableMappingAtParent(Integer mappingId) {
      updateableMappingsAsParent.add(mappingId);
    }

    /**
     * Register updateable mappings at parent.
     *
     * @param mappingIds the mapping ids
     */
    public void registerUpdateableMappingsAtParent(
        HashSet<Integer> mappingIds) {
      updateableMappingsAsParent.addAll(mappingIds);
    }

    /**
     * Gets the updateable mappings as parent.
     *
     * @return the updateable mappings as parent
     */
    public HashSet<Integer> getUpdateableMappingsAsParent() {
      return updateableMappingsAsParent;
    }

    /**
     * Reset updateable mappings as parent.
     */
    public void resetUpdateableMappingsAsParent() {
      updateableMappingsAsParent.clear();
    }

    /**
     * Adds the updateable mapping with position.
     *
     * @param mappingId the mapping id
     */
    public void addUpdateableMappingWithPosition(Integer mappingId) {
      updateableMappingsWithPosition.add(mappingId);
    }

    /**
     * Adds the updateable id with offset.
     *
     * @param id the id
     */
    public void addUpdateableIdWithOffset(String id) {
      updateableIdsWithOffset.add(id);
    }

    /**
     * Adds the updateable mapping with offset.
     *
     * @param mappingId the mapping id
     */
    public void addUpdateableMappingWithOffset(Integer mappingId) {
      updateableMappingsWithOffset.add(mappingId);
    }

    /**
     * Update mappings.
     *
     * @param idPositions the id positions
     * @param idOffsets the id offsets
     */
    public void updateMappings(HashMap<String, TreeSet<Integer>> idPositions,
        HashMap<String, Integer[]> idOffsets) {
      for (Integer mappingId : updateableMappingsWithPosition) {
        tokenCollection.get(mappingId).addPositions(objectPositions);
      }
      for (Integer mappingId : updateableMappingsWithOffset) {
        tokenCollection.get(mappingId).addOffset(objectOffsetStart,
            objectOffsetEnd);
      }
      for (String id : updateableIdsWithPosition) {
        if (idPositions.containsKey(id)) {
          if (idPositions.get(id) == null) {
            idPositions.put(id, objectPositions);
          } else {
            idPositions.get(id).addAll(objectPositions);
          }
        }
      }
      for (String id : updateableIdsWithOffset) {
        if (idOffsets.containsKey(id)) {
          if (idOffsets.get(id) == null) {
            idOffsets.put(id,
                new Integer[] { objectOffsetStart, objectOffsetEnd });
          }
        }
      }
    }

    /**
     * Gets the attribute.
     *
     * @param name the name
     * @return the attribute
     */
    public String getAttribute(String name) {
      if (name != null) {
        return objectAttributes.get(name);
      } else {
        return null;
      }
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public String getId() {
      return objectId;
    }

    /**
     * Gets the type.
     *
     * @return the type
     */
    MtasParserType getType() {
      return objectType;
    }

    /**
     * Sets the text.
     *
     * @param text the new text
     */
    public void setText(String text) {
      objectText = text;
    }

    /**
     * Adds the text.
     *
     * @param text the text
     */
    public void addText(String text) {
      if (objectText == null) {
        objectText = text;
      } else {
        objectText += text;
      }
    }

    /**
     * Gets the text.
     *
     * @return the text
     */
    public String getText() {
      return objectText;
    }

    /**
     * Sets the unknown ancestor number.
     *
     * @param i the new unknown ancestor number
     */
    public void setUnknownAncestorNumber(Integer i) {
      objectUnknownAncestorNumber = i;
    }

    /**
     * Gets the unknown ancestor number.
     *
     * @return the unknown ancestor number
     */
    public Integer getUnknownAncestorNumber() {
      return objectUnknownAncestorNumber;
    }

    /**
     * Sets the real offset start.
     *
     * @param start the new real offset start
     */
    public void setRealOffsetStart(Integer start) {
      objectRealOffsetStart = start;
    }

    /**
     * Gets the real offset start.
     *
     * @return the real offset start
     */
    public Integer getRealOffsetStart() {
      return objectRealOffsetStart;
    }

    /**
     * Sets the real offset end.
     *
     * @param end the new real offset end
     */
    public void setRealOffsetEnd(Integer end) {
      objectRealOffsetEnd = end;
    }

    /**
     * Gets the real offset end.
     *
     * @return the real offset end
     */
    public Integer getRealOffsetEnd() {
      return objectRealOffsetEnd;
    }

    /**
     * Sets the offset start.
     *
     * @param start the new offset start
     */
    public void setOffsetStart(Integer start) {
      objectOffsetStart = start;
    }

    /**
     * Adds the offset start.
     *
     * @param start the start
     */
    public void addOffsetStart(Integer start) {
      if ((start != null)
          && ((objectOffsetStart == null) || (start < objectOffsetStart))) {
        objectOffsetStart = start;
      }
    }

    /**
     * Adds the offset end.
     *
     * @param end the end
     */
    public void addOffsetEnd(Integer end) {
      if ((end != null)
          && ((objectOffsetEnd == null) || (end > objectOffsetEnd))) {
        objectOffsetEnd = end;
      }
    }

    /**
     * Gets the offset start.
     *
     * @return the offset start
     */
    public Integer getOffsetStart() {
      return objectOffsetStart;
    }

    /**
     * Sets the offset end.
     *
     * @param end the new offset end
     */
    public void setOffsetEnd(Integer end) {
      objectOffsetEnd = end;
    }

    /**
     * Gets the offset end.
     *
     * @return the offset end
     */
    public Integer getOffsetEnd() {
      return objectOffsetEnd;
    }

    /**
     * Gets the offset.
     *
     * @return the offset
     */
    public Integer[] getOffset() {
      if (objectOffsetStart != null && objectOffsetStart != null) {
        Integer[] list = new Integer[] { objectOffsetStart, objectOffsetEnd };
        return list;
      } else {
        return null;
      }
    }

    /**
     * Adds the position.
     *
     * @param position the position
     */
    public void addPosition(Integer position) {
      objectPositions.add(position);
    }

    /**
     * Adds the positions.
     *
     * @param positions the positions
     */
    public void addPositions(TreeSet<Integer> positions) {
      objectPositions.addAll(positions);
    }

    /**
     * Gets the positions.
     *
     * @return the positions
     */
    public TreeSet<Integer> getPositions() {
      return objectPositions;
    }

    /**
     * Adds the ref id.
     *
     * @param id the id
     */
    public void addRefId(String id) {
      if (id != null) {
        refIds.add(id);
      }
    }

    /**
     * Gets the ref ids.
     *
     * @return the ref ids
     */
    public HashSet<String> getRefIds() {
      return refIds;
    }

  }

}
