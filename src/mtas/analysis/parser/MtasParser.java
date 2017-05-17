package mtas.analysis.parser;

import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
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

  /** The Constant TOKEN_OFFSET. */
  protected static final String TOKEN_OFFSET = "offset";

  /** The Constant TOKEN_REALOFFSET. */
  protected static final String TOKEN_REALOFFSET = "realoffset";

  /** The Constant TOKEN_PARENT. */
  protected static final String TOKEN_PARENT = "parent";

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
    private SortedSet<Integer> objectPositions = new TreeSet<>();

    /** The ref ids. */
    private Set<String> refIds = new HashSet<>();

    /** The updateable mappings as parent. */
    private Set<Integer> updateableMappingsAsParent = new HashSet<>();

    /** The updateable ids with position. */
    private Set<String> updateableIdsWithPosition = new HashSet<>();

    /** The updateable mappings with position. */
    protected Set<Integer> updateableMappingsWithPosition = new HashSet<>();

    /** The updateable ids with offset. */
    private Set<String> updateableIdsWithOffset = new HashSet<>();

    /** The updateable mappings with offset. */
    protected Set<Integer> updateableMappingsWithOffset = new HashSet<>();

    /** The referred start position. */
    protected Map<String, Integer> referredStartPosition = new HashMap<>();

    /** The referred end position. */
    protected Map<String, Integer> referredEndPosition = new HashMap<>();

    /** The referred start offset. */
    protected Map<String, Integer> referredStartOffset = new HashMap<>();

    /** The referred end offset. */
    protected Map<String, Integer> referredEndOffset = new HashMap<>();

    /**
     * Instantiates a new mtas parser object.
     *
     * @param type the type
     */
    MtasParserObject(MtasParserType type) {
      objectType = type;
      objectAttributes = new HashMap<>();
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
    public void registerUpdateableMappingsAtParent(Set<Integer> mappingIds) {
      updateableMappingsAsParent.addAll(mappingIds);
    }

    /**
     * Gets the updateable mappings as parent.
     *
     * @return the updateable mappings as parent
     */
    public Set<Integer> getUpdateableMappingsAsParent() {
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
    public void updateMappings(Map<String, Set<Integer>> idPositions,
        Map<String, Integer[]> idOffsets) {
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
          Integer[] currentOffset = idOffsets.get(id);
          if (currentOffset == null || currentOffset.length==0) {
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
      if (objectOffsetStart != null) {
        return new Integer[] { objectOffsetStart, objectOffsetEnd };
      } else {
        return new Integer[0];
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
    public void addPositions(Set<Integer> positions) {
      objectPositions.addAll(positions);
    }

    /**
     * Gets the positions.
     *
     * @return the positions
     */
    public SortedSet<Integer> getPositions() {
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
    public Set<String> getRefIds() {
      return refIds;
    }

    /**
     * Sets the referred start position.
     *
     * @param id the id
     * @param position the position
     */
    public void setReferredStartPosition(String id, Integer position) {
      referredStartPosition.put(id, position);
    }

    /**
     * Sets the referred end position.
     *
     * @param id the id
     * @param position the position
     */
    public void setReferredEndPosition(String id, Integer position) {
      referredEndPosition.put(id, position);
    }

    /**
     * Sets the referred start offset.
     *
     * @param id the id
     * @param offset the offset
     */
    public void setReferredStartOffset(String id, Integer offset) {
      referredStartOffset.put(id, offset);
    }

    /**
     * Sets the referred end offset.
     *
     * @param id the id
     * @param offset the offset
     */
    public void setReferredEndOffset(String id, Integer offset) {
      referredEndOffset.put(id, offset);
    }

    /**
     * Clear referred.
     */
    public void clearReferred() {
      referredStartPosition.clear();
      referredEndPosition.clear();
      referredStartOffset.clear();
      referredEndOffset.clear();
    }

  }

}
