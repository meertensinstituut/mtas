package mtas.codec.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import mtas.analysis.token.MtasToken;
import mtas.codec.MtasCodecPostingsFormat;
import mtas.codec.tree.IntervalRBTree;
import mtas.codec.tree.IntervalTreeNodeData;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;

/**
 * The Class CodecInfo.
 */
public class CodecInfo {

  /** The index input list. */
  HashMap<String, IndexInput> indexInputList;

  /** The index input offset list. */
  HashMap<String, Long> indexInputOffsetList;

  /** The version. */
  int version;

  /** The field references. */
  private HashMap<String, FieldReferences> fieldReferences;

  /** The prefix references. */
  private HashMap<String, LinkedHashMap<String, Long>> prefixReferences;

  /**
   * Instantiates a new codec info.
   *
   * @param indexInputList
   *          the index input list
   * @param indexInputOffsetList
   *          the index input offset list
   * @param version
   *          the version
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public CodecInfo(HashMap<String, IndexInput> indexInputList,
      HashMap<String, Long> indexInputOffsetList, int version)
      throws IOException {
    this.indexInputList = indexInputList;
    this.indexInputOffsetList = indexInputOffsetList;
    this.version = version;
    init();
  }

  /**
   * Gets the codec info from terms.
   *
   * @param t
   *          the t
   * @return the codec info from terms
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unchecked")
  public static CodecInfo getCodecInfoFromTerms(Terms t) throws IOException {
    try {
      HashMap<String, IndexInput> indexInputList = null;
      HashMap<String, Long> indexInputOffsetList = null;
      Integer version = null;
      Method[] methods = t.getClass().getMethods();
      for (Method m : methods) {
        if (m.getName().equals("getIndexInputList")) {
          indexInputList = (HashMap<String, IndexInput>) m.invoke(t,
              (Object[]) null);
        } else if (m.getName().equals("getIndexInputOffsetList")) {
          indexInputOffsetList = (HashMap<String, Long>) m.invoke(t,
              (Object[]) null);
        } else if (m.getName().equals("getVersion")) {
          version = (int) m.invoke(t, (Object[]) null);
        }
      }
      if (indexInputList == null || indexInputOffsetList == null
          || version == null) {
        throw new IOException("Reader doesn't provide MtasFieldsProducer");
      } else {
        return new CodecInfo(indexInputList, indexInputOffsetList, version);
      }
    } catch (Exception e) {
      throw new IOException("Can't get codecInfo");
    }
  }

  /**
   * Inits the.
   *
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void init() throws IOException {
    // move to begin
    IndexInput inField = indexInputList.get("field");
    inField.seek(indexInputOffsetList.get("field"));
    // store field references in memory
    fieldReferences = new HashMap<String, FieldReferences>();
    while (true) {
      try {
        String field = inField.readString();
        long refIndexDoc = inField.readVLong();
        long refIndexDocId = inField.readVLong();
        int numberOfDocs = inField.readVInt();
        long refTerm = inField.readVLong();
        int numberOfTerms = inField.readVInt();
        long refPrefix = inField.readVLong();
        int numberOfPrefixes = inField.readVInt();
        fieldReferences.put(field,
            new FieldReferences(refIndexDoc, refIndexDocId, numberOfDocs,
                refTerm, numberOfTerms, refPrefix, numberOfPrefixes));
      } catch (Exception ex) {
        break;
      }
    }
    // prefixReferences
    prefixReferences = new HashMap<String, LinkedHashMap<String, Long>>();
  }

  /**
   * Gets the object by id.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @param mtasId
   *          the mtas id
   * @return the object by id
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public MtasToken<?> getObjectById(String field, int docId, int mtasId)
      throws IOException {
    try {
      Long ref, objectRefApproxCorrection;
      IndexDoc doc = getDoc(field, docId);
      IndexInput inObjectId = indexInputList.get("indexObjectId");
      IndexInput inObject = indexInputList.get("object");
      IndexInput inTerm = indexInputList.get("term");
      if (doc.storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_BYTE) {
        inObjectId.seek((doc.fpIndexObjectId + (mtasId * 1)));
        objectRefApproxCorrection = Long.valueOf(inObjectId.readByte());
      } else if (doc.storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_SHORT) {
        inObjectId.seek((doc.fpIndexObjectId + (mtasId * 2)));
        objectRefApproxCorrection = Long.valueOf(inObjectId.readShort());
      } else if (doc.storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_INTEGER) {
        inObjectId.seek((doc.fpIndexObjectId + (mtasId * 4)));
        objectRefApproxCorrection = Long.valueOf(inObjectId.readInt());
      } else {
        inObjectId.seek((doc.fpIndexObjectId + (mtasId * 8)));
        objectRefApproxCorrection = Long.valueOf(inObjectId.readLong());
      }
      ref = objectRefApproxCorrection + doc.objectRefApproxOffset
          + (mtasId * doc.objectRefApproxQuotient);
      return MtasCodecPostingsFormat.getToken(inObject, inTerm, ref);
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Gets the objects by parent id.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @param position
   *          the position
   * @return the objects by parent id
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public List<MtasToken<String>> getObjectsByParentId(String field, int docId,
      int position) throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectParent = indexInputList.get("indexObjectParent");
    ArrayList<MtasTreeHit<?>> hits = CodecSearchTree.searchMtasTree(position,
        inIndexObjectParent, doc.fpIndexObjectParent,
        doc.smallestObjectFilepointer);
    return getObjects(hits);
  }

  /**
   * Gets the objects by position.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @param position
   *          the position
   * @return the objects by position
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public ArrayList<MtasToken<String>> getObjectsByPosition(String field,
      int docId, int position) throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectPosition = indexInputList
        .get("indexObjectPosition");
    ArrayList<MtasTreeHit<?>> hits = CodecSearchTree.searchMtasTree(position,
        inIndexObjectPosition, doc.fpIndexObjectPosition,
        doc.smallestObjectFilepointer);
    return getObjects(hits);
  }

  // public ArrayList<MtasToken<String>> getObjectsByPositions(String field,
  // int docId, int startPosition, int endPosition) throws IOException {
  // IndexDoc doc = getDoc(field, docId);
  // IndexInput inIndexObjectPosition = indexInputList
  // .get("indexObjectPosition");
  // ArrayList<TreeHit<?>> hits = CodecSearchTree.searchTree(startPosition,
  // endPosition, inIndexObjectPosition, doc.fpIndexObjectPosition,
  // doc.smallestObjectFilepointer);
  // return getObjects(hits);
  // }

  /**
   * Gets the prefix filtered objects by positions.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @param prefixes
   *          the prefixes
   * @param startPosition
   *          the start position
   * @param endPosition
   *          the end position
   * @return the prefix filtered objects by positions
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public ArrayList<MtasToken<String>> getPrefixFilteredObjectsByPositions(
      String field, int docId, ArrayList<String> prefixes, int startPosition,
      int endPosition) throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectPosition = indexInputList
        .get("indexObjectPosition");
    ArrayList<MtasTreeHit<?>> hits = CodecSearchTree.searchMtasTree(
        startPosition, endPosition, inIndexObjectPosition,
        doc.fpIndexObjectPosition, doc.smallestObjectFilepointer);
    return getPrefixFilteredObjects(hits, prefixes);
  }

  /**
   * Gets the prefix filtered objects.
   *
   * @param hits
   *          the hits
   * @param prefixes
   *          the prefixes
   * @return the prefix filtered objects
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private ArrayList<MtasToken<String>> getPrefixFilteredObjects(
      List<MtasTreeHit<?>> hits, ArrayList<String> prefixes)
      throws IOException {
    ArrayList<MtasToken<String>> tokens = new ArrayList<MtasToken<String>>();
    IndexInput inObject = indexInputList.get("object");
    IndexInput inTerm = indexInputList.get("term");
    for (MtasTreeHit<?> hit : hits) {
      MtasToken<String> token = MtasCodecPostingsFormat.getToken(inObject,
          inTerm, hit.ref); 
      if (token != null) {
        if (prefixes.size() > 0) {
          if (prefixes.contains(token.getPrefix())) {
            tokens.add(token);
          }
        } else {
          tokens.add(token);
        }
      }
    }
    return tokens;
  }

  /**
   * Gets the positioned terms by prefixes and position.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @param prefixes
   *          the prefixes
   * @param position
   *          the position
   * @return the positioned terms by prefixes and position
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public ArrayList<MtasTreeHit<String>> getPositionedTermsByPrefixesAndPosition(
      String field, int docId, ArrayList<String> prefixes, int position)
      throws IOException {
    return getPositionedTermsByPrefixesAndPositionRange(field, docId, prefixes,
        position, position);
  }

  /**
   * Gets the positioned terms by prefixes and position range.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @param prefixes
   *          the prefixes
   * @param startPosition
   *          the start position
   * @param endPosition
   *          the end position
   * @return the positioned terms by prefixes and position range
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public ArrayList<MtasTreeHit<String>> getPositionedTermsByPrefixesAndPositionRange(
      String field, int docId, ArrayList<String> prefixes, int startPosition,
      int endPosition) throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectPosition = indexInputList
        .get("indexObjectPosition");
    ArrayList<MtasTreeHit<?>> hitItems = CodecSearchTree.searchMtasTree(
        startPosition, endPosition, inIndexObjectPosition,
        doc.fpIndexObjectPosition, doc.smallestObjectFilepointer);
    ArrayList<MtasTreeHit<String>> hits = new ArrayList<MtasTreeHit<String>>();
    if (version == MtasCodecPostingsFormat.VERSION_OLD_1) {
      // old way
      ArrayList<MtasToken<String>> objects = getObjects(hitItems);
      for (MtasToken<String> token : objects) {
        if (prefixes.contains(token.getPrefix())) {
          MtasTreeHit<String> hit = new MtasTreeHit<String>(
              token.getPositionStart(), token.getPositionEnd(),
              token.getTokenRef(), 0, token.getValue());
          hits.add(hit);
        }
      }
    } else {
      HashMap<String, Integer> prefixIds = getPrefixesIds(field, prefixes);
      if (prefixIds != null && prefixIds.size() > 0) {
        ArrayList<MtasTreeHit<?>> filteredHitItems = new ArrayList<MtasTreeHit<?>>();
        for (MtasTreeHit<?> hitItem : hitItems) {
          if (prefixIds.containsValue(hitItem.additionalId)) {
            filteredHitItems.add(hitItem);
          }
        }
        if (filteredHitItems.size() > 0) {
          ArrayList<MtasToken<String>> objects = getObjects(filteredHitItems);
          for (MtasToken<String> token : objects) {
            MtasTreeHit<String> hit = new MtasTreeHit<String>(
                token.getPositionStart(), token.getPositionEnd(),
                token.getTokenRef(), 0, token.getValue());
            hits.add(hit);
          }
        }
      }
    }
    return hits;
  }

  /**
   * Collect terms by prefixes for list of hit positions.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @param prefixes
   *          the prefixes
   * @param positionsHits
   *          the positions hits
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void collectTermsByPrefixesForListOfHitPositions(String field,
      int docId, ArrayList<String> prefixes,
      ArrayList<IntervalTreeNodeData<String>> positionsHits)
      throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectPosition = indexInputList
        .get("indexObjectPosition");
    IndexInput inObject = indexInputList.get("object");
    IndexInput inTerm = indexInputList.get("term");
    // create tree interval hits
    IntervalRBTree<String> positionTree = new IntervalRBTree<String>(
        positionsHits);
    if (version == MtasCodecPostingsFormat.VERSION_OLD_1) {
      CodecSearchTree.searchMtasTreeWithIntervalTree(null, positionTree,
          inIndexObjectPosition, doc.fpIndexObjectPosition,
          doc.smallestObjectFilepointer);
    } else {
      // find prefixIds
      HashMap<String, Integer> prefixIds = getPrefixesIds(field, prefixes);
      // search matching tokens
      CodecSearchTree.searchMtasTreeWithIntervalTree(prefixIds.values(),
          positionTree, inIndexObjectPosition, doc.fpIndexObjectPosition,
          doc.smallestObjectFilepointer);
    }
    for (IntervalTreeNodeData<String> positionHit : positionsHits) {
      for (MtasTreeHit<String> hit : positionHit.list) {
        if (hit.data == null) {
          MtasToken<String> token = MtasCodecPostingsFormat.getToken(inObject,
              inTerm, hit.ref);
          hit.data = token.getValue();
        }
      }
    }

  }

  /**
   * Gets the objects.
   *
   * @param hits
   *          the hits
   * @return the objects
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public ArrayList<MtasToken<String>> getObjects(List<MtasTreeHit<?>> hits)
      throws IOException {
    ArrayList<MtasToken<String>> tokens = new ArrayList<MtasToken<String>>();
    IndexInput inObject = indexInputList.get("object");
    IndexInput inTerm = indexInputList.get("term");
    for (MtasTreeHit<?> hit : hits) {
      MtasToken<String> token = MtasCodecPostingsFormat.getToken(inObject,
          inTerm, hit.ref);
      if (token != null) {
        tokens.add(token);
      }
    }
    return tokens;
  }

  /**
   * Gets the terms.
   *
   * @param refs
   *          the refs
   * @return the terms
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public ArrayList<MtasTreeHit<String>> getTerms(ArrayList<MtasTreeHit<?>> refs)
      throws IOException {
    try {
      ArrayList<MtasTreeHit<String>> terms = new ArrayList<MtasTreeHit<String>>();
      IndexInput inTerm = indexInputList.get("term");
      for (MtasTreeHit<?> hit : refs) {
        inTerm.seek(hit.ref);
        String term = inTerm.readString();
        MtasTreeHit<String> newHit = new MtasTreeHit<String>(hit.startPosition,
            hit.endPosition, hit.ref, hit.additionalId, term);
        terms.add(newHit);
      }
      return terms;
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Gets the prefixes ids.
   *
   * @param field
   *          the field
   * @param prefixes
   *          the prefixes
   * @return the prefixes ids
   */
  HashMap<String, Integer> getPrefixesIds(String field,
      ArrayList<String> prefixes) {
    LinkedHashMap<String, Long> refs = getPrefixes(field);
    if (refs != null) {
      ArrayList<String> list = new ArrayList<String>(refs.keySet());
      HashMap<String, Integer> result = new HashMap<String, Integer>();
      for (String prefix : prefixes) {
        int id = list.indexOf(prefix);
        if (id >= 0) {
          result.put(prefix, id + 1);
        }
      }
      return result;
    } else {
      return null;
    }
  }

  /**
   * Gets the prefixes.
   *
   * @param field
   *          the field
   * @return the prefixes
   */
  private LinkedHashMap<String, Long> getPrefixes(String field) {
    if (fieldReferences.containsKey(field)) {
      FieldReferences fr = fieldReferences.get(field);
      if (!prefixReferences.containsKey(field)) {
        LinkedHashMap<String, Long> refs = new LinkedHashMap<String, Long>();
        try {
          IndexInput inPrefix = indexInputList.get("prefix");
          inPrefix.seek(fr.refPrefix);
          for (int i = 0; i < fr.numberOfPrefixes; i++) {
            Long ref = inPrefix.getFilePointer();
            refs.put(inPrefix.readString(), ref);
          }
        } catch (Exception e) {
          refs.clear();
        }
        prefixReferences.put(field, refs);
        return refs;
      } else {
        return prefixReferences.get(field);
      }
    } else {
      return null;
    }
  }

  /**
   * Gets the doc.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @return the doc
   */
  public IndexDoc getDoc(String field, int docId) {
    if (fieldReferences.containsKey(field)) {
      FieldReferences fr = fieldReferences.get(field);
      try {
        IndexInput inIndexDocId = indexInputList.get("indexDocId");
        ArrayList<MtasTreeHit<?>> list = CodecSearchTree.searchMtasTree(docId,
            inIndexDocId, fr.refIndexDocId, fr.refIndexDoc);
        if (list.size() == 1) {
          return new IndexDoc(list.get(0).ref);
        }
      } catch (IOException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Gets the next doc.
   *
   * @param field
   *          the field
   * @param previousDocId
   *          the previous doc id
   * @return the next doc
   */
  public IndexDoc getNextDoc(String field, int previousDocId) {
    if (fieldReferences.containsKey(field)) {
      FieldReferences fr = fieldReferences.get(field);
      try {
        if (previousDocId < 0) {
          return new IndexDoc(fr.refIndexDoc);
        } else {
          int nextDocId = previousDocId + 1;
          IndexInput inIndexDocId = indexInputList.get("indexDocId");
          ArrayList<MtasTreeHit<?>> list = CodecSearchTree.advanceMtasTree(
              nextDocId, inIndexDocId, fr.refIndexDocId, fr.refIndexDoc);
          if (list.size() == 1) {
            IndexInput inDoc = indexInputList.get("doc");
            inDoc.seek(list.get(0).ref);
            return new IndexDoc(inDoc.getFilePointer());
          }
        }
      } catch (IOException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Gets the number of positions.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @return the number of positions
   */
  public Integer getNumberOfPositions(String field, int docId) {
    if (fieldReferences.containsKey(field)) {
      IndexDoc doc = getDoc(field, docId);
      if (doc != null) {
        return 1 + doc.maxPosition - doc.minPosition;
      }
    }
    return null;
  }

  /**
   * Gets the all number of positions.
   *
   * @param field
   *          the field
   * @param docBase
   *          the doc base
   * @return the all number of positions
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public HashMap<Integer, Integer> getAllNumberOfPositions(String field,
      int docBase) throws IOException {
    HashMap<Integer, Integer> numbers = new HashMap<Integer, Integer>();
    if (fieldReferences.containsKey(field)) {
      FieldReferences fr = fieldReferences.get(field);
      IndexInput inIndexDoc = indexInputList.get("doc");
      inIndexDoc.seek(fr.refIndexDoc);
      IndexDoc doc;
      for (int i = 0; i < fr.numberOfDocs; i++) {
        doc = new IndexDoc(null);
        numbers.put((doc.docId + docBase),
            (1 + doc.maxPosition - doc.minPosition));
      }
    }
    return numbers;
  }

  /**
   * Gets the number of tokens.
   *
   * @param field
   *          the field
   * @param docId
   *          the doc id
   * @return the number of tokens
   */
  public Integer getNumberOfTokens(String field, int docId) {
    if (fieldReferences.containsKey(field)) {
      IndexDoc doc = getDoc(field, docId);
      if (doc != null) {
        return doc.size;
      }
    }
    return null;
  }

  /**
   * Gets the all number of tokens.
   *
   * @param field
   *          the field
   * @param docBase
   *          the doc base
   * @return the all number of tokens
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public HashMap<Integer, Integer> getAllNumberOfTokens(String field,
      int docBase) throws IOException {
    HashMap<Integer, Integer> numbers = new HashMap<Integer, Integer>();
    if (fieldReferences.containsKey(field)) {
      FieldReferences fr = fieldReferences.get(field);
      IndexInput inIndexDoc = indexInputList.get("doc");
      inIndexDoc.seek(fr.refIndexDoc);
      IndexDoc doc;
      for (int i = 0; i < fr.numberOfDocs; i++) {
        doc = new IndexDoc(null);
        numbers.put((doc.docId + docBase), doc.size);
      }
    }
    return numbers;
  }

  /**
   * The Class IndexDoc.
   */
  public class IndexDoc {

    /** The doc id. */
    public int docId;

    /** The fp index object parent. */
    public long fpIndexObjectId, fpIndexObjectPosition, fpIndexObjectParent;

    /** The object ref approx offset. */
    public long smallestObjectFilepointer, objectRefApproxOffset;

    /** The object ref approx quotient. */
    public int objectRefApproxQuotient;

    /** The offset. */
    public long offset;

    /** The storage flags. */
    public byte storageFlags;

    /** The max position. */
    public int size, minPosition, maxPosition;

    /**
     * Instantiates a new index doc.
     *
     * @param ref
     *          the ref
     * @throws IOException
     *           Signals that an I/O exception has occurred.
     */
    public IndexDoc(Long ref) throws IOException {
      try {
        IndexInput inIndexDoc = indexInputList.get("doc");
        if (ref != null) {
          inIndexDoc.seek(ref);
        }
        docId = inIndexDoc.readVInt(); // docId
        fpIndexObjectId = inIndexDoc.readVLong(); // ref indexObjectId
        fpIndexObjectPosition = inIndexDoc.readVLong(); // ref
                                                        // indexObjectPosition
        fpIndexObjectParent = inIndexDoc.readVLong(); // ref indexObjectParent
        if (version == MtasCodecPostingsFormat.VERSION_OLD_1) {
          inIndexDoc.readVLong(); // fpIndexTermPrefixPosition ref
        }
        smallestObjectFilepointer = inIndexDoc.readVLong(); // offset
        objectRefApproxQuotient = inIndexDoc.readVInt(); // slope
        objectRefApproxOffset = inIndexDoc.readZLong(); // offset
        storageFlags = inIndexDoc.readByte(); // flag
        size = inIndexDoc.readVInt(); // number of objects
        minPosition = inIndexDoc.readVInt(); // minimum position
        maxPosition = inIndexDoc.readVInt(); // maximum position
      } catch (Exception e) {
        throw new IOException(e.getMessage());
      }
    }
  }

  /**
   * The Class FieldReferences.
   */
  public class FieldReferences {

    /** The ref prefix. */
    public long refIndexDoc, refIndexDocId, refTerm, refPrefix;

    /** The number of prefixes. */
    public int numberOfDocs, numberOfTerms, numberOfPrefixes;

    /**
     * Instantiates a new field references.
     *
     * @param refIndexDoc
     *          the ref index doc
     * @param refIndexDocId
     *          the ref index doc id
     * @param numberOfDocs
     *          the number of docs
     * @param refTerm
     *          the ref term
     * @param numberOfTerms
     *          the number of terms
     * @param refPrefix
     *          the ref prefix
     * @param numberOfPrefixes
     *          the number of prefixes
     */
    public FieldReferences(long refIndexDoc, long refIndexDocId,
        int numberOfDocs, long refTerm, int numberOfTerms, long refPrefix,
        int numberOfPrefixes) {
      this.refIndexDoc = refIndexDoc;
      this.refIndexDocId = refIndexDocId;
      this.numberOfDocs = numberOfDocs;
      this.refTerm = refTerm;
      this.numberOfTerms = numberOfTerms;
      this.refPrefix = refPrefix;
      this.numberOfPrefixes = numberOfPrefixes;
    }
  }

}
