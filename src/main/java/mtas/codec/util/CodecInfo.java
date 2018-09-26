package mtas.codec.util;

import mtas.analysis.token.MtasToken;
import mtas.analysis.token.MtasTokenString;
import mtas.codec.MtasCodecPostingsFormat;
import mtas.codec.tree.IntervalRBTree;
import mtas.codec.tree.IntervalTreeNodeData;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CodecInfo {
  private static Log log = LogFactory.getLog(CodecInfo.class);

  HashMap<String, IndexInput> indexInputList;
  HashMap<String, Long> indexInputOffsetList;
  int version;

  private HashMap<String, FieldReferences> fieldReferences;
  private HashMap<String, LinkedHashMap<String, Long>> prefixReferences;

  public CodecInfo(HashMap<String, IndexInput> indexInputList,
      HashMap<String, Long> indexInputOffsetList, int version)
      throws IOException {
    this.indexInputList = indexInputList;
    this.indexInputOffsetList = indexInputOffsetList;
    this.version = version;
    init();
  }

  @SuppressWarnings("unchecked")
  public static CodecInfo getCodecInfoFromTerms(Terms t) throws IOException {
    try {
      HashMap<String, IndexInput> indexInputList = null;
      HashMap<String, Long> indexInputOffsetList = null;
      Object version = null;
      Method[] methods = t.getClass().getMethods();
      Object[] emptyArgs = null;
      for (Method m : methods) {
        if (m.getName().equals("getIndexInputList")) {
          indexInputList = (HashMap<String, IndexInput>) m.invoke(t, emptyArgs);
        } else if (m.getName().equals("getIndexInputOffsetList")) {
          indexInputOffsetList = (HashMap<String, Long>) m.invoke(t, emptyArgs);
        } else if (m.getName().equals("getVersion")) {
          version = m.invoke(t, emptyArgs);
        }
      }
      if (indexInputList == null || indexInputOffsetList == null
          || version == null) {
        throw new IOException("Reader doesn't provide MtasFieldsProducer");
      } else {
        return new CodecInfo(indexInputList, indexInputOffsetList,
            (int) version);
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IOException("Can't get codecInfo", e);
    }
  }

  private void init() throws IOException {
    // move to begin
    IndexInput inField = indexInputList.get("field");
    inField.seek(indexInputOffsetList.get("field"));
    // store field references in memory
    fieldReferences = new HashMap<String, FieldReferences>();
    boolean doInit = true;
    while (doInit) {
      try {
        String field = inField.readString();
        long refIndexDoc = inField.readVLong();
        long refIndexDocId = inField.readVLong();
        int numberOfDocs = inField.readVInt();
        inField.readVLong(); // refTerm
        inField.readVInt(); // numberOfTerms
        long refPrefix = inField.readVLong();
        int numberOfPrefixes = inField.readVInt();
        fieldReferences.put(field, new FieldReferences(refIndexDoc,
            refIndexDocId, numberOfDocs, refPrefix, numberOfPrefixes));
      } catch (IOException e) {
        log.debug(e);
        doInit = false;
      }
    }
    // prefixReferences
    prefixReferences = new HashMap<String, LinkedHashMap<String, Long>>();
  }

  public MtasToken getObjectById(String field, int docId, int mtasId)
      throws IOException {
    try {
      Long ref;
      Long objectRefApproxCorrection;
      IndexDoc doc = getDoc(field, docId);
      IndexInput inObjectId = indexInputList.get("indexObjectId");
      IndexInput inObject = indexInputList.get("object");
      IndexInput inTerm = indexInputList.get("term");
      if (doc.storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_BYTE) {
        inObjectId.seek(doc.fpIndexObjectId + (mtasId * 1L));
        objectRefApproxCorrection = Long.valueOf(inObjectId.readByte());
      } else if (doc.storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_SHORT) {
        inObjectId.seek(doc.fpIndexObjectId + (mtasId * 2L));
        objectRefApproxCorrection = Long.valueOf(inObjectId.readShort());
      } else if (doc.storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_INTEGER) {
        inObjectId.seek(doc.fpIndexObjectId + (mtasId * 4L));
        objectRefApproxCorrection = Long.valueOf(inObjectId.readInt());
      } else {
        inObjectId.seek(doc.fpIndexObjectId + (mtasId * 8L));
        objectRefApproxCorrection = Long.valueOf(inObjectId.readLong());
      }
      ref = objectRefApproxCorrection + doc.objectRefApproxOffset
          + (mtasId * (long) doc.objectRefApproxQuotient);
      return MtasCodecPostingsFormat.getToken(inObject, inTerm, ref);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public List<MtasTokenString> getObjectsByParentId(String field, int docId,
      int position) throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectParent = indexInputList.get("indexObjectParent");
    ArrayList<MtasTreeHit<?>> hits = CodecSearchTree.searchMtasTree(position,
        inIndexObjectParent, doc.fpIndexObjectParent,
        doc.smallestObjectFilepointer);
    return getObjects(hits);
  }

  public ArrayList<MtasTokenString> getObjectsByPosition(String field,
      int docId, int position) throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectPosition = indexInputList
        .get("indexObjectPosition");
    ArrayList<MtasTreeHit<?>> hits = CodecSearchTree.searchMtasTree(position,
        inIndexObjectPosition, doc.fpIndexObjectPosition,
        doc.smallestObjectFilepointer);
    return getObjects(hits);
  }

  public ArrayList<MtasTokenString> getObjectsByPositions(String field,
      int docId, int startPosition, int endPosition) throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectPosition = indexInputList
        .get("indexObjectPosition");
    ArrayList<MtasTreeHit<?>> hits = CodecSearchTree.searchMtasTree(
        startPosition, endPosition, inIndexObjectPosition,
        doc.fpIndexObjectPosition, doc.smallestObjectFilepointer);
    return getObjects(hits);
  }

  public List<MtasTokenString> getPrefixFilteredObjectsByPositions(String field,
      int docId, List<String> prefixes, int startPosition, int endPosition)
      throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectPosition = indexInputList
        .get("indexObjectPosition");
    if (doc != null) {
      ArrayList<MtasTreeHit<?>> hits = CodecSearchTree.searchMtasTree(
          startPosition, endPosition, inIndexObjectPosition,
          doc.fpIndexObjectPosition, doc.smallestObjectFilepointer);
      return getPrefixFilteredObjects(hits, prefixes);
    } else {
      return new ArrayList<>();
    }
  }

  private List<MtasTokenString> getPrefixFilteredObjects(
      List<MtasTreeHit<?>> hits, List<String> prefixes) throws IOException {
    ArrayList<MtasTokenString> tokens = new ArrayList<>();
    IndexInput inObject = indexInputList.get("object");
    IndexInput inTerm = indexInputList.get("term");
    for (MtasTreeHit<?> hit : hits) {
      MtasTokenString token = MtasCodecPostingsFormat.getToken(inObject, inTerm,
          hit.ref);
      if (token != null) {
        if (prefixes != null && !prefixes.isEmpty()) {
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

  public List<MtasTreeHit<String>> getPositionedTermsByPrefixesAndPosition(
      String field, int docId, List<String> prefixes, int position)
      throws IOException {
    return getPositionedTermsByPrefixesAndPositionRange(field, docId, prefixes,
        position, position);
  }

  public List<MtasTreeHit<String>> getPositionedTermsByPrefixesAndPositionRange(
      String field, int docId, List<String> prefixes, int startPosition,
      int endPosition) throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectPosition = indexInputList
        .get("indexObjectPosition");
    if (doc != null) {
      ArrayList<MtasTreeHit<?>> hitItems = CodecSearchTree.searchMtasTree(
          startPosition, endPosition, inIndexObjectPosition,
          doc.fpIndexObjectPosition, doc.smallestObjectFilepointer);
      List<MtasTreeHit<String>> hits = new ArrayList<>();
      Map<String, Integer> prefixIds = getPrefixesIds(field, prefixes);
      if (prefixIds != null && prefixIds.size() > 0) {
        ArrayList<MtasTreeHit<?>> filteredHitItems = new ArrayList<MtasTreeHit<?>>();

        for (MtasTreeHit<?> hitItem : hitItems) {
          if (prefixIds.containsValue(hitItem.additionalId)) {
            filteredHitItems.add(hitItem);
          }
        }
        if (filteredHitItems.size() > 0) {
          ArrayList<MtasTokenString> objects = getObjects(filteredHitItems);
          for (MtasTokenString token : objects) {
            MtasTreeHit<String> hit = new MtasTreeHit<String>(
                token.getPositionStart(), token.getPositionEnd(),
                token.getTokenRef(), 0, 0, token.getValue());
            hits.add(hit);
          }
        }
      }
      return hits;
    } else {
      return new ArrayList<MtasTreeHit<String>>();
    }
  }

  public void collectTermsByPrefixesForListOfHitPositions(String field,
      int docId, ArrayList<String> prefixes,
      ArrayList<IntervalTreeNodeData<String>> positionsHits)
      throws IOException {
    IndexDoc doc = getDoc(field, docId);
    IndexInput inIndexObjectPosition = indexInputList
        .get("indexObjectPosition");
    IndexInput inTerm = indexInputList.get("term");
    // create tree interval hits
    IntervalRBTree<String> positionTree = new IntervalRBTree<String>(
        positionsHits);

    // find prefixIds
    Map<String, Integer> prefixIds = getPrefixesIds(field, prefixes);
    // search matching tokens
    if (prefixIds != null) {
      CodecSearchTree.searchMtasTreeWithIntervalTree(prefixIds.values(),
          positionTree, inIndexObjectPosition, doc.fpIndexObjectPosition,
          doc.smallestObjectFilepointer);

      // reverse list
      Map<Integer, String> idPrefixes = new HashMap<>();
      for (Entry<String, Integer> entry : prefixIds.entrySet()) {
        idPrefixes.put(entry.getValue(), entry.getKey());
      }
      // term administration
      Map<Long, String> refTerms = new HashMap<>();

      for (IntervalTreeNodeData<String> positionHit : positionsHits) {
        for (MtasTreeHit<String> hit : positionHit.list) {
          if (hit.idData == null) {
            hit.idData = idPrefixes.get(hit.additionalId);
            if (!refTerms.containsKey(hit.additionalRef)) {
              refTerms.put(hit.additionalRef,
                  MtasCodecPostingsFormat.getTerm(inTerm, hit.additionalRef));
            }
            hit.refData = refTerms.get(hit.additionalRef);
          }
        }
      }
    }
  }

  public ArrayList<MtasTokenString> getObjects(List<MtasTreeHit<?>> hits)
      throws IOException {
    ArrayList<MtasTokenString> tokens = new ArrayList<>();
    IndexInput inObject = indexInputList.get("object");
    IndexInput inTerm = indexInputList.get("term");
    for (MtasTreeHit<?> hit : hits) {
      MtasTokenString token = MtasCodecPostingsFormat.getToken(inObject, inTerm,
          hit.ref);
      if (token != null) {
        tokens.add(token);
      }
    }
    return tokens;
  }

  public ArrayList<MtasTreeHit<String>> getTerms(ArrayList<MtasTreeHit<?>> refs)
      throws IOException {
    try {
      ArrayList<MtasTreeHit<String>> terms = new ArrayList<MtasTreeHit<String>>();
      IndexInput inTerm = indexInputList.get("term");
      for (MtasTreeHit<?> hit : refs) {
        inTerm.seek(hit.ref);
        String term = inTerm.readString();
        MtasTreeHit<String> newHit = new MtasTreeHit<String>(hit.startPosition,
            hit.endPosition, hit.ref, hit.additionalId, hit.additionalRef,
            term);
        terms.add(newHit);
      }
      return terms;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  Map<String, Integer> getPrefixesIds(String field, List<String> prefixes) {
    LinkedHashMap<String, Long> refs = getPrefixRefs(field);
    if (refs != null) {
      List<String> list = new ArrayList<>(refs.keySet());
      Map<String, Integer> result = new HashMap<>();
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

  public Set<String> getPrefixes(String field) {
    LinkedHashMap<String, Long> prefixRefs = this.getPrefixRefs(field);
    return prefixRefs.keySet();
  }
  
  private LinkedHashMap<String, Long> getPrefixRefs(String field) {
    if (fieldReferences.containsKey(field)) {
      FieldReferences fr = fieldReferences.get(field);
      if (!prefixReferences.containsKey(field)) {
        LinkedHashMap<String, Long> refs = new LinkedHashMap<String, Long>();
        try {
          IndexInput inPrefix = indexInputList.get("prefix");
          inPrefix.seek(fr.refPrefix);
          for (int i = 0; i < fr.numberOfPrefixes; i++) {
            Long ref = inPrefix.getFilePointer();
            String prefix = inPrefix.readString();
            refs.put(prefix, ref);
          }
        } catch (Exception e) {
          log.error(e);
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
        log.debug(e);
        return null;
      }
    }
    return null;
  }

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
        log.debug(e);
        return null;
      }
    }
    return null;
  }

  public int getNumberOfDocs(String field) {
    if (fieldReferences.containsKey(field)) {
      FieldReferences fr = fieldReferences.get(field);
      return fr.numberOfDocs;
    } else {
      return 0;
    }
  }

  public Integer getNumberOfPositions(String field, int docId) {
    if (fieldReferences.containsKey(field)) {
      IndexDoc doc = getDoc(field, docId);
      if (doc != null) {
        return 1 + doc.maxPosition - doc.minPosition;
      }
    }
    return null;
  }

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

  public Integer getNumberOfTokens(String field, int docId) {
    if (fieldReferences.containsKey(field)) {
      IndexDoc doc = getDoc(field, docId);
      if (doc != null) {
        return doc.size;
      }
    }
    return null;
  }

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

  public class IndexDoc {
    public int docId;
    public long fpIndexObjectId;
    public long fpIndexObjectPosition;
    public long fpIndexObjectParent;
    public long smallestObjectFilepointer;
    public long objectRefApproxOffset;
    public int objectRefApproxQuotient;
    public byte storageFlags;
    public int size;
    public int minPosition;
    public int maxPosition;

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
        smallestObjectFilepointer = inIndexDoc.readVLong(); // offset
        objectRefApproxQuotient = inIndexDoc.readVInt(); // slope
        objectRefApproxOffset = inIndexDoc.readZLong(); // offset
        storageFlags = inIndexDoc.readByte(); // flag
        size = inIndexDoc.readVInt(); // number of objects
        minPosition = inIndexDoc.readVInt(); // minimum position
        maxPosition = inIndexDoc.readVInt(); // maximum position
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  private static class FieldReferences {
    public long refIndexDoc;
    public long refIndexDocId;
    public long refPrefix;
    public int numberOfDocs;
    public int numberOfPrefixes;

    public FieldReferences(long refIndexDoc, long refIndexDocId,
        int numberOfDocs, long refPrefix, int numberOfPrefixes) {
      this.refIndexDoc = refIndexDoc;
      this.refIndexDocId = refIndexDocId;
      this.numberOfDocs = numberOfDocs;
      this.refPrefix = refPrefix;
      this.numberOfPrefixes = numberOfPrefixes;
    }
  }
}
