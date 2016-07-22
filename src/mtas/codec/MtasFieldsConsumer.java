package mtas.codec;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import mtas.analysis.token.MtasOffset;
import mtas.analysis.token.MtasPosition;
import mtas.analysis.token.MtasToken;
import mtas.codec.payload.MtasPayloadDecoder;
import mtas.codec.tree.MtasRBTree;
import mtas.codec.tree.MtasTree;
import mtas.codec.tree.MtasTreeNode;
import mtas.codec.tree.MtasTreeNodeId;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;

/**
 * The Class MtasFieldsConsumer.
 *
 * 
 * The Class MtasFieldsConsumer constructs several temporal and permanent files
 * to provide a forward index
 *
 * <ul>
 * <li><b>Temporary files</b><br>
 * <ul>
 * <li><b>Temporary file {@link #mtasTmpFieldFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_TMP_FIELD_EXTENSION} </b><br>
 * Contains for each field a reference to the list of documents. Structure of
 * content:
 * <ul>
 * <li><b>String</b>: field</li>
 * <li><b>VLong</b>: reference to {@link #mtasDocFileName}</li>
 * <li><b>VInt</b>: number of documents</li>
 * <li><b>VLong</b>: reference to {@link #mtasTermFileName}</li>
 * <li><b>VInt</b>: number of terms</li>
 * <li><b>VLong</b>: reference to {@link #mtasPrefixFileName}</li>
 * <li><b>VInt</b>: number of prefixes</li>
 * </ul>
 * </li>
 * <li><b>Temporary file {@link #mtasTmpObjectFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_TMP_OBJECT_EXTENSION}</b><br>
 * Contains for a specific field all objects constructed by
 * {@link createObjectAndRegisterPrefix}. For all fields, the objects are later
 * on copied to {@link #mtasObjectFileName} while statistics are collected.
 * Structure of content identical to {@link #mtasObjectFileName}.</li>
 * <li><b>Temporary file {@link #mtasTmpDocsFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_TMP_DOCS_EXTENSION}</b> <br>
 * Contains for a specific field for each doc multiple fragments. Each occurring
 * term results in a fragment. Structure of content:
 * <ul>
 * <li><b>VInt</b>: docId</li>
 * <li><b>VInt</b>: number of objects in this fragment</li>
 * <li><b>VLong</b>: offset references to {@link #mtasTmpObjectFileName}</li>
 * <li><b>VInt</b>,<b>VLong</b>: mtasId object, reference temporary object in
 * {@link #mtasTmpObjectFileName} minus offset</li>
 * <li><b>VInt</b>,<b>VLong</b>: ...</li>
 * </ul>
 * </li>
 * <li><b>Temporary file {@link #mtasTmpDocsChainedFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_TMP_DOCS_CHAINED_EXTENSION}
 * </b><br>
 * Contains for a specific field for each doc multiple chained fragments.
 * Structure of content:
 * <ul>
 * <li><b>VInt</b>: docId</li>
 * <li><b>VInt</b>: number of objects in this fragment</li>
 * <li><b>VLong</b>: offset references to {@link #mtasTmpObjectFileName}</li>
 * <li><b>VInt</b>,<b>VLong</b>: mtasId object, reference temporary object in
 * {@link #mtasTmpObjectFileName} minus offset</li>
 * <li><b>VInt</b>,<b>VLong</b>: ...</li>
 * <li><b>VLong</b>: reference to next fragment in
 * {@link #mtasTmpDocsChainedFileName}, self reference indicates end of chain
 * </ul>
 * </li>
 * <li><b>Temporary file {@link #mtasTmpDocFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_TMP_DOC_EXTENSION}</b><br>
 * For each document
 * <ul>
 * <li><b>VInt</b>: docId</li>
 * <li><b>VLong</b>: reference to {@link #mtasIndexObjectIdFileName}</li>
 * <li><b>VLong</b>: reference first object, used as offset for tree index
 * <li><b>VInt</b>: slope used in approximation reference objects index on id
 * </li>
 * <li><b>ZLong</b>: offset used in approximation reference objects index on id
 * </li>
 * <li><b>Byte</b>: flag indicating how corrections on the approximation
 * references objects for the index on id are stored:
 * {@link MtasCodecPostingsFormat#MTAS_STORAGE_BYTE},
 * {@link MtasCodecPostingsFormat#MTAS_STORAGE_SHORT},
 * {@link MtasCodecPostingsFormat#MTAS_STORAGE_INTEGER} or
 * {@link MtasCodecPostingsFormat#MTAS_STORAGE_LONG}</li>
 * <li><b>VInt</b>: number of objects in this document</li>
 * <li><b>VInt</b>: first position</li>
 * <li><b>VInt</b>: last position</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * <li><b>Final files</b><br>
 * <ul>
 * <li><b>File {@link #mtasIndexFieldFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_FIELD_EXTENSION}</b><br>
 * Contains for each field a reference to the list of documents and the
 * prefixes. Structure of content:
 * <ul>
 * <li><b>String</b>: field</li>
 * <li><b>VLong</b>: reference to {@link #mtasDocFileName}</li>
 * <li><b>VLong</b>: reference to {@link #mtasIndexDocIdFileName}</li>
 * <li><b>VInt</b>: number of documents</li>
 * <li><b>VLong</b>: reference to {@link #mtasTermFileName}</li>
 * <li><b>VInt</b>: number of terms</li>
 * <li><b>VLong</b>: reference to {@link #mtasPrefixFileName}</li>
 * <li><b>VInt</b>: number of prefixes</li>
 * </ul>
 * </li>
 * <li><b>File {@link #mtasTermFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_TERM_EXTENSION}</b><br>
 * For each field, all unique terms are stored here. Structure of content:
 * <ul>
 * <li><b>String</b>: term</li>
 * </ul>
 * </li>
 * <li><b>File {@link #mtasPrefixFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_PREFIX_EXTENSION}</b><br>
 * For each field, all unique prefixes are stored here. Structure of content:
 * <ul>
 * <li><b>String</b>: prefix</li>
 * </ul>
 * </li>
 * <li><b>File {@link #mtasObjectFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_OBJECT_EXTENSION}</b><br>
 * Contains all objects for all fields. Structure of content:
 * <ul>
 * <li><b>VInt</b>: mtasId</li>
 * <li><b>VInt</b>: objectFlags
 * <ul>
 * <li>{@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_PARENT}</li>
 * <li>{@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_POSITION_RANGE}</li>
 * <li>{@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_POSITION_SET}</li>
 * <li>{@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_OFFSET}</li>
 * <li>{@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_REALOFFSET}</li>
 * <li>{@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_PAYLOAD}</li>
 * </ul>
 * </li>
 * <li>Only if {@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_PARENT}<br>
 * <b>VInt</b>: parentId
 * <li>Only if
 * {@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_POSITION_RANGE}<br>
 * <b>VInt</b>,<b>VInt</b>: startPosition and (endPosition-startPosition)
 * <li>Only if {@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_POSITION_SET}<br>
 * <b>VInt</b>,<b>VInt</b>,<b>VInt</b>,...: number of positions, firstPosition,
 * (position-previousPosition),...
 * <li>Only if no {@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_POSITION_RANGE}
 * or {@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_POSITION_SET}<br>
 * <b>VInt</b>: position
 * <li>Only if {@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_OFFSET}<br>
 * <b>VInt</b>,<b>VInt</b>: startOffset, (endOffset-startOffset)
 * <li>Only if {@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_REALOFFSET}<br>
 * <b>VInt</b>,<b>VInt</b>: startRealOffset, (endRealOffset-startRealOffset)
 * <li>Only if {@link MtasCodecPostingsFormat#MTAS_OBJECT_HAS_PAYLOAD}<br>
 * <b>VInt</b>,<b>Bytes</b>: number of bytes, payload
 * <li><b>VLong</b>: reference to Term in {@link #mtasTermFileName}</li>
 * </ul>
 * </li>
 * <li><b>File {@link #mtasIndexDocIdFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_INDEX_DOC_ID_EXTENSION}
 * </b><br>
 * Contains for each field a tree structure {@link MtasTree} to search reference
 * to {@link #mtasDocFileName} by id. Structure of content for each node:
 * <ul>
 * <li><b>VLong</b>: offset references to {@link #mtasIndexDocIdFileName}, only
 * available in root node</li>
 * <li><b>Byte</b>: flag, should be zero for this tree, only available in root
 * node</li>
 * <li><b>VInt</b>: left</li>
 * <li><b>VInt</b>: right</li>
 * <li><b>VInt</b>: max</li>
 * <li><b>VLong</b>: left reference to {@link #mtasIndexDocIdFileName} minus the
 * offset stored in the root node</li>
 * <li><b>VLong</b>: right reference to {@link #mtasIndexDocIdFileName} minus
 * the offset stored in the root node</li>
 * <li><b>VInt</b>: number of objects on this node (always 1 for this tree)</li>
 * <li><b>VLong</b>: reference to {@link #mtasDocFileName} minus offset</li>
 * </ul>
 * </li>
 * <li><b>File {@link #mtasDocFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_DOC_EXTENSION}</b><br>
 * For each document
 * <ul>
 * <li><b>VInt</b>: docId</li>
 * <li><b>VLong</b>: reference to {@link #mtasIndexObjectIdFileName}</li>
 * <li><b>VLong</b>: reference to {@link #mtasIndexObjectPositionFileName}</li>
 * <li><b>VLong</b>: reference to {@link #mtasIndexObjectParentFileName}</li>
 * <li><b>VLong</b>: reference first object, used as offset for tree index
 * <li><b>VInt</b>: slope used in approximation reference objects index on id
 * </li>
 * <li><b>ZLong</b>: offset used in approximation reference objects index on id
 * </li>
 * <li><b>Byte</b>: flag indicating how corrections on the approximation
 * references objects for the index on id are stored:
 * {@link MtasCodecPostingsFormat#MTAS_STORAGE_BYTE},
 * {@link MtasCodecPostingsFormat#MTAS_STORAGE_SHORT},
 * {@link MtasCodecPostingsFormat#MTAS_STORAGE_INTEGER} or
 * {@link MtasCodecPostingsFormat#MTAS_STORAGE_LONG}</li>
 * <li><b>VInt</b>: number of objects</li>
 * <li><b>VInt</b>: first position</li>
 * <li><b>VInt</b>: last position</li>
 * </ul>
 * </li>
 * <li><b>File {@link #mtasIndexObjectIdFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_INDEX_OBJECT_ID_EXTENSION}
 * </b><br>
 * Provides for each mtasId the reference to {@link #mtasObjectFileName}. These
 * references are grouped by document, sorted by mtasId, and because the
 * mtasId's for each document will always start with 0 and are sequential
 * without gaps, a reference can be computed if the position of the first
 * reference for a document is known from {@link #mtasDocFileName}. The
 * reference is approximated by the reference to the first object plus the
 * mtasId times a slope. Only a correction to this approximation is stored.
 * Structure of content:
 * <ul>
 * <li><b>Byte</b>/<b>Short</b>/<b>Int</b>/<b>Long</b>: correction reference to
 * {@link #mtasObjectFileName}</li>
 * </ul>
 * </li>
 * <li><b>File {@link #mtasIndexObjectPositionFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_INDEX_OBJECT_POSITION_EXTENSION}
 * </b><br>
 * Contains for each document a tree structure {@link MtasTree} to search
 * objects by position. Structure of content for each node:
 * <ul>
 * <li><b>VLong</b>: offset references to
 * {@link #mtasIndexObjectPositionFileName}, only available in root node</li>
 * <li><b>Byte</b>: flag, should be zero for this tree, only available in root
 * node</li>
 * <li><b>VInt</b>: left</li>
 * <li><b>VInt</b>: right</li>
 * <li><b>VInt</b>: max</li>
 * <li><b>VLong</b>: left reference to {@link #mtasIndexObjectPositionFileName}
 * minus the offset stored in the root node</li>
 * <li><b>VLong</b>: right reference to {@link #mtasIndexObjectPositionFileName}
 * minus the offset stored in the root node</li>
 * <li><b>VInt</b>: number of objects on this node</li>
 * <li><b>VLong</b>,<b>VInt</b>,<b>VLong</b>: set of the first reference to
 * {@link #mtasObjectFileName} minus offset, the prefixId referring to the
 * position the prefix in {@link #mtasPrefixFileName} and the reference to
 * {@link #mtasTermFileName} minus offset</li>
 * <li><b>VLong</b>,<b>VInt</b>,<b>VLong</b>,...: for optional other sets of
 * reference to {@link #mtasObjectFileName}, position of the prefix in
 * {@link #mtasPrefixFileName} and the reference to {@link #mtasTermFileName};
 * for the first item the difference between this reference minus the previous
 * reference is stored</li>
 * </ul>
 * </li>
 * <li><b>File {@link #mtasIndexObjectParentFileName} with extension
 * {@value mtas.codec.MtasCodecPostingsFormat#MTAS_INDEX_OBJECT_PARENT_EXTENSION}
 * </b><br>
 * Contains for each document a tree structure {@link MtasTree} to search
 * objects by parent. Structure of content for each node:
 * <ul>
 * <li><b>VLong</b>: offset references to {@link #mtasIndexObjectParentFileName}
 * , only available in root node</li>
 * <li><b>Byte</b>: flag, for this tree equal to
 * {@link mtas.codec.tree.MtasTree#SINGLE_POSITION_TREE} indicating a tree with
 * exactly one point at each node, only available in root node</li>
 * <li><b>VInt</b>: left</li>
 * <li><b>VInt</b>: right</li>
 * <li><b>VInt</b>: max</li>
 * <li><b>VLong</b>: left reference to {@link #mtasIndexObjectParentFileName}
 * minus the offset stored in the root node</li>
 * <li><b>VLong</b>: right reference to {@link #mtasIndexObjectParentFileName}
 * minus the offset stored in the root node</li>
 * <li><b>VInt</b>: number of objects on this node</li>
 * <li><b>VLong</b>,<b>VInt</b>,<b>VLong</b>: set of the first reference to
 * {@link #mtasObjectFileName} minus offset, the prefixId referring to the
 * position the prefix in {@link #mtasPrefixFileName} and the reference to
 * {@link #mtasTermFileName} minus offset</li>
 * <li><b>VLong</b>,<b>VInt</b>,<b>VLong</b>,...: for optional other sets of
 * reference to {@link #mtasObjectFileName}, position of the prefix in
 * {@link #mtasPrefixFileName} and the reference to {@link #mtasTermFileName};
 * for the first item the difference between this reference minus the previous
 * reference is stored</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * </ul>
 * 
 */

public class MtasFieldsConsumer extends FieldsConsumer {

  /** The delegate fields consumer. */
  private FieldsConsumer delegateFieldsConsumer;

  /** The state. */
  private SegmentWriteState state;

  /** The single position prefix. */
  private HashMap<String, HashSet<String>> singlePositionPrefix;

  /** The multiple position prefix. */
  private HashMap<String, HashSet<String>> multiplePositionPrefix;

  /** The set position prefix. */
  private HashMap<String, HashSet<String>> setPositionPrefix;

  /** The prefix reference index. */
  private HashMap<String, HashMap<String, Long>> prefixReferenceIndex;

  /** The prefix id index. */
  private HashMap<String, HashMap<String, Integer>> prefixIdIndex;

  /** The token stats min pos. */
  Integer tokenStatsMinPos;

  /** The token stats max pos. */
  Integer tokenStatsMaxPos;

  /** The token stats number. */
  Integer tokenStatsNumber;

  /** The mtas tmp docs chained file name. */
  private String mtasTmpFieldFileName, mtasTmpObjectFileName,
      mtasTmpDocsFileName, mtasTmpDocFileName, mtasTmpDocsChainedFileName;

  /** The mtas index object parent file name. */
  private String mtasObjectFileName, mtasTermFileName, mtasIndexFieldFileName,
      mtasPrefixFileName, mtasDocFileName, mtasIndexDocIdFileName,
      mtasIndexObjectIdFileName, mtasIndexObjectPositionFileName,
      mtasIndexObjectParentFileName;

  /** The delegate postings format name. */
  private String name, delegatePostingsFormatName;

  private static int mtasFieldsConsumerWriteLockCounterMax = 10;
  private static int mtasFieldsConsumerWriteLockSleepTime = 30000;
  
  
  /**
   * Instantiates a new mtas fields consumer.
   *
   * @param fieldsConsumer
   *          the fields consumer
   * @param state
   *          the state
   * @param name
   *          the name
   * @param delegatePostingsFormatName
   *          the delegate postings format name
   */
  public MtasFieldsConsumer(FieldsConsumer fieldsConsumer,
      SegmentWriteState state, String name, String delegatePostingsFormatName) {
    this.delegateFieldsConsumer = fieldsConsumer;
    this.state = state;
    this.name = name;
    this.delegatePostingsFormatName = delegatePostingsFormatName;
    // prefix stats
    singlePositionPrefix = new HashMap<String, HashSet<String>>();
    multiplePositionPrefix = new HashMap<String, HashSet<String>>();
    setPositionPrefix = new HashMap<String, HashSet<String>>();
    prefixReferenceIndex = new HashMap<String, HashMap<String, Long>>();
    prefixIdIndex = new HashMap<String, HashMap<String, Integer>>();
    // temporary fileNames
    mtasTmpFieldFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix,
        MtasCodecPostingsFormat.MTAS_TMP_FIELD_EXTENSION);
    mtasTmpObjectFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix,
        MtasCodecPostingsFormat.MTAS_TMP_OBJECT_EXTENSION);
    mtasTmpDocsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, MtasCodecPostingsFormat.MTAS_TMP_DOCS_EXTENSION);
    mtasTmpDocFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, MtasCodecPostingsFormat.MTAS_TMP_DOC_EXTENSION);
    mtasTmpDocsChainedFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix,
        MtasCodecPostingsFormat.MTAS_TMP_DOCS_CHAINED_EXTENSION);
    // fileNames
    mtasObjectFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, MtasCodecPostingsFormat.MTAS_OBJECT_EXTENSION);
    mtasTermFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, MtasCodecPostingsFormat.MTAS_TERM_EXTENSION);
    mtasIndexFieldFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix,
        MtasCodecPostingsFormat.MTAS_FIELD_EXTENSION);
    mtasPrefixFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, MtasCodecPostingsFormat.MTAS_PREFIX_EXTENSION);
    mtasDocFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, MtasCodecPostingsFormat.MTAS_DOC_EXTENSION);
    mtasIndexDocIdFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix,
        MtasCodecPostingsFormat.MTAS_INDEX_DOC_ID_EXTENSION);
    mtasIndexObjectIdFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix,
        MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_ID_EXTENSION);
    mtasIndexObjectPositionFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix,
        MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_POSITION_EXTENSION);
    mtasIndexObjectParentFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix,
        MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_PARENT_EXTENSION);
  }

  /**
   * Register prefix.
   *
   * @param field
   *          the field
   * @param prefix
   *          the prefix
   * @param outPrefix
   *          the out prefix
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void registerPrefix(String field, String prefix,
      IndexOutput outPrefix) throws IOException {
    if (!prefixReferenceIndex.containsKey(field)) {
      prefixReferenceIndex.put(field, new HashMap<String, Long>());
      prefixIdIndex.put(field, new HashMap<String, Integer>());
    }
    if (!prefixReferenceIndex.get(field).containsKey(prefix)) {
      int id = prefixReferenceIndex.get(field).size();
      prefixReferenceIndex.get(field).put(prefix, outPrefix.getFilePointer());
      prefixIdIndex.get(field).put(prefix, id);
      outPrefix.writeString(prefix);
    }
  }

  /**
   * Register prefix stats single position value.
   *
   * @param field
   *          the field
   * @param value
   *          the value
   * @param outPrefix
   *          the out prefix
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void registerPrefixStatsSinglePositionValue(String field, String value,
      IndexOutput outPrefix) throws IOException {
    initPrefixStatsField(field);
    String prefix = MtasToken.getPrefixFromValue(value);
    registerPrefix(field, prefix, outPrefix);
    if (!multiplePositionPrefix.get(field).contains(prefix)) {
      singlePositionPrefix.get(field).add(prefix);
    }
  }

  /**
   * Register prefix stats range position value.
   *
   * @param field
   *          the field
   * @param value
   *          the value
   * @param outPrefix
   *          the out prefix
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void registerPrefixStatsRangePositionValue(String field, String value,
      IndexOutput outPrefix) throws IOException {
    initPrefixStatsField(field);
    String prefix = MtasToken.getPrefixFromValue(value);
    registerPrefix(field, prefix, outPrefix);
    singlePositionPrefix.get(field).remove(prefix);
    multiplePositionPrefix.get(field).add(prefix);
  }

  /**
   * Register prefix stats set position value.
   *
   * @param field
   *          the field
   * @param value
   *          the value
   * @param outPrefix
   *          the out prefix
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void registerPrefixStatsSetPositionValue(String field, String value,
      IndexOutput outPrefix) throws IOException {
    initPrefixStatsField(field);
    String prefix = MtasToken.getPrefixFromValue(value);
    registerPrefix(field, prefix, outPrefix);
    singlePositionPrefix.get(field).remove(prefix);
    multiplePositionPrefix.get(field).add(prefix);
    setPositionPrefix.get(field).add(prefix);
  }

  /**
   * Inits the prefix stats field.
   *
   * @param field
   *          the field
   */
  private void initPrefixStatsField(String field) {
    if (!singlePositionPrefix.containsKey(field)) {
      singlePositionPrefix.put(field, new HashSet<String>());
    }
    if (!multiplePositionPrefix.containsKey(field)) {
      multiplePositionPrefix.put(field, new HashSet<String>());
    }
    if (!setPositionPrefix.containsKey(field)) {
      setPositionPrefix.put(field, new HashSet<String>());
    }
  }

  /**
   * Gets the prefix stats single position prefix attribute.
   *
   * @param field
   *          the field
   * @return the prefix stats single position prefix attribute
   */
  public String getPrefixStatsSinglePositionPrefixAttribute(String field) {
    return StringUtils.join(singlePositionPrefix.get(field).toArray(),
        MtasToken.DELIMITER);
  }

  /**
   * Gets the prefix stats multiple position prefix attribute.
   *
   * @param field
   *          the field
   * @return the prefix stats multiple position prefix attribute
   */
  public String getPrefixStatsMultiplePositionPrefixAttribute(String field) {
    return StringUtils.join(multiplePositionPrefix.get(field).toArray(),
        MtasToken.DELIMITER);
  }

  /**
   * Gets the prefix stats set position prefix attribute.
   *
   * @param field
   *          the field
   * @return the prefix stats set position prefix attribute
   */
  public String getPrefixStatsSetPositionPrefixAttribute(String field) {
    return StringUtils.join(setPositionPrefix.get(field).toArray(),
        MtasToken.DELIMITER);
  }

  /**
   * Prefix stats to string.
   *
   * @return the string
   */
  public String prefixStatsToString() {
    String text = "";
    text += "PrefixStats\n";
    for (String field : singlePositionPrefix.keySet()) {
      text += "* Field " + field + ":\n";
      text += "  - single-position: " + singlePositionPrefix.get(field) + "\n";
      text += "  - multiple-position: " + multiplePositionPrefix.get(field)
          + "\n";
    }
    return text;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.codecs.FieldsConsumer#write(org.apache.lucene.index.
   * Fields )
   */
  @Override
  public void write(Fields fields) throws IOException {         
    write(state.fieldInfos, fields);       
    delegateFieldsConsumer.write(fields);
    
  }

  /**
   * Write.
   *
   * @param fieldInfos
   *          the field infos
   * @param fields
   *          the fields
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void write(FieldInfos fieldInfos, Fields fields) throws IOException {
    IndexOutput outField, outDoc, outIndexDocId, outIndexObjectId,
        outIndexObjectPosition, outIndexObjectParent, outTerm, outObject,
        outPrefix;
    IndexOutput outTmpDoc, outTmpField;
        
    // temporary temporary index in memory for doc
    TreeMap<Integer, Long> memoryIndexTemporaryObject = new TreeMap<Integer, Long>();
    // create (backwards) chained new temporary index docs
    TreeMap<Integer, Long> memoryTmpDocChainList = new TreeMap<Integer, Long>();
    // list of objectIds and references to objects
    TreeMap<Integer, Long> memoryIndexDocList = new TreeMap<Integer, Long>();

    
    //get lock
    Lock mtasFieldsConsumerLock = null;
    int mtasFieldsConsumerLockCounter = 0;
    while(mtasFieldsConsumerLock==null) {
      mtasFieldsConsumerLockCounter++;
      try {
        mtasFieldsConsumerLock = state.directory.obtainLock("MtasFieldsConsumer");
      } catch (LockObtainFailedException e) {
        if(mtasFieldsConsumerLockCounter>=mtasFieldsConsumerWriteLockCounterMax) {
          throw new IOException("couldn't obtain lock for MtasFieldsConsumer to write, tried "+mtasFieldsConsumerLockCounter+" times");
        }
        try {
          Thread.sleep(mtasFieldsConsumerWriteLockSleepTime);
        } catch (InterruptedException e1) {
          //shouldn't happen?
        }
      }
    }  
    
    //try to release lock in case of IOException
    try {
    
    // create file tmpDoc
    outTmpDoc = state.directory.createOutput(mtasTmpDocFileName, state.context);
    // create file tmpField
    outTmpField = state.directory.createOutput(mtasTmpFieldFileName,
        state.context);
    // create file indexDoc
    outDoc = state.directory.createOutput(mtasDocFileName, state.context);
    CodecUtil.writeIndexHeader(outDoc, name,
        MtasCodecPostingsFormat.VERSION_CURRENT, state.segmentInfo.getId(),
        state.segmentSuffix);
    outDoc.writeString(delegatePostingsFormatName);
    // create file indexDocId
    outIndexDocId = state.directory.createOutput(mtasIndexDocIdFileName,
        state.context);
    CodecUtil.writeIndexHeader(outIndexDocId, name,
        MtasCodecPostingsFormat.VERSION_CURRENT, state.segmentInfo.getId(),
        state.segmentSuffix);
    outIndexDocId.writeString(delegatePostingsFormatName);
    // create file indexObjectId
    outIndexObjectId = state.directory.createOutput(mtasIndexObjectIdFileName,
        state.context);
    CodecUtil.writeIndexHeader(outIndexObjectId, name,
        MtasCodecPostingsFormat.VERSION_CURRENT, state.segmentInfo.getId(),
        state.segmentSuffix);
    outIndexObjectId.writeString(delegatePostingsFormatName);
    // create file indexObjectPosition
    outIndexObjectPosition = state.directory
        .createOutput(mtasIndexObjectPositionFileName, state.context);
    CodecUtil.writeIndexHeader(outIndexObjectPosition, name,
        MtasCodecPostingsFormat.VERSION_CURRENT, state.segmentInfo.getId(),
        state.segmentSuffix);
    outIndexObjectPosition.writeString(delegatePostingsFormatName);
    // create file indexObjectParent
    outIndexObjectParent = state.directory
        .createOutput(mtasIndexObjectParentFileName, state.context);
    CodecUtil.writeIndexHeader(outIndexObjectParent, name,
        MtasCodecPostingsFormat.VERSION_CURRENT, state.segmentInfo.getId(),
        state.segmentSuffix);
    outIndexObjectParent.writeString(delegatePostingsFormatName);
    // create file term
    outTerm = state.directory.createOutput(mtasTermFileName, state.context);
    CodecUtil.writeIndexHeader(outTerm, name,
        MtasCodecPostingsFormat.VERSION_CURRENT, state.segmentInfo.getId(),
        state.segmentSuffix);
    outTerm.writeString(delegatePostingsFormatName);
    // create file prefix
    outPrefix = state.directory.createOutput(mtasPrefixFileName, state.context);
    CodecUtil.writeIndexHeader(outPrefix, name,
        MtasCodecPostingsFormat.VERSION_CURRENT, state.segmentInfo.getId(),
        state.segmentSuffix);
    outPrefix.writeString(delegatePostingsFormatName);
    // create file object
    outObject = state.directory.createOutput(mtasObjectFileName, state.context);
    CodecUtil.writeIndexHeader(outObject, name,
        MtasCodecPostingsFormat.VERSION_CURRENT, state.segmentInfo.getId(),
        state.segmentSuffix);
    outObject.writeString(delegatePostingsFormatName);
    // For each field
    for (String field : fields) {
      Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      } else {
        // new temporary object storage for this field
        IndexOutput outTmpObject = state.directory
            .createOutput(mtasTmpObjectFileName, state.context);
        // new temporary index docs for this field
        IndexOutput outTmpDocs = state.directory
            .createOutput(mtasTmpDocsFileName, state.context);
        // get fieldInfo
        FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        // get properties terms
        boolean hasPositions = terms.hasPositions();
        boolean hasFreqs = terms.hasFreqs();
        boolean hasPayloads = fieldInfo.hasPayloads();
        boolean hasOffsets = terms.hasOffsets();
        // register references
        Long smallestTermFilepointer = outTerm.getFilePointer();
        Long smallestPrefixFilepointer = outTerm.getFilePointer();
        int termCounter = 0;
        // only if freqs, positions and payload available
        if (hasFreqs && hasPositions && hasPayloads) {
          // compute flags
          int flags = PostingsEnum.POSITIONS | PostingsEnum.PAYLOADS;
          if (hasOffsets) {
            flags = flags | PostingsEnum.OFFSETS;
          }
          // get terms
          TermsEnum termsEnum = terms.iterator();
          PostingsEnum postingsEnum = null;
          // for each term in field
          while (true) {
            BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            // store term and get ref
            Long termRef = outTerm.getFilePointer();
            outTerm.writeString(term.utf8ToString());
            termCounter++;
            // get postings
            postingsEnum = termsEnum.postings(postingsEnum, flags);
            // for each doc in field+term
            while (true) {
              Integer doc = postingsEnum.nextDoc();
              if (doc.equals(DocIdSetIterator.NO_MORE_DOCS)) {
                break;
              }
              int freq = postingsEnum.freq();
              // temporary storage objects and temporary index in memory for
              // doc
              memoryIndexTemporaryObject.clear();
              Long offsetFilePointerTmpObject = outTmpObject.getFilePointer();
              for (int i = 0; i < freq; i++) {
                Long currentFilePointerTmpObject = outTmpObject
                    .getFilePointer();
                Integer mtasId;
                int position = postingsEnum.nextPosition();
                BytesRef payload = postingsEnum.getPayload();
                if (hasOffsets) {
                  mtasId = createObjectAndRegisterPrefix(field, outTmpObject,
                      term, termRef, position, payload,
                      postingsEnum.startOffset(), postingsEnum.endOffset(),
                      outPrefix);
                } else {
                  mtasId = createObjectAndRegisterPrefix(field, outTmpObject,
                      term, termRef, position, payload, outPrefix);
                }
                if (mtasId != null) {
                  assert !memoryIndexTemporaryObject.containsKey(
                      mtasId) : "mtasId should be unique in this selection";
                  memoryIndexTemporaryObject.put(mtasId,
                      currentFilePointerTmpObject);
                }
              } // end loop positions
              // store temporary index for this doc
              if (memoryIndexTemporaryObject.size() > 0) {
                // docId for this part
                outTmpDocs.writeVInt(doc);
                // number of objects/tokens in this part
                outTmpDocs.writeVInt(memoryIndexTemporaryObject.size());
                // offset to be used for references
                outTmpDocs.writeVLong(offsetFilePointerTmpObject);
                // loop over tokens
                for (Entry<Integer, Long> entry : memoryIndexTemporaryObject
                    .entrySet()) {
                  // mtasId object
                  outTmpDocs.writeVInt(entry.getKey());
                  // reference object
                  outTmpDocs.writeVLong(
                      (entry.getValue() - offsetFilePointerTmpObject));
                }
              }
              // clean up
              memoryIndexTemporaryObject.clear();
            } // end loop docs
          } // end loop terms
          // set fieldInfo
          fieldInfos.fieldInfo(field).putAttribute(
              MtasCodecPostingsFormat.MTAS_FIELDINFO_ATTRIBUTE_PREFIX_SINGLE_POSITION,
              getPrefixStatsSinglePositionPrefixAttribute(field));
          fieldInfos.fieldInfo(field).putAttribute(
              MtasCodecPostingsFormat.MTAS_FIELDINFO_ATTRIBUTE_PREFIX_MULTIPLE_POSITION,
              getPrefixStatsMultiplePositionPrefixAttribute(field));
          fieldInfos.fieldInfo(field).putAttribute(
              MtasCodecPostingsFormat.MTAS_FIELDINFO_ATTRIBUTE_PREFIX_SET_POSITION,
              getPrefixStatsSetPositionPrefixAttribute(field));

        } // end processing field with freqs, positions and payload
        // close temporary object storage and index docs
        outTmpObject.close();
        outTmpDocs.close();

        // create (backwards) chained new temporary index docs
        IndexInput inTmpDocs = state.directory.openInput(mtasTmpDocsFileName,
            state.context);
        IndexOutput outTmpDocsChained = state.directory
            .createOutput(mtasTmpDocsChainedFileName, state.context);
        memoryTmpDocChainList.clear();
        while (true) {
          try {
            Long currentFilepointer = outTmpDocsChained.getFilePointer();
            // copy docId
            int docId = inTmpDocs.readVInt();
            outTmpDocsChained.writeVInt(docId);
            // copy size
            int size = inTmpDocs.readVInt();
            outTmpDocsChained.writeVInt(size);
            // offset references
            outTmpDocsChained.writeVLong(inTmpDocs.readVLong());
            for (int t = 0; t < size; t++) {
              outTmpDocsChained.writeVInt(inTmpDocs.readVInt());
              outTmpDocsChained.writeVLong(inTmpDocs.readVLong());
            }
            // set back reference to part with same docId
            if (memoryTmpDocChainList.containsKey(docId)) {
              // reference to previous
              outTmpDocsChained.writeVLong(memoryTmpDocChainList.get(docId));
            } else {
              // self reference indicates end of chain
              outTmpDocsChained.writeVLong(currentFilepointer);
            }
            // update temporary index in memory
            memoryTmpDocChainList.put(docId, currentFilepointer);
          } catch (IOException ex) {
            break;
          }
        }
        outTmpDocsChained.close();
        inTmpDocs.close();
        state.directory.deleteFile(mtasTmpDocsFileName);

        // set reference to tmpDoc in Field
        if (memoryTmpDocChainList.size() > 0) {
          outTmpField.writeString(field);
          outTmpField.writeVLong(outTmpDoc.getFilePointer());
          outTmpField.writeVInt(memoryTmpDocChainList.size());
          outTmpField.writeVLong(smallestTermFilepointer);
          outTmpField.writeVInt(termCounter);
          outTmpField.writeVLong(smallestPrefixFilepointer);
          outTmpField.writeVInt(prefixReferenceIndex.get(field).size());
          // fill indexDoc
          IndexInput inTmpDocsChained = state.directory
              .openInput(mtasTmpDocsChainedFileName, state.context);
          IndexInput inTmpObject = state.directory
              .openInput(mtasTmpObjectFileName, state.context);
          for (Entry<Integer, Long> entry : memoryTmpDocChainList.entrySet()) {
            Integer docId = entry.getKey();
            Long currentFilePointer, newFilePointer;
            // list of objectIds and references to objects
            memoryIndexDocList.clear();
            // construct final object + indexObjectId for docId
            currentFilePointer = entry.getValue();
            // collect objects for document
            tokenStatsMinPos = null;
            tokenStatsMaxPos = null;
            tokenStatsNumber = 0;
            while (true) {
              inTmpDocsChained.seek(currentFilePointer);
              Integer docIdPart = inTmpDocsChained.readVInt();
              assert docIdPart.equals(
                  docId) : "conflicting docId in reference to temporaryIndexDocsChained";
              // number of objects/tokens in part
              int size = inTmpDocsChained.readVInt();
              long offsetFilePointerTmpObject = inTmpDocsChained.readVLong();
              assert size > 0 : "number of objects/tokens in part cannot be "
                  + size;
              for (int t = 0; t < size; t++) {
                int mtasId = inTmpDocsChained.readVInt();
                Long tmpObjectRef = inTmpDocsChained.readVLong()
                    + offsetFilePointerTmpObject;
                assert !memoryIndexDocList.containsKey(
                    mtasId) : "mtasId should be unique in this selection";
                // initially, store ref to tmpObject
                memoryIndexDocList.put(mtasId, tmpObjectRef);
              }
              // reference to next part
              newFilePointer = inTmpDocsChained.readVLong();
              if (newFilePointer.equals(currentFilePointer)) {
                break; // end of chained parts
              } else {
                currentFilePointer = newFilePointer;
              }
            }
            // now create new objects, sorted by mtasId
            Long smallestObjectFilepointer = outObject.getFilePointer();
            for (Entry<Integer, Long> objectEntry : memoryIndexDocList
                .entrySet()) {
              int mtasId = objectEntry.getKey();
              Long tmpObjectRef = objectEntry.getValue();
              Long objectRef = outObject.getFilePointer();
              copyObjectAndUpdateStats(mtasId, inTmpObject, tmpObjectRef,
                  outObject);
              // update with new ref
              memoryIndexDocList.put(mtasId, objectRef);
            }
            // check mtasIds properties
            assert memoryIndexDocList.firstKey()
                .equals(0) : "first mtasId should not be "
                    + memoryIndexDocList.firstKey();
            assert (1 + memoryIndexDocList.lastKey()
                - memoryIndexDocList.firstKey()) == memoryIndexDocList
                    .size() : "missing mtasId";
            assert tokenStatsNumber.equals(memoryIndexDocList
                .size()) : "incorrect number of items in tokenStats";

            // store item in tmpDoc
            outTmpDoc.writeVInt(docId);
            outTmpDoc.writeVLong(outIndexObjectId.getFilePointer());

            int mtasId = 0;
            // compute linear approximation (least squares method, integer
            // constants)
            long tmpN = memoryIndexDocList.size();
            long tmpSumY = 0, tmpSumXY = 0;
            long tmpSumX = 0, tmpSumXX = 0;
            for (Entry<Integer, Long> objectEntry : memoryIndexDocList
                .entrySet()) {
              assert objectEntry.getKey().equals(mtasId) : "unexpected mtasId";
              tmpSumY += objectEntry.getValue();
              tmpSumX += mtasId;
              tmpSumXY += mtasId * objectEntry.getValue();
              tmpSumXX += mtasId * mtasId;
              mtasId++;
            }
            int objectRefApproxQuotient = (int) (((tmpN * tmpSumXY)
                - (tmpSumX * tmpSumY))
                / ((tmpN * tmpSumXX) - (tmpSumX * tmpSumX)));
            long objectRefApproxOffset = (tmpSumY
                - objectRefApproxQuotient * tmpSumX) / tmpN;
            Long objectRefApproxCorrection;
            long maxAbsObjectRefApproxCorrection = 0;
            // compute maximum correction
            mtasId = 0;
            for (Entry<Integer, Long> objectEntry : memoryIndexDocList
                .entrySet()) {
              objectRefApproxCorrection = (objectEntry.getValue()
                  - (objectRefApproxOffset
                      + (mtasId * objectRefApproxQuotient)));
              maxAbsObjectRefApproxCorrection = Math.max(
                  maxAbsObjectRefApproxCorrection,
                  Math.abs(objectRefApproxCorrection));
              mtasId++;
            }
            byte storageFlags;
            if (maxAbsObjectRefApproxCorrection <= Long.valueOf(Byte.MAX_VALUE)
                + 1) {
              storageFlags = MtasCodecPostingsFormat.MTAS_STORAGE_BYTE;
            } else if (maxAbsObjectRefApproxCorrection <= Long
                .valueOf(Short.MAX_VALUE) + 1) {
              storageFlags = MtasCodecPostingsFormat.MTAS_STORAGE_SHORT;
            } else if (maxAbsObjectRefApproxCorrection <= Long
                .valueOf(Integer.MAX_VALUE) + 1) {
              storageFlags = MtasCodecPostingsFormat.MTAS_STORAGE_INTEGER;
            } else {
              storageFlags = MtasCodecPostingsFormat.MTAS_STORAGE_LONG;
            }
            // update indexObjectId with correction on approximated ref (assume
            // can be stored as int)
            mtasId = 0;
            for (Entry<Integer, Long> objectEntry : memoryIndexDocList
                .entrySet()) {
              objectRefApproxCorrection = (objectEntry.getValue()
                  - (objectRefApproxOffset
                      + (mtasId * objectRefApproxQuotient)));
              if (storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_BYTE) {
                outIndexObjectId
                    .writeByte(objectRefApproxCorrection.byteValue());
              } else if (storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_SHORT) {
                outIndexObjectId
                    .writeShort(objectRefApproxCorrection.shortValue());
              } else if (storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_INTEGER) {
                outIndexObjectId.writeInt(objectRefApproxCorrection.intValue());
              } else {
                outIndexObjectId.writeLong(objectRefApproxCorrection);
              }
              mtasId++;
            }
            outTmpDoc.writeVLong(smallestObjectFilepointer);
            outTmpDoc.writeVInt(objectRefApproxQuotient);
            outTmpDoc.writeZLong(objectRefApproxOffset);
            outTmpDoc.writeByte(storageFlags);
            outTmpDoc.writeVInt(tokenStatsNumber);
            outTmpDoc.writeVInt(tokenStatsMinPos);
            outTmpDoc.writeVInt(tokenStatsMaxPos);
            // clean up
            memoryIndexDocList.clear();
          } // end loop over docs
          inTmpDocsChained.close();
          inTmpObject.close();
        }
        // clean up
        memoryTmpDocChainList.clear();
        // remove temporary files
        state.directory.deleteFile(mtasTmpObjectFileName);
        state.directory.deleteFile(mtasTmpDocsChainedFileName);
        // store references for field

      } // end processing field
    } // end loop fields
    // close temporary index doc
    outTmpDoc.close();
    // close indexField, indexObjectId and object
    CodecUtil.writeFooter(outTmpField);
    outTmpField.close();
    CodecUtil.writeFooter(outIndexObjectId);
    outIndexObjectId.close();
    CodecUtil.writeFooter(outObject);
    outObject.close();
    CodecUtil.writeFooter(outTerm);
    outTerm.close();
    CodecUtil.writeFooter(outPrefix);
    outPrefix.close();

    // create final doc, fill indexObjectPosition, indexObjectParent and
    // indexTermPrefixPosition, create final field
    IndexInput inTmpField = state.directory.openInput(mtasTmpFieldFileName,
        state.context);
    IndexInput inTmpDoc = state.directory.openInput(mtasTmpDocFileName,
        state.context);
    IndexInput inObjectId = state.directory.openInput(mtasIndexObjectIdFileName,
        state.context);
    IndexInput inObject = state.directory.openInput(mtasObjectFileName,
        state.context);
    IndexInput inTerm = state.directory.openInput(mtasTermFileName,
        state.context);
    outField = state.directory.createOutput(mtasIndexFieldFileName,
        state.context);
    CodecUtil.writeIndexHeader(outField, name,
        MtasCodecPostingsFormat.VERSION_CURRENT, state.segmentInfo.getId(),
        state.segmentSuffix);
    outField.writeString(delegatePostingsFormatName);

    while (true) {
      try {
        // read from tmpField
        String field = inTmpField.readString();
        long fpTmpDoc = inTmpField.readVLong();
        int numberDocs = inTmpField.readVInt();
        long fpTerm = inTmpField.readVLong();
        int numberTerms = inTmpField.readVInt();
        long fpPrefix = inTmpField.readVLong();
        int numberPrefixes = inTmpField.readVInt();
        inTmpDoc.seek(fpTmpDoc);
        long fpFirstDoc = outDoc.getFilePointer();
        // get prefixId index
        HashMap<String, Integer> prefixIdIndexField = prefixIdIndex.get(field);
        // construct MtasRBTree for indexDocId
        MtasRBTree mtasDocIdTree = new MtasRBTree(true, false);
        for (int docCounter = 0; docCounter < numberDocs; docCounter++) {
          try {
            // get info from tmpDoc
            int docId = inTmpDoc.readVInt();
            // filePointer indexObjectId
            Long fpIndexObjectId = inTmpDoc.readVLong();
            // filePointer indexObjectPosition (unknown)
            Long fpIndexObjectPosition;
            // filePointer indexObjectParent (unknown)
            Long fpIndexObjectParent;
            // constants for approximation object references for this document
            long smallestObjectFilepointer = inTmpDoc.readVLong();
            int objectRefApproxQuotient = inTmpDoc.readVInt();
            long objectRefApproxOffset = inTmpDoc.readZLong();
            byte storageFlags = inTmpDoc.readByte();
            // number objects/tokens
            int size = inTmpDoc.readVInt();
            // construct MtasRBTree
            MtasRBTree mtasPositionTree = new MtasRBTree(false, true);
            MtasRBTree mtasParentTree = new MtasRBTree(false, true);
            inObjectId.seek(fpIndexObjectId);
            long refCorrection;
            long ref;
            for (int mtasId = 0; mtasId < size; mtasId++) {
              if (storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_BYTE) {
                refCorrection = inObjectId.readByte();
              } else if (storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_SHORT) {
                refCorrection = inObjectId.readShort();
              } else if (storageFlags == MtasCodecPostingsFormat.MTAS_STORAGE_INTEGER) {
                refCorrection = inObjectId.readInt();
              } else {
                refCorrection = inObjectId.readLong();
              }
              ref = objectRefApproxOffset + mtasId * objectRefApproxQuotient
                  + refCorrection;
              MtasToken<String> token = MtasCodecPostingsFormat
                  .getToken(inObject, inTerm, ref);
              String prefix = token.getPrefix();
              Integer prefixId = prefixIdIndexField.get(prefix);
              token.setPrefixId(prefixId);              
              assert token.getId().equals(mtasId) : "unexpected mtasId "
                  + mtasId;
              mtasPositionTree.addPositionAndObjectFromToken(token);
              mtasParentTree.addParentFromToken(token);
            }
            // store mtasPositionTree and mtasParentTree
            fpIndexObjectPosition = storeTree(mtasPositionTree,
                outIndexObjectPosition, smallestObjectFilepointer);
            fpIndexObjectParent = storeTree(mtasParentTree,
                outIndexObjectParent, smallestObjectFilepointer);
            long fpDoc = outDoc.getFilePointer();
            // create indexDoc with updated fpIndexObjectPosition from tmpDoc
            outDoc.writeVInt(docId); // docId
            // reference indexObjectId
            outDoc.writeVLong(fpIndexObjectId);
            // reference indexObjectPosition
            outDoc.writeVLong(fpIndexObjectPosition);
            // reference indexObjectParent
            outDoc.writeVLong(fpIndexObjectParent);
            // variables approximation and storage references object
            outDoc.writeVLong(smallestObjectFilepointer);
            outDoc.writeVInt(objectRefApproxQuotient);
            outDoc.writeZLong(objectRefApproxOffset);
            outDoc.writeByte(storageFlags);
            // number of objects
            outDoc.writeVInt(size);
            // minPosition
            outDoc.writeVInt(inTmpDoc.readVInt());
            // maxPosition
            outDoc.writeVInt(inTmpDoc.readVInt());
            // add to tree for indexDocId
            mtasDocIdTree.addIdFromDoc(docId, fpDoc);
          } catch (IOException ex) {
            break;
          }

        }
        long fpIndexDocId = storeTree(mtasDocIdTree, outIndexDocId, fpFirstDoc);

        // store in indexField
        outField.writeString(field);
        outField.writeVLong(fpFirstDoc);
        outField.writeVLong(fpIndexDocId);
        outField.writeVInt(numberDocs);
        outField.writeVLong(fpTerm);
        outField.writeVInt(numberTerms);
        outField.writeVLong(fpPrefix);
        outField.writeVInt(numberPrefixes);
      } catch (EOFException e) {
        break;
      }
      // end loop over fields
    }
    inObject.close();
    inObjectId.close();
    inTmpDoc.close();
    inTmpField.close();

    // remove temporary files
    state.directory.deleteFile(mtasTmpDocFileName);
    state.directory.deleteFile(mtasTmpFieldFileName);
    // close indexDoc, indexObjectPosition and indexObjectParent
    CodecUtil.writeFooter(outDoc);
    outDoc.close();
    CodecUtil.writeFooter(outIndexObjectPosition);
    outIndexObjectPosition.close();
    CodecUtil.writeFooter(outIndexObjectParent);
    outIndexObjectParent.close();
    CodecUtil.writeFooter(outIndexDocId);
    outIndexDocId.close();
    CodecUtil.writeFooter(outField);
    outField.close();
    
    //release lock
    mtasFieldsConsumerLock.close();
    } catch(IOException e) {
      //release lock in case of IOException 
      mtasFieldsConsumerLock.close();
      //System.out.println(e.getStackTrace().toString());
      throw (IOException) new IOException().initCause(e);
    }
  }

  /**
   * Creates the object and register prefix.
   *
   * @param field
   *          the field
   * @param out
   *          the out
   * @param term
   *          the term
   * @param termRef
   *          the term ref
   * @param startPosition
   *          the start position
   * @param payload
   *          the payload
   * @param outPrefix
   *          the out prefix
   * @return the integer
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private Integer createObjectAndRegisterPrefix(String field, IndexOutput out,
      BytesRef term, Long termRef, int startPosition, BytesRef payload,
      IndexOutput outPrefix) throws IOException {
    return createObjectAndRegisterPrefix(field, out, term, termRef,
        startPosition, payload, null, null, outPrefix);
  }

  /**
   * Creates the object and register prefix.
   *
   * @param field
   *          the field
   * @param out
   *          the out
   * @param term
   *          the term
   * @param termRef
   *          the term ref
   * @param startPosition
   *          the start position
   * @param payload
   *          the payload
   * @param startOffset
   *          the start offset
   * @param endOffset
   *          the end offset
   * @param outPrefix
   *          the out prefix
   * @return the integer
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private Integer createObjectAndRegisterPrefix(String field, IndexOutput out,
      BytesRef term, Long termRef, int startPosition, BytesRef payload,
      Integer startOffset, Integer endOffset, IndexOutput outPrefix)
      throws IOException {
    Integer mtasId = null;
    if (payload != null) {
      MtasPayloadDecoder payloadDecoder = new MtasPayloadDecoder();
      payloadDecoder.init(startPosition, Arrays.copyOfRange(payload.bytes,
          payload.offset, (payload.offset + payload.length)));
      mtasId = payloadDecoder.getMtasId();
      Integer mtasParentId = payloadDecoder.getMtasParentId();
      byte[] mtasPayload = payloadDecoder.getMtasPayload();
      MtasPosition mtasPosition = payloadDecoder.getMtasPosition();
      if (mtasPosition == null) {
        if (startOffset != null) {
          mtasPosition = new MtasPosition(startOffset, endOffset);
        }
      }
      MtasOffset mtasOffset = payloadDecoder.getMtasOffset();
      MtasOffset mtasRealOffset = payloadDecoder.getMtasRealOffset();
      // only if really mtas object
      if (mtasId != null) {
        // compute flags
        int objectFlags = 0;
        if (mtasPosition != null) {
          if (mtasPosition.checkType(MtasPosition.POSITION_RANGE)) {
            objectFlags = objectFlags
                | MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_RANGE;
            registerPrefixStatsRangePositionValue(field, term.utf8ToString(),
                outPrefix);
          } else if (mtasPosition.checkType(MtasPosition.POSITION_SET)) {
            objectFlags = objectFlags
                | MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_SET;
            registerPrefixStatsSetPositionValue(field, term.utf8ToString(),
                outPrefix);
          } else {
            registerPrefixStatsSinglePositionValue(field, term.utf8ToString(),
                outPrefix);
          }
        }
        if (mtasParentId != null) {
          objectFlags = objectFlags
              | MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PARENT;
        }
        if (mtasOffset != null) {
          objectFlags = objectFlags
              | MtasCodecPostingsFormat.MTAS_OBJECT_HAS_OFFSET;
        }
        if (mtasRealOffset != null) {
          objectFlags = objectFlags
              | MtasCodecPostingsFormat.MTAS_OBJECT_HAS_REALOFFSET;
        }
        if (mtasPayload != null) {
          objectFlags = objectFlags
              | MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PAYLOAD;
        }
        // create object
        out.writeVInt(mtasId);
        out.writeVInt(objectFlags);
        if ((objectFlags
            & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PARENT) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PARENT) {
          out.writeVInt(mtasParentId);
        }
        if ((objectFlags
            & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_RANGE) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_RANGE) {
          int tmpStart = mtasPosition.getStart();
          out.writeVInt(tmpStart);
          out.writeVInt((mtasPosition.getEnd() - tmpStart));
        } else if ((objectFlags
            & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_SET) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_SET) {
          TreeSet<Integer> positions = mtasPosition.getPositions();
          out.writeVInt(positions.size());
          int tmpPrevious = 0;
          for (int position : positions) {
            out.writeVInt((position - tmpPrevious));
            tmpPrevious = position;
          }
        } else {
          out.writeVInt(mtasPosition.getStart());
        }
        if ((objectFlags
            & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_OFFSET) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_OFFSET) {
          int tmpStart = mtasOffset.getStart();
          out.writeVInt(mtasOffset.getStart());
          out.writeVInt((mtasOffset.getEnd() - tmpStart));
        }
        if ((objectFlags
            & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_REALOFFSET) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_REALOFFSET) {
          int tmpStart = mtasRealOffset.getStart();
          out.writeVInt(mtasRealOffset.getStart());
          out.writeVInt((mtasRealOffset.getEnd() - tmpStart));
        }
        if ((objectFlags
            & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PAYLOAD) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PAYLOAD) {
          out.writeVInt(mtasPayload.length);
          out.writeBytes(mtasPayload, mtasPayload.length);
        }
        out.writeVLong(termRef);
      } // storage token
    } // payload available

    return mtasId;
  }

  /**
   * Store tree.
   *
   * @param tree
   *          the tree
   * @param out
   *          the out
   * @param refApproxOffset
   *          the ref approx offset
   * @return the long
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private Long storeTree(MtasTree<?> tree, IndexOutput out,
      long refApproxOffset) throws IOException {
    return storeTree(tree.close(), tree.isSinglePoint(),
        tree.isStorePrefixAndTermRef(), out, null, refApproxOffset);
  }

  /**
   * Store tree.
   *
   * @param node
   *          the node
   * @param isSinglePoint
   *          the is single point
   * @param storeAdditionalInformation
   *          the store additional information
   * @param out
   *          the out
   * @param nodeRefApproxOffset
   *          the node ref approx offset
   * @param refApproxOffset
   *          the ref approx offset
   * @return the long
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private Long storeTree(MtasTreeNode<?> node, boolean isSinglePoint,
      boolean storeAdditionalInformation, IndexOutput out,
      Long nodeRefApproxOffset, long refApproxOffset) throws IOException {
    if (node != null) {
      Boolean isRoot = false;
      if (nodeRefApproxOffset == null) {
        nodeRefApproxOffset = out.getFilePointer();
        isRoot = true;
      }
      Long fpIndexObjectPositionLeftChild, fpIndexObjectPositionRightChild;
      if (node.leftChild != null) {
        fpIndexObjectPositionLeftChild = storeTree(node.leftChild,
            isSinglePoint, storeAdditionalInformation, out, nodeRefApproxOffset,
            refApproxOffset);
      } else {
        fpIndexObjectPositionLeftChild = (long) 0; // tmp
      }
      if (node.rightChild != null) {
        fpIndexObjectPositionRightChild = storeTree(node.rightChild,
            isSinglePoint, storeAdditionalInformation, out, nodeRefApproxOffset,
            refApproxOffset);
      } else {
        fpIndexObjectPositionRightChild = (long) 0; // tmp
      }
      Long fpIndexObjectPosition = out.getFilePointer();
      if (node.leftChild == null) {
        fpIndexObjectPositionLeftChild = fpIndexObjectPosition;
      }
      if (node.rightChild == null) {
        fpIndexObjectPositionRightChild = fpIndexObjectPosition;
      }
      if (isRoot) {
        out.writeVLong(nodeRefApproxOffset);
        byte flag = 0;
        if (isSinglePoint) {
          flag |= MtasTree.SINGLE_POSITION_TREE;
        }
        if (storeAdditionalInformation) {
          flag |= MtasTree.STORE_ADDITIONAL_ID;
        }
        out.writeByte(flag);
      }
      out.writeVInt(node.left);
      out.writeVInt(node.right);
      out.writeVInt(node.max);
      out.writeVLong((fpIndexObjectPositionLeftChild - nodeRefApproxOffset));
      out.writeVLong((fpIndexObjectPositionRightChild - nodeRefApproxOffset));
      if (!isSinglePoint) {
        out.writeVInt(node.ids.size());
      }
      HashMap<Integer, MtasTreeNodeId> ids = node.ids;
      Long objectRefCorrected;
      long objectRefCorrectedPrevious = 0;
      // sort refs
      List<MtasTreeNodeId> nodeIds = new ArrayList<MtasTreeNodeId>(
          ids.values());
      Collections.sort(nodeIds);
      if (isSinglePoint && (nodeIds.size() != 1)) {
        throw new IOException("singlePoint tree, but missing single point...");
      }
      for (MtasTreeNodeId nodeId : nodeIds) {
        objectRefCorrected = (nodeId.ref - refApproxOffset);
        out.writeVLong((objectRefCorrected - objectRefCorrectedPrevious));
        objectRefCorrectedPrevious = objectRefCorrected;
        if (storeAdditionalInformation) {
          out.writeVInt(nodeId.additionalId);
          out.writeVLong(nodeId.additionalRef);
        }
      }
      return fpIndexObjectPosition;
    } else {
      return null;
    }
  }

  /**
   * Token stats add.
   *
   * @param min
   *          the min
   * @param max
   *          the max
   */
  private void tokenStatsAdd(int min, int max) {
    tokenStatsNumber++;
    if (tokenStatsMinPos == null) {
      tokenStatsMinPos = min;
    } else {
      tokenStatsMinPos = Math.min(tokenStatsMinPos, min);
    }
    if (tokenStatsMaxPos == null) {
      tokenStatsMaxPos = max;
    } else {
      tokenStatsMaxPos = Math.max(tokenStatsMaxPos, max);
    }
  }

  /**
   * Copy object and update stats.
   *
   * @param id
   *          the id
   * @param in
   *          the in
   * @param inRef
   *          the in ref
   * @param out
   *          the out
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void copyObjectAndUpdateStats(int id, IndexInput in, Long inRef,
      IndexOutput out) throws IOException {
    int mtasId, objectFlags;
    // read
    in.seek(inRef);
    mtasId = in.readVInt();
    assert id == mtasId : "wrong id detected while copying object";
    objectFlags = in.readVInt();
    out.writeVInt(mtasId);
    out.writeVInt(objectFlags);
    if ((objectFlags
        & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PARENT) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PARENT) {
      out.writeVInt(in.readVInt());
    }
    if ((objectFlags
        & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_RANGE) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_RANGE) {
      int minPos = in.readVInt();
      int maxPos = in.readVInt();
      out.writeVInt(minPos);
      out.writeVInt(maxPos);
      tokenStatsAdd(minPos, maxPos);
    } else if ((objectFlags
        & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_SET) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_POSITION_SET) {
      int size = in.readVInt();
      out.writeVInt(size);
      TreeSet<Integer> list = new TreeSet<Integer>();
      int previousPosition = 0;
      for (int t = 0; t < size; t++) {
        int pos = in.readVInt();
        out.writeVInt(pos);
        previousPosition = (pos + previousPosition);
        list.add(previousPosition);
      }
      assert list.size() == size : "duplicate positions in set are not allowed";
      tokenStatsAdd(list.first(), list.last());
    } else {
      int pos = in.readVInt();
      out.writeVInt(pos);
      tokenStatsAdd(pos, pos);
    }
    if ((objectFlags
        & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_OFFSET) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_OFFSET) {
      out.writeVInt(in.readVInt());
      out.writeVInt(in.readVInt());
    }
    if ((objectFlags
        & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_REALOFFSET) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_REALOFFSET) {
      out.writeVInt(in.readVInt());
      out.writeVInt(in.readVInt());
    }
    if ((objectFlags
        & MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PAYLOAD) == MtasCodecPostingsFormat.MTAS_OBJECT_HAS_PAYLOAD) {
      int length = in.readVInt();
      out.writeVInt(length);
      byte[] payload = new byte[length];
      in.readBytes(payload, 0, length);
      out.writeBytes(payload, payload.length);
    }
    out.writeVLong(in.readVLong());
  }

  /** The closed. */
  private boolean closed;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.codecs.FieldsConsumer#close()
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    delegateFieldsConsumer.close();
  }

}
