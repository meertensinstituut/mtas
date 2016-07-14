package mtas.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

/**
 * The Class MtasFieldsProducer.
 */
public class MtasFieldsProducer extends FieldsProducer {

  /** The delegate fields producer. */
  private FieldsProducer delegateFieldsProducer;

  /** The index input list. */
  private HashMap<String, IndexInput> indexInputList;
  
  /** The index input offset list. */
  private HashMap<String, Long> indexInputOffsetList;
  
  /** The version. */
  private int version;

  /**
   * Instantiates a new mtas fields producer.
   *
   * @param state the state
   * @param name the name
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasFieldsProducer(SegmentReadState state, String name)
      throws IOException {
    String postingsFormatName = null;
    indexInputList = new HashMap<String, IndexInput>();
    indexInputOffsetList = new HashMap<String, Long>();
    version = MtasCodecPostingsFormat.VERSION_CURRENT;
    postingsFormatName = addIndexInputToList("object",
        openMtasFile(state, name, MtasCodecPostingsFormat.MTAS_OBJECT_EXTENSION),
        postingsFormatName);
    addIndexInputToList("term",
        openMtasFile(state, name, MtasCodecPostingsFormat.MTAS_TERM_EXTENSION),
        postingsFormatName);
    addIndexInputToList("prefix",
        openMtasFile(state, name, MtasCodecPostingsFormat.MTAS_PREFIX_EXTENSION),
        postingsFormatName);
    addIndexInputToList("field",
        openMtasFile(state, name, MtasCodecPostingsFormat.MTAS_FIELD_EXTENSION),
        postingsFormatName);
    addIndexInputToList("indexDocId",
        openMtasFile(state, name,
            MtasCodecPostingsFormat.MTAS_INDEX_DOC_ID_EXTENSION),
        postingsFormatName);
    addIndexInputToList("indexObjectId",
        openMtasFile(state, name,
            MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_ID_EXTENSION),
        postingsFormatName);    
    try {
      addIndexInputToList("doc",
          openMtasFile(state, name, MtasCodecPostingsFormat.MTAS_DOC_EXTENSION, version, version),
          postingsFormatName);
      addIndexInputToList("indexObjectPosition",
          openMtasFile(state, name,
              MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_POSITION_EXTENSION, version, version),
          postingsFormatName);
      addIndexInputToList("indexObjectParent",
          openMtasFile(state, name,
              MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_PARENT_EXTENSION, version, version),
          postingsFormatName);
    } catch (IndexFormatTooOldException e) {
      version = MtasCodecPostingsFormat.VERSION_OLD;
      addIndexInputToList("doc",
          openMtasFile(state, name, MtasCodecPostingsFormat.MTAS_DOC_EXTENSION, version, version),
          postingsFormatName);
      addIndexInputToList("indexObjectPosition",
          openMtasFile(state, name,
              MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_POSITION_EXTENSION, version, version),
          postingsFormatName);
      addIndexInputToList("indexObjectParent",
          openMtasFile(state, name,
              MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_PARENT_EXTENSION, version, version),
          postingsFormatName);
    }
    // Load the delegate postingsFormatName from this file
    this.delegateFieldsProducer = PostingsFormat.forName(postingsFormatName)
        .fieldsProducer(state);
  }

  /**
   * Adds the index input to list.
   *
   * @param name the name
   * @param in the in
   * @param postingsFormatName the postings format name
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private String addIndexInputToList(String name, IndexInput in,
      String postingsFormatName) throws IOException {
    if (postingsFormatName == null) {
      postingsFormatName = in.readString();
    } else if (!in.readString().equals(postingsFormatName)) {
      throw new IOException(
          "delegate codec " + name + " doesn't equal " + postingsFormatName);
    }
    indexInputList.put(name, in);
    indexInputOffsetList.put(name, in.getFilePointer());
    return postingsFormatName;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.index.Fields#iterator()
   */
  @Override
  public Iterator<String> iterator() {
    return delegateFieldsProducer.iterator();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.codecs.FieldsProducer#close()
   */
  @Override
  public void close() throws IOException {
    delegateFieldsProducer.close();
    for (String name : indexInputList.keySet()) {
      indexInputList.get(name).close();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.index.Fields#terms(java.lang.String)
   */
  @Override
  public Terms terms(String field) throws IOException {
    return new MtasTerms(delegateFieldsProducer.terms(field), indexInputList,
        indexInputOffsetList, version);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.index.Fields#size()
   */
  @Override
  public int size() {
    return delegateFieldsProducer.size();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.util.Accountable#ramBytesUsed()
   */
  @Override
  public long ramBytesUsed() {
    // return BASE_RAM_BYTES_USED + delegateFieldsProducer.ramBytesUsed();
    return 3 * delegateFieldsProducer.ramBytesUsed();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.util.Accountable#getChildResources()
   */
  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    if (delegateFieldsProducer != null) {
      resources.add(
          Accountables.namedAccountable("delegate", delegateFieldsProducer));
    }
    return Collections.unmodifiableList(resources);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.codecs.FieldsProducer#checkIntegrity()
   */
  @Override
  public void checkIntegrity() throws IOException {
    delegateFieldsProducer.checkIntegrity();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return getClass().getSimpleName() + "(delegate=" + delegateFieldsProducer
        + ")";
  }

  /**
   * Open mtas file.
   *
   * @param state the state
   * @param name the name
   * @param extension the extension
   * @param minimum the minimum
   * @param maximum the maximum
   * @return the index input
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private IndexInput openMtasFile(SegmentReadState state, String name,
      String extension, Integer minimum, Integer maximum) throws IOException  {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, extension);
    IndexInput object;
    try {
      object = state.directory.openInput(fileName, state.context);
    } catch (IOException e) {
      throw new IOException(e.getMessage());
    }
    int minVersion = (minimum == null) ? MtasCodecPostingsFormat.VERSION_START
        : minimum.intValue();
    int maxVersion = (maximum == null) ? MtasCodecPostingsFormat.VERSION_CURRENT
        : maximum.intValue();
    try {
      CodecUtil.checkIndexHeader(object, name, minVersion, maxVersion,
          state.segmentInfo.getId(), state.segmentSuffix);
    } catch (IndexFormatTooOldException e) {
      object.close();      
      throw new IndexFormatTooOldException(e.getMessage(),e.getVersion(), e.getMinVersion(), e.getMaxVersion());
    }    
    return object;
  }

  /**
   * Open mtas file.
   *
   * @param state the state
   * @param name the name
   * @param extension the extension
   * @return the index input
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private IndexInput openMtasFile(SegmentReadState state, String name,
      String extension) throws IOException {
    return openMtasFile(state, name, extension, null, null);
  }

}
