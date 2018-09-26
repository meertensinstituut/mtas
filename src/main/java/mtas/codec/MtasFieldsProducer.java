package mtas.codec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

public class MtasFieldsProducer extends FieldsProducer {
  private static final Log log = LogFactory.getLog(MtasFieldsProducer.class);
  private FieldsProducer delegateFieldsProducer;
  private HashMap<String, IndexInput> indexInputList;
  private HashMap<String, Long> indexInputOffsetList;
  private int version;

  public MtasFieldsProducer(SegmentReadState state, String name)
      throws IOException {
    String postingsFormatName = null;
    indexInputList = new HashMap<>();
    indexInputOffsetList = new HashMap<>();
    version = MtasCodecPostingsFormat.VERSION_CURRENT;    
    postingsFormatName = addIndexInputToList("object", openMtasFile(state, name,
        MtasCodecPostingsFormat.MTAS_OBJECT_EXTENSION), postingsFormatName);
    addIndexInputToList("term",
        openMtasFile(state, name, MtasCodecPostingsFormat.MTAS_TERM_EXTENSION),
        postingsFormatName);
    addIndexInputToList("prefix", openMtasFile(state, name,
        MtasCodecPostingsFormat.MTAS_PREFIX_EXTENSION), postingsFormatName);
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
      addIndexInputToList(
          "doc", openMtasFile(state, name,
              MtasCodecPostingsFormat.MTAS_DOC_EXTENSION, version, version),
          postingsFormatName);
      addIndexInputToList("indexObjectPosition",
          openMtasFile(state, name,
              MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_POSITION_EXTENSION,
              version, version),
          postingsFormatName);
      addIndexInputToList("indexObjectParent",
          openMtasFile(state, name,
              MtasCodecPostingsFormat.MTAS_INDEX_OBJECT_PARENT_EXTENSION,
              version, version),
          postingsFormatName);
    } catch (IndexFormatTooOldException e) {
      log.debug(e);
      throw new IOException(
          "This MTAS doesn't support your index version, please upgrade");
    }
    // Load the delegate postingsFormatName from this file
    this.delegateFieldsProducer = PostingsFormat.forName(postingsFormatName)
        .fieldsProducer(state);      
  }

  private String addIndexInputToList(String name, IndexInput in,
      String postingsFormatName) throws IOException {
    if (indexInputList.get(name) != null) {
      indexInputList.get(name).close();
    }
    if (in != null) {
      String localPostingsFormatName = postingsFormatName;
      if (localPostingsFormatName == null) {
        localPostingsFormatName = in.readString();
      } else if (!in.readString().equals(localPostingsFormatName)) {
        throw new IOException("delegate codec " + name + " doesn't equal "
            + localPostingsFormatName);
      }
      indexInputList.put(name, in);
      indexInputOffsetList.put(name, in.getFilePointer());
      return localPostingsFormatName;
    } else {
      log.debug("no " + name + " registered");
      return null;
    }
  }

  @Override
  public Iterator<String> iterator() {
    return delegateFieldsProducer.iterator();
  }

  @Override
  public void close() throws IOException {
    delegateFieldsProducer.close();
    for (Entry<String, IndexInput> entry : indexInputList.entrySet()) {
      entry.getValue().close();
    }
  }

  @Override
  public Terms terms(String field) throws IOException {
    return new MtasTerms(delegateFieldsProducer.terms(field), indexInputList,
        indexInputOffsetList, version);
  }

  @Override
  public int size() {
    return delegateFieldsProducer.size();
  }

  @Override
  public long ramBytesUsed() {
    // return BASE_RAM_BYTES_USED + delegateFieldsProducer.ramBytesUsed();
    return 3 * delegateFieldsProducer.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    if (delegateFieldsProducer != null) {
      resources.add(
          Accountables.namedAccountable("delegate", delegateFieldsProducer));
    }
    return Collections.unmodifiableList(resources);
  }

  @Override
  public void checkIntegrity() throws IOException {
    delegateFieldsProducer.checkIntegrity();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(delegate=" + delegateFieldsProducer
        + ")";
  }

  private IndexInput openMtasFile(SegmentReadState state, String name,
      String extension, Integer minimum, Integer maximum) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, extension);
    IndexInput object;
    try {
      object = state.directory.openInput(fileName, state.context);
    } catch (FileNotFoundException | NoSuchFileException e) {
      log.debug(e);
      // throw new NoSuchFileException(e.getMessage());
      return null;
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
      log.debug(e);
      throw new IndexFormatTooOldException(e.getMessage(), e.getVersion(),
          e.getMinVersion(), e.getMaxVersion());
    } catch (EOFException e) {
      object.close();
      log.debug(e);
      // throw new EOFException(e.getMessage());
      return null;
    }
    return object;
  }

  private IndexInput openMtasFile(SegmentReadState state, String name,
      String extension) throws IOException {
    return openMtasFile(state, name, extension, null, null);
  }
}
