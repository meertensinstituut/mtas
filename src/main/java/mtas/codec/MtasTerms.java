package mtas.codec;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class MtasTerms extends Terms {
  HashMap<String, IndexInput> indexInputList;
  HashMap<String, Long> indexInputOffsetList;
  int version;
  Terms delegateTerms;

  public MtasTerms(Terms terms, HashMap<String, IndexInput> indexInputList,
      HashMap<String, Long> indexInputOffsetList, int version) {
    delegateTerms = terms;
    this.indexInputList = indexInputList;
    this.indexInputOffsetList = indexInputOffsetList;
    this.version = version;
  }

  @Override
  public TermsEnum iterator() throws IOException {
    if (delegateTerms != null) {
      return delegateTerms.iterator();
    } else {
      return TermsEnum.EMPTY;
    }
  }

  @Override
  public long size() throws IOException {
    if (delegateTerms != null) {
      return delegateTerms.size();
    } else {
      return -1;
    }
  }

  @Override
  public long getSumTotalTermFreq() throws IOException {
    if (delegateTerms != null) {
      return delegateTerms.getSumTotalTermFreq();
    } else {
      return -1;
    }
  }

  @Override
  public long getSumDocFreq() throws IOException {
    if (delegateTerms != null) {
      return delegateTerms.getSumDocFreq();
    } else {
      return -1;
    }
  }

  @Override
  public int getDocCount() throws IOException {
    if (delegateTerms != null) {
      return delegateTerms.getDocCount();
    } else {
      return -1;
    }
  }

  @Override
  public boolean hasFreqs() {
    if (delegateTerms != null) {
      return delegateTerms.hasFreqs();
    } else {
      return false;
    }
  }

  @Override
  public boolean hasOffsets() {
    if (delegateTerms != null) {
      return delegateTerms.hasOffsets();
    } else {
      return false;
    }
  }

  @Override
  public boolean hasPositions() {
    if (delegateTerms != null) {
      return delegateTerms.hasPositions();
    } else {
      return false;
    }
  }

  @Override
  public boolean hasPayloads() {
    if (delegateTerms != null) {
      return delegateTerms.hasPayloads();
    } else {
      return false;
    }
  }

  public int getVersion() {
    return version;
  }

  public HashMap<String, IndexInput> getIndexInputList() {
    HashMap<String, IndexInput> clonedIndexInputList = new HashMap<String, IndexInput>();
    for (Entry<String, IndexInput> entry : indexInputList.entrySet()) {
      clonedIndexInputList.put(entry.getKey(), entry.getValue().clone());
    }
    return clonedIndexInputList;
  }

  public HashMap<String, Long> getIndexInputOffsetList() {
    return indexInputOffsetList;
  }
}
