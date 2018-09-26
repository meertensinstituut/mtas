package mtas.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SimpleCollector;

import java.io.IOException;

public class MtasCollector extends SimpleCollector {
  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
  }

  @Override
  public void collect(int doc) throws IOException {
    // System.out.println("Mtas collector voor doc "+doc);
  }
}
