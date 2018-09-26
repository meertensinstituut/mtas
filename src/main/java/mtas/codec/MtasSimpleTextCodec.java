package mtas.codec;

import org.apache.lucene.codecs.simpletext.SimpleTextCodec;

public class MtasSimpleTextCodec extends MtasCodec {
  public MtasSimpleTextCodec() {
    super("MtasSimpleTextCodec", new SimpleTextCodec());
  }
}
