package mtas.codec;

import org.apache.lucene.codecs.simpletext.SimpleTextCodec;

/**
 * The Class MtasSimpleTextCodec.
 */
public class MtasSimpleTextCodec extends MtasCodec {

  /** The Constant MTAS_CODEC_NAME. */
  public static final String MTAS_CODEC_NAME = "MtasSimpleTextCodec";

  /**
   * Instantiates a new mtas simple text codec.
   */
  public MtasSimpleTextCodec() {
    super(MTAS_CODEC_NAME, new SimpleTextCodec());
  }
}
