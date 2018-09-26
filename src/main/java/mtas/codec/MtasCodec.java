package mtas.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

public class MtasCodec extends Codec {
  public static final String MTAS_CODEC_NAME = "MtasCodec";

  Codec delegate;

  public MtasCodec() {
    super(MTAS_CODEC_NAME);
    delegate = null;
  }

  protected MtasCodec(String name, Codec delegate) {
    super(name);
    this.delegate = delegate;
  }

  private void initDelegate() {
    if (delegate == null) {
      delegate = Codec.getDefault();
    }
  }

  @Override
  public PostingsFormat postingsFormat() {
    initDelegate();
    if (delegate.postingsFormat() instanceof PerFieldPostingsFormat) {
      Codec defaultCodec = Codec.getDefault();
      PostingsFormat defaultPostingsFormat = defaultCodec.postingsFormat();
      if (defaultPostingsFormat instanceof PerFieldPostingsFormat) {
        defaultPostingsFormat = ((PerFieldPostingsFormat) defaultPostingsFormat)
            .getPostingsFormatForField(null);
        if ((defaultPostingsFormat == null)
            || (defaultPostingsFormat instanceof PerFieldPostingsFormat)) {
          // fallback option
          return new MtasCodecPostingsFormat(
              PostingsFormat.forName("Lucene70"));
        } else {
          return new MtasCodecPostingsFormat(defaultPostingsFormat);
        }
      } else {
        return new MtasCodecPostingsFormat(defaultPostingsFormat);
      }
    } else {
      return new MtasCodecPostingsFormat(delegate.postingsFormat());
    }
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    initDelegate();
    return delegate.docValuesFormat();
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    initDelegate();
    return delegate.storedFieldsFormat();
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    initDelegate();
    return delegate.termVectorsFormat();
  }

  @Override
  public FieldInfosFormat fieldInfosFormat() {
    initDelegate();
    return delegate.fieldInfosFormat();
  }

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    initDelegate();
    return delegate.segmentInfoFormat();
  }

  @Override
  public NormsFormat normsFormat() {
    initDelegate();
    return delegate.normsFormat();
  }

  @Override
  public LiveDocsFormat liveDocsFormat() {
    initDelegate();
    return delegate.liveDocsFormat();
  }

  @Override
  public CompoundFormat compoundFormat() {
    initDelegate();
    return delegate.compoundFormat();
  }

  @Override
  public PointsFormat pointsFormat() {
    initDelegate();
    return delegate.pointsFormat();
  }
}
