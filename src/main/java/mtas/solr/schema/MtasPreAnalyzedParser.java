package mtas.solr.schema;

import mtas.solr.update.processor.MtasUpdateRequestProcessorResultItem;
import mtas.solr.update.processor.MtasUpdateRequestProcessorResultReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.AttributeSource.State;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.PreAnalyzedField.ParseResult;
import org.apache.solr.schema.PreAnalyzedField.PreAnalyzedParser;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

/**
 * Parser that reads a temporary file name from a field and reads that file.
 * The format is: originally uploaded content, followed by tokens,
 * encoded as Java-serialized MtasUpdateRequestProcessorResultItems.
 */
public class MtasPreAnalyzedParser implements PreAnalyzedParser {
  private static Log log = LogFactory.getLog(MtasPreAnalyzedParser.class);

  @Override
  public ParseResult parse(Reader reader, AttributeSource parent)
      throws IOException {
    ParseResult res = new ParseResult();

    // get MtasUpdateRequestProcessorResult
    StringBuilder sb = new StringBuilder();
    char[] buf = new char[128];
    int cnt;
    while ((cnt = reader.read(buf)) > 0) {
      sb.append(buf, 0, cnt);
    }
    Iterator<MtasUpdateRequestProcessorResultItem> iterator;

    try (
        MtasUpdateRequestProcessorResultReader result = new MtasUpdateRequestProcessorResultReader(
          sb.toString())) {
      iterator = result.getIterator();
      if (iterator != null && iterator.hasNext()) {
        res.str = result.getStoredStringValue();
        res.bin = result.getStoredBinValue();
      } else {
        res.str = null;
        res.bin = null;
        result.close();
        return res;
      }
      parent.clearAttributes();
      while (iterator.hasNext()) {
        MtasUpdateRequestProcessorResultItem item = iterator.next();
        if (item.tokenTerm != null) {
          CharTermAttribute catt = parent.addAttribute(CharTermAttribute.class);
          catt.append(item.tokenTerm);
        }
        if (item.tokenFlags != null) {
          FlagsAttribute flags = parent.addAttribute(FlagsAttribute.class);
          flags.setFlags(item.tokenFlags);
        }
        if (item.tokenPosIncr != null) {
          PositionIncrementAttribute patt = parent
              .addAttribute(PositionIncrementAttribute.class);
          patt.setPositionIncrement(item.tokenPosIncr);
        }
        if (item.tokenPayload != null) {
          PayloadAttribute p = parent.addAttribute(PayloadAttribute.class);
          p.setPayload(new BytesRef(item.tokenPayload));
        }
        if (item.tokenOffsetStart != null && item.tokenOffsetEnd != null) {
          OffsetAttribute offset = parent.addAttribute(OffsetAttribute.class);
          offset.setOffset(item.tokenOffsetStart, item.tokenOffsetEnd);
        }
        // capture state and add to result
        State state = parent.captureState();
        res.states.add(state.clone());
        // reset for reuse
        parent.clearAttributes();
      }
    } catch (IOException e) {
      // ignore
      log.warn(e);
    }
    return res;
  }

  @Override
  public String toFormattedString(Field f) throws IOException {
    return this.getClass().getName() + " " + f.name();
  }
}
