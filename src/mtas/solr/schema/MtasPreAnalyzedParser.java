package mtas.solr.schema;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.AttributeSource.State;
import org.apache.solr.schema.PreAnalyzedField.ParseResult;
import org.apache.solr.schema.PreAnalyzedField.PreAnalyzedParser;
import mtas.solr.update.processor.MtasUpdateRequestProcessorResult;

/**
 * The Class MtasPreAnalyzedParser.
 */
public class MtasPreAnalyzedParser implements PreAnalyzedParser {

  /* (non-Javadoc)
   * @see org.apache.solr.schema.PreAnalyzedField.PreAnalyzedParser#parse(java.io.Reader, org.apache.lucene.util.AttributeSource)
   */
  @Override
  public ParseResult parse(Reader reader, AttributeSource parent)
      throws IOException {
    ParseResult res = new ParseResult();
    //get MtasUpdateRequestProcessorResult    
    StringBuilder sb = new StringBuilder();
    char[] buf = new char[128];
    int cnt;
    while ((cnt = reader.read(buf)) > 0) {
      sb.append(buf, 0, cnt);
    }
    MtasUpdateRequestProcessorResult result;
    try {
      result = MtasUpdateRequestProcessorResult.fromString(sb.toString());
      if(result!=null) {
        res.str = result.getStoredStringValue();
        res.bin = result.getStoredBinValue();
      } else {
        res.str = sb.toString();
        res.bin = null;
        return res;
      }
    } catch (ClassNotFoundException e) {
      return null;
    }    
                
    List<Map<String,Object>>tokens = result.getTokens();
    parent.clearAttributes();
    for(Map<String,Object> item : tokens) {
      if(item.containsKey(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.TERM_KEY)) {
        CharTermAttribute catt = parent.addAttribute(CharTermAttribute.class);
        catt.append((String) item.get(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.TERM_KEY));
      }  
      if(item.containsKey(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.FLAGS_KEY)) {
        FlagsAttribute flags = parent.addAttribute(FlagsAttribute.class);
        flags.setFlags((int) item.get(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.FLAGS_KEY));
      }
      if(item.containsKey(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.POSINCR_KEY)) {
        PositionIncrementAttribute patt = parent.addAttribute(PositionIncrementAttribute.class);
        patt.setPositionIncrement((int) item.get(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.POSINCR_KEY));
      }  
      if(item.containsKey(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.PAYLOAD_KEY)) {
        PayloadAttribute p = parent.addAttribute(PayloadAttribute.class);
        p.setPayload(new BytesRef((byte[]) item.get(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.PAYLOAD_KEY)));
      }
      if(item.containsKey(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.OFFSET_START_KEY) && item.containsKey(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.OFFSET_END_KEY)) {
        OffsetAttribute offset = parent.addAttribute(OffsetAttribute.class);
        offset.setOffset((int) item.get(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.OFFSET_START_KEY), (int) item.get(mtas.solr.update.processor.MtasUpdateRequestProcessorResult.OFFSET_END_KEY));
      }
      // capture state and add to result
      State state = parent.captureState();
      res.states.add(state.clone());
      // reset for reuse
      parent.clearAttributes();      
    }

    return res;
  }

  /* (non-Javadoc)
   * @see org.apache.solr.schema.PreAnalyzedField.PreAnalyzedParser#toFormattedString(org.apache.lucene.document.Field)
   */
  @Override
  public String toFormattedString(Field f) throws IOException {
    return this.getClass().getName()+" "+f.name();
  }

}
