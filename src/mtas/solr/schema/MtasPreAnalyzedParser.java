package mtas.solr.schema;

import java.io.IOException;
import java.io.Reader;
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
                
    Integer numberOfTokens =  result.getTokenNumber();
    parent.clearAttributes();
    for(int i=0; i<numberOfTokens; i++) {
      String tokenTerm = result.getTokenTerm(i);
      Integer tokenFlags = result.getTokenFlag(i);
      Integer tokenPosIncr = result.getTokenPosIncr(i);
      Integer tokenOffsetStart = result.getTokenOffsetStart(i);
      Integer tokenOffsetEnd = result.getTokenOffsetEnd(i);
      byte[] tokenPayload = result.getTokenPayload(i);
      if(tokenTerm!=null) {
        CharTermAttribute catt = parent.addAttribute(CharTermAttribute.class);
        catt.append(tokenTerm);
      }  
      if(tokenFlags!=null) {
        FlagsAttribute flags = parent.addAttribute(FlagsAttribute.class);
        flags.setFlags(tokenFlags);
      }
      if(tokenPosIncr!=null) {
        PositionIncrementAttribute patt = parent.addAttribute(PositionIncrementAttribute.class);
        patt.setPositionIncrement(tokenPosIncr);
      }  
      if(tokenPayload!=null) {
        PayloadAttribute p = parent.addAttribute(PayloadAttribute.class);
        p.setPayload(new BytesRef(tokenPayload));
      }
      if(tokenOffsetStart!=null && tokenOffsetEnd!=null) {
        OffsetAttribute offset = parent.addAttribute(OffsetAttribute.class);
        offset.setOffset(tokenOffsetStart, tokenOffsetEnd);
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
