package mtas.analysis.util;

import java.io.IOException;
import java.util.Map;
import mtas.analysis.token.MtasToken;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;

public class MtasPrefixTokenFilterFactory extends TokenFilterFactory {

  private String prefix;

  public MtasPrefixTokenFilterFactory(Map<String, String> args) {
    super(args);
    prefix = get(args, "prefix");
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new MtasPrefixTokenFilter(input, prefix);
  }

  
  public class MtasPrefixTokenFilter extends TokenFilter {

    private String prefix;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    protected MtasPrefixTokenFilter(TokenStream input, String prefix) {
      super(input);
      this.prefix = prefix+MtasToken.DELIMITER;
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        int oldLen = termAtt.length();  
        char [] buffer = termAtt.resizeBuffer(oldLen + prefix.length());
        
        for (int i = 0; i < oldLen; i++) {  
          buffer[(oldLen + prefix.length() - 1 - i)] = buffer[(oldLen - 1 - i)];  
        }  
        for(int i=0;i< prefix.length();i++) {
          buffer[i]=prefix.charAt(i);
        }
        termAtt.copyBuffer(buffer, 0, oldLen + prefix.length());  
        return true;
      } else {
        return false;
      }
    }

  }


}
