package mtas.analysis.util;

import java.io.IOException;
import java.util.Map;
import mtas.analysis.token.MtasToken;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * A factory for creating MtasPrefixTokenFilter objects.
 */
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

  private static class MtasPrefixTokenFilter extends TokenFilter {
    private String prefix;

    private final CharTermAttribute termAtt = addAttribute(
        CharTermAttribute.class);

    protected MtasPrefixTokenFilter(TokenStream input, String prefix) {
      super(input);
      this.prefix = prefix + MtasToken.DELIMITER;
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        int oldLen = termAtt.length();
        char[] buffer = termAtt.resizeBuffer(oldLen + prefix.length());

        for (int i = 0; i < oldLen; i++) {
          buffer[(oldLen + prefix.length() - 1 - i)] = buffer[(oldLen - 1 - i)];
        }
        for (int i = 0; i < prefix.length(); i++) {
          buffer[i] = prefix.charAt(i);
        }
        termAtt.copyBuffer(buffer, 0, oldLen + prefix.length());
        return true;
      } else {
        return false;
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      final MtasPrefixTokenFilter that = (MtasPrefixTokenFilter) obj;
      return prefix.equals(that.prefix) && super.equals(that);
    }

    @Override
    public int hashCode() {
      int h = this.getClass().getSimpleName().hashCode();
      h = (h * 7) ^ prefix.hashCode();
      return h;
    }

  }
}
