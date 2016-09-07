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

  /** The prefix. */
  private String prefix;

  /**
   * Instantiates a new mtas prefix token filter factory.
   *
   * @param args
   *          the args
   */
  public MtasPrefixTokenFilterFactory(Map<String, String> args) {
    super(args);
    prefix = get(args, "prefix");
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.analysis.util.TokenFilterFactory#create(org.apache.lucene
   * .analysis.TokenStream)
   */
  @Override
  public TokenStream create(TokenStream input) {
    return new MtasPrefixTokenFilter(input, prefix);
  }

  /**
   * The Class MtasPrefixTokenFilter.
   */
  public class MtasPrefixTokenFilter extends TokenFilter {

    /** The prefix. */
    private String prefix;

    /** The term att. */
    private final CharTermAttribute termAtt = addAttribute(
        CharTermAttribute.class);

    /**
     * Instantiates a new mtas prefix token filter.
     *
     * @param input
     *          the input
     * @param prefix
     *          the prefix
     */
    protected MtasPrefixTokenFilter(TokenStream input, String prefix) {
      super(input);
      this.prefix = prefix + MtasToken.DELIMITER;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lucene.analysis.TokenStream#incrementToken()
     */
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

  }

}
