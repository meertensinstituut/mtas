package mtas.analysis.token;

/**
 * The Class MtasTokenString.
 */
public class MtasTokenString extends MtasToken {

  /** The Constant TOKEN_TYPE. */
  public static final String TOKEN_TYPE = "string";

  /**
   * Instantiates a new mtas token string.
   *
   * @param tokenId the token id
   * @param value the value
   */
  public MtasTokenString(Integer tokenId, String value) {
    super(tokenId, value);
  }

  /**
   * Instantiates a new mtas token string.
   *
   * @param tokenId the token id
   * @param value the value
   * @param position the position
   */
  public MtasTokenString(Integer tokenId, String value, Integer position) {
    super(tokenId, value, position);
  }

  /*
   * (non-Javadoc)
   * 
   * @see mtas.analysis.token.MtasToken#setType()
   */
  @Override
  public void setType() {
    tokenType = TOKEN_TYPE;
  }

}
