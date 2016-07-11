package mtas.analysis.token;

// TODO: Auto-generated Javadoc
/**
 * The Class MtasTokenString.
 */
public class MtasTokenString extends MtasToken<String>  {

  /** The Constant TOKEN_TYPE. */
  public static final String TOKEN_TYPE = "string";
  
  /**
   * Instantiates a new mtas token string.
   *
   * @param value the value
   */
  public MtasTokenString(String value) {
    super(value);    
  }

  /**
   * Instantiates a new mtas token string.
   *
   * @param value the value
   * @param position the position
   */
  public MtasTokenString(String value, Integer position) {
    super(value, position);    
  }
  
  /* (non-Javadoc)
   * @see mtas.analysis.token.MtasToken#setType()
   */
  @Override
  public void setType() {
    tokenType = TOKEN_TYPE;
  }  
  	
  
}
