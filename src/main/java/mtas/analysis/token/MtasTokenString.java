package mtas.analysis.token;

public class MtasTokenString extends MtasToken {
  public static final String TOKEN_TYPE = "string";

  public MtasTokenString(Integer tokenId, String value) {
    super(tokenId, value);
  }

  public MtasTokenString(Integer tokenId, String prefix, String postfix) {
    super(tokenId, prefix, postfix);
  }

  public MtasTokenString(Integer tokenId, String value, Integer position) {
    super(tokenId, value, position);
  }

  public MtasTokenString(Integer tokenId, String prefix, String postfix,
      Integer position) {
    super(tokenId, prefix, postfix, position);
  }

  @Override
  public void setType() {
    tokenType = TOKEN_TYPE;
  }

}
