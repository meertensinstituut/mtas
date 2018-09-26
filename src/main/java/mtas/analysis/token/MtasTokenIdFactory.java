package mtas.analysis.token;

public final class MtasTokenIdFactory {
  Integer tokenId;

  public MtasTokenIdFactory() {
    tokenId = 0;
  }

  public Integer createTokenId() {
    return tokenId++;
  }
}
