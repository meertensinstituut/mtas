package mtas.analysis.token;

import java.io.IOException;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.util.BytesRef;

/**
 * The Class MtasToken.
 *
 * @param <GenericType> the generic type
 */
public abstract class MtasToken<GenericType> {

  /** The Constant DELIMITER. */
  public static final String DELIMITER = "\u0001";

  /** The Constant MTAS_TOKEN_ID. */
  static final AtomicInteger MTAS_TOKEN_ID = new AtomicInteger(0);  
  
  /** The token id. */
  private Integer tokenId = MTAS_TOKEN_ID.getAndIncrement();
  
  /** The token ref. */
  private Long tokenRef = null;
  
  /** The term ref. */
  private Long termRef = null;
  
  /** The prefix id. */
  private Integer prefixId = null;
  
  /** The token type. */
  protected String tokenType = null;
  
  /** The token parent id. */
  private Integer tokenParentId = null;
  
  /** The token value. */
  private String tokenValue = null;
  
  /** The token position. */
  private MtasPosition tokenPosition = null;
  
  /** The token offset. */
  private MtasOffset tokenOffset = null;
  
  /** The token real offset. */
  private MtasOffset tokenRealOffset = null;
  
  /** The token payload. */
  private BytesRef tokenPayload = null;
  
  /** The provide offset. */
  private Boolean provideOffset = true;
  
  /** The provide real offset. */
  private Boolean provideRealOffset = true;
  
  /** The provide parent id. */
  private Boolean provideParentId = true;

  /**
   * Reset id.
   */
  public static void resetId() {
    MTAS_TOKEN_ID.set(0);
  }
  
  /**
   * Instantiates a new mtas token.
   *
   * @param value the value
   */
  protected MtasToken(String value) {
    setType();
    setValue(value);
  }
  
  /**
   * Instantiates a new mtas token.
   *
   * @param value the value
   * @param position the position
   */
  protected MtasToken(String value, Integer position) {
    setType();
    setValue(value);
    addPosition(position);
  }

  /**
   * Sets the token ref.
   *
   * @param ref the new token ref
   */
  final public void setTokenRef(Long ref) {
    tokenRef = ref;
  }

  /**
   * Gets the token ref.
   *
   * @return the token ref
   */
  final public Long getTokenRef() {
    return tokenRef;
  }
  
  /**
   * Sets the term ref.
   *
   * @param ref the new term ref
   */
  final public void setTermRef(Long ref) {
    termRef = ref;
  }

  /**
   * Gets the term ref.
   *
   * @return the term ref
   */
  final public Long getTermRef() {
    return termRef;
  }
  
  /**
   * Sets the prefix id.
   *
   * @param id the new prefix id
   */
  final public void setPrefixId(int id) {
    prefixId = id;
  }

  /**
   * Gets the prefix id.
   *
   * @return the prefix id
   * @throws IOException Signals that an I/O exception has occurred.
   */
  final public int getPrefixId() throws IOException {
    if(prefixId!=null) {
      return prefixId;
    } else {
      throw new IOException("no prefixId");
    }
  }

  /**
   * Sets the id.
   *
   * @param id the new id
   */
  final public void setId(Integer id) {
    tokenId = id;
  }

  /**
   * Gets the id.
   *
   * @return the id
   */
  final public Integer getId() {
    return tokenId;
  }

  /**
   * Sets the parent id.
   *
   * @param id the new parent id
   */
  final public void setParentId(Integer id) {
    tokenParentId = id;
  }

  /**
   * Gets the parent id.
   *
   * @return the parent id
   */
  final public Integer getParentId() {
    return tokenParentId;
  }
  
  /**
   * Sets the provide parent id.
   *
   * @param provide the new provide parent id
   */
  final public void setProvideParentId(Boolean provide) {
    provideParentId = provide;
  }
  
  /**
   * Gets the provide parent id.
   *
   * @return the provide parent id
   */
  final public boolean getProvideParentId() {
    return provideParentId;
  }

  /**
   * Sets the type.
   */
  protected void setType() {
    throw new IllegalArgumentException("Type not implemented");
  }

  /**
   * Gets the type.
   *
   * @return the type
   */
  final public String getType() {
    return tokenType;
  }

  /**
   * Adds the position.
   *
   * @param position the position
   */
  final public void addPosition(int position) {
    if (tokenPosition == null) {
      tokenPosition = new MtasPosition(position);
    } else {
      tokenPosition.add(position);
    }
  }

  /**
   * Adds the position range.
   *
   * @param start the start
   * @param end the end
   */
  final public void addPositionRange(int start, int end) {
    if (tokenPosition == null) {
      tokenPosition = new MtasPosition(start, end);
    } else {
      TreeSet<Integer> positions = new TreeSet<Integer>();
      for (int i = start; i <= end; i++) {
        positions.add(i);
      }
      tokenPosition.add(positions);
    }
  }

  /**
   * Adds the positions.
   *
   * @param positions the positions
   */
  final public void addPositions(TreeSet<Integer> positions) {
    if (positions != null && positions.size() > 0) {
      if (tokenPosition == null) {
        tokenPosition = new MtasPosition(positions);
      } else {
        tokenPosition.add(positions);
      }
    }
  }

  /**
   * Check position type.
   *
   * @param type the type
   * @return the boolean
   */
  final public Boolean checkPositionType(String type) {
    return tokenPosition == null ? false : tokenPosition.checkType(type);
  }

  /**
   * Gets the position start.
   *
   * @return the position start
   */
  final public Integer getPositionStart() {
    return tokenPosition == null ? null : tokenPosition.getStart();
  }

  /**
   * Gets the position end.
   *
   * @return the position end
   */
  final public Integer getPositionEnd() {
    return tokenPosition == null ? null : tokenPosition.getEnd();
  }

  /**
   * Gets the position length.
   *
   * @return the position length
   */
  final public Integer getPositionLength() {
    return tokenPosition == null ? null : tokenPosition.getLength();
  }

  /**
   * Gets the positions.
   *
   * @return the positions
   */
  final public TreeSet<Integer> getPositions() {
    return tokenPosition == null ? null : tokenPosition.getPositions();
  }

  /**
   * Check offset.
   *
   * @return the boolean
   */
  final public Boolean checkOffset() {
    if ((tokenOffset == null)||!provideOffset) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Check real offset.
   *
   * @return the boolean
   */
  final public Boolean checkRealOffset() {
    if ((tokenRealOffset == null)||!provideRealOffset) {
      return false;
    } else if (tokenOffset == null) {
      return true;
    } else if (tokenOffset.getStart() == tokenRealOffset.getStart()
        && tokenOffset.getEnd() == tokenRealOffset.getEnd()) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Sets the offset.
   *
   * @param start the start
   * @param end the end
   */
  final public void setOffset(Integer start, Integer end) {
    if ((start == null) || (end == null)) {
      // do nothing
    } else if (start > end) {
      throw new IllegalArgumentException("Start offset after end offset");
    } else {
      tokenOffset = new MtasOffset(start, end);
    }
  }

  /**
   * Adds the offset.
   *
   * @param start the start
   * @param end the end
   */
  final public void addOffset(Integer start, Integer end) {
    if (tokenOffset == null) {
      setOffset(start, end);
    } else if ((start == null) || (end == null)) {
      // do nothing
    } else if (start > end) {
      throw new IllegalArgumentException("Start offset after end offset");
    } else {
      tokenOffset.add(start, end);
    }
  }
  
  /**
   * Sets the provide offset.
   *
   * @param provide the new provide offset
   */
  final public void setProvideOffset(Boolean provide) {
    provideOffset = provide;
  }

  /**
   * Sets the real offset.
   *
   * @param start the start
   * @param end the end
   */
  final public void setRealOffset(Integer start, Integer end) {
    if ((start == null) || (end == null)) {
      // do nothing
    } else if (start > end) {
      throw new IllegalArgumentException(
          "Start real offset after end real offset");
    } else {
      tokenRealOffset = new MtasOffset(start, end);
    }
  }
  
 
  /**
   * Sets the provide real offset.
   *
   * @param provide the new provide real offset
   */
  final public void setProvideRealOffset(Boolean provide) {
    provideRealOffset = provide;
  }
  
  /**
   * Gets the provide offset.
   *
   * @return the provide offset
   */
  final public boolean getProvideOffset() {
    return provideOffset;
  }
  
  /**
   * Gets the provide real offset.
   *
   * @return the provide real offset
   */
  final public boolean getProvideRealOffset() {
    return provideRealOffset;
  }

  /**
   * Gets the offset start.
   *
   * @return the offset start
   */
  final public Integer getOffsetStart() {
    return tokenOffset == null ? null : tokenOffset.getStart();
  }

  /**
   * Gets the offset end.
   *
   * @return the offset end
   */
  final public Integer getOffsetEnd() {
    return tokenOffset == null ? null : tokenOffset.getEnd();
  }

  /**
   * Gets the real offset start.
   *
   * @return the real offset start
   */
  final public Integer getRealOffsetStart() {
    return tokenRealOffset == null ? null : tokenRealOffset.getStart();
  }

  /**
   * Gets the real offset end.
   *
   * @return the real offset end
   */
  final public Integer getRealOffsetEnd() {
    return tokenRealOffset == null ? null : tokenRealOffset.getEnd();
  }

  /**
   * Sets the value.
   *
   * @param value the new value
   */
  public void setValue(String value) {
    tokenValue = value;   
  }
  
  /**
   * Gets the prefix from value.
   *
   * @param value the value
   * @return the prefix from value
   */
  public static String getPrefixFromValue(String value) {
    String prefix = null;
    if(value.contains(DELIMITER)) {
      prefix = value.split(DELIMITER)[0];
    } else {
      prefix = value;
    }
    return prefix.replaceAll("\u0000", "");
  }
  
  /**
   * Gets the postfix from value.
   *
   * @param value the value
   * @return the postfix from value
   */
  public static String getPostfixFromValue(String value) {
    String postfix = "";
    if(value.contains(DELIMITER)) {
      String[] list = value.split(DELIMITER);
      postfix = StringUtils.join(Arrays.copyOfRange(list,1,list.length),DELIMITER);
    } 
    return postfix.replaceAll("\u0000", "");
  }

  /**
   * Gets the value.
   *
   * @return the value
   */
  public String getValue() {
    return tokenValue;
  }
  
  /**
   * Gets the prefix.
   *
   * @return the prefix
   */
  public String getPrefix() {
    return getPrefixFromValue(tokenValue);
  }
  
  /**
   * Gets the postfix.
   *
   * @return the postfix
   */
  public String getPostfix() {
    return getPostfixFromValue(tokenValue);
  }

  /**
   * Check parent id.
   *
   * @return the boolean
   */
  final public Boolean checkParentId() {
    if ((tokenParentId==null)||!provideParentId) {
      return false;
    } else {
      return true;
    }
  }
  
  /**
   * Check payload.
   *
   * @return the boolean
   */
  final public Boolean checkPayload() {
    if (tokenPayload  == null) {
      return false;
    } else {
      return true;
    }
  }
  
  /**
   * Sets the payload.
   *
   * @param payload the new payload
   */
  public void setPayload(BytesRef payload) {
    tokenPayload = payload;
  }

  /**
   * Gets the payload.
   *
   * @return the payload
   */
  public BytesRef getPayload() {
    return tokenPayload;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    String text = "";
    text+="[" + String.format("%05d", getId()) + "] ";
    text+=((getRealOffsetStart() == null) ? "[-------,-------]"
            : "[" + String.format("%07d", getRealOffsetStart()) + "-"
                + String.format("%07d", getRealOffsetEnd()) + "]");
    text+=(provideRealOffset?"  ":"* ");
    text+=((getOffsetStart() == null) ? "[-------,-------]"
        : "[" + String.format("%07d", getOffsetStart()) + "-"
            + String.format("%07d", getOffsetEnd()) + "]");
    text+=(provideOffset?"  ":"* ");
    if (getPositionLength() == null) {
      text+=String.format("%11s", "");
    } else if (getPositionStart().equals(getPositionEnd())) {
      text+=String.format("%11s", "[" + getPositionStart()
          + "]");
    } else if ((getPositions() == null)
        || (getPositions().size() == (1 + getPositionEnd() -getPositionStart()))) {
      text+=String.format("%11s", "[" + getPositionStart()
          + "-" + getPositionEnd() + "]");
    } else {
      text+=String.format("%11s", getPositions());
    }
    text+=((getParentId() == null) ? "[-----]" : "["
        + String.format("%05d", getParentId()) + "]");
    text+=(provideParentId?"  ":"* ");    
    BytesRef payload = getPayload();
    text+=(payload == null) ? "[------] " : "["
        + String.format("%.4f",
            PayloadHelper.decodeFloat(Arrays.copyOfRange(payload.bytes, payload.offset, (payload.offset+payload.length)))) + "] ";
    text+=String.format("%25s", "[" + getPrefix() + "]") + " ";
    text+=((getPostfix()==null)?"---":"[" + getPostfix() + "]") + " ";
    return text;
  }

}
