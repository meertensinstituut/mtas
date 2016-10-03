package mtas.analysis.token;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

/**
 * The Class MtasToken.
 *
 * @param <GenericType>
 *          the generic type
 */
public abstract class MtasToken<GenericType> {

  /** The Constant DELIMITER. */
  public static final String DELIMITER = "\u0001";

  /** The Constant regexpPrePostFix. */
  public static final String regexpPrePostFix = "(.*)" + DELIMITER
      + "(.[^\u0000]*)";

  /** The Constant patternPrePostFix. */
  public static final Pattern patternPrePostFix = Pattern
      .compile(regexpPrePostFix);

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
   * @param value
   *          the value
   */
  protected MtasToken(String value) {
    setType();
    setValue(value);
  }

  /**
   * Instantiates a new mtas token.
   *
   * @param value
   *          the value
   * @param position
   *          the position
   */
  protected MtasToken(String value, Integer position) {
    setType();
    setValue(value);
    addPosition(position);
  }

  /**
   * Sets the token ref.
   *
   * @param ref
   *          the new token ref
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
   * @param ref
   *          the new term ref
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
   * @param id
   *          the new prefix id
   */
  final public void setPrefixId(int id) {
    prefixId = id;
  }

  /**
   * Gets the prefix id.
   *
   * @return the prefix id
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  final public int getPrefixId() throws IOException {
    if (prefixId != null) {
      return prefixId;
    } else {
      throw new IOException("no prefixId");
    }
  }

  /**
   * Sets the id.
   *
   * @param id
   *          the new id
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
   * @param id
   *          the new parent id
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
   * @param provide
   *          the new provide parent id
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
   * @param position
   *          the position
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
   * @param start
   *          the start
   * @param end
   *          the end
   */
  final public void addPositionRange(int start, int end) {
    if (tokenPosition == null) {
      tokenPosition = new MtasPosition(start, end);
    } else {
      int[] positions = new int[end - start + 1];
      for (int i = start; i <= end; i++) {
        positions[i - start] = i;
      }
      tokenPosition.add(positions);
    }
  }

  /**
   * Adds the positions.
   *
   * @param positions
   *          the positions
   */
  final public void addPositions(int[] positions) {
    if (positions != null && positions.length > 0) {
      if (tokenPosition == null) {
        tokenPosition = new MtasPosition(positions);
      } else {
        tokenPosition.add(positions);
      }
    }
  }

  /**
   * Adds the positions.
   *
   * @param list
   *          the list
   */
  final public void addPositions(TreeSet<Integer> list) {
    int[] positions = ArrayUtils
        .toPrimitive(list.toArray(new Integer[list.size()]));
    addPositions(positions);
  }

  /**
   * Check position type.
   *
   * @param type
   *          the type
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
  final public int[] getPositions() {
    return tokenPosition == null ? null : tokenPosition.getPositions();
  }

  /**
   * Check offset.
   *
   * @return the boolean
   */
  final public Boolean checkOffset() {
    if ((tokenOffset == null) || !provideOffset) {
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
    if ((tokenRealOffset == null) || !provideRealOffset) {
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
   * @param start
   *          the start
   * @param end
   *          the end
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
   * @param start
   *          the start
   * @param end
   *          the end
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
   * @param provide
   *          the new provide offset
   */
  final public void setProvideOffset(Boolean provide) {
    provideOffset = provide;
  }

  /**
   * Sets the real offset.
   *
   * @param start
   *          the start
   * @param end
   *          the end
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
   * @param provide
   *          the new provide real offset
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
   * @param value
   *          the new value
   */
  public void setValue(String value) {
    tokenValue = value;
  }

  /**
   * Gets the prefix from value.
   *
   * @param value
   *          the value
   * @return the prefix from value
   */
  public static String getPrefixFromValue(String value) {
    String prefix = null;
    if (value.contains(DELIMITER)) {
      prefix = value.split(DELIMITER)[0];
    } else {
      prefix = value;
    }
    return prefix.replaceAll("\u0000", "");
  }

  /**
   * Gets the postfix from value.
   *
   * @param value
   *          the value
   * @return the postfix from value
   */
  public static String getPostfixFromValue(String value) {
    String postfix = "";
    Matcher m = patternPrePostFix.matcher(value);
    if (m.find()) {
      postfix = m.group(2);

    }
    return postfix;
  }

  /**
   * Gets the postfix from value.
   *
   * @param term
   *          the term
   * @return the postfix from value
   */
  public static String getPostfixFromValue(BytesRef term) {
    int i = term.offset, length = term.offset + term.length;
    byte[] postfix = new byte[length];
    while (i < length) {
      if ((term.bytes[i] & 0b10000000) == 0b00000000) {
        if (term.bytes[i] == 0b00000001) {
          i++;
          break;
        } else {
          i++;
        }
      } else if ((term.bytes[i] & 0b11100000) == 0b11000000) {
        i += 2;
      } else if ((term.bytes[i] & 0b11110000) == 0b11100000) {
        i += 3;
      } else if ((term.bytes[i] & 0b11111000) == 0b11110000) {
        i += 4;
      } else if ((term.bytes[i] & 0b11111100) == 0b11111000) {
        i += 5;
      } else if ((term.bytes[i] & 0b11111110) == 0b11111100) {
        i += 6;
      } else {
        return "";
      }
    }
    int start = i;
    while (i < length) {
      if ((term.bytes[i] & 0b10000000) == 0b00000000) {
        if (term.bytes[i] == 0b00000000) {
          break;
        }
        postfix[i] = term.bytes[i];
        i++;
      } else if ((term.bytes[i] & 0b11100000) == 0b11000000) {
        postfix[i] = term.bytes[i];
        postfix[i + 1] = term.bytes[i + 1];
        i += 2;
      } else if ((term.bytes[i] & 0b11110000) == 0b11100000) {
        postfix[i] = term.bytes[i];
        postfix[i + 1] = term.bytes[i + 1];
        postfix[i + 2] = term.bytes[i + 2];
        i += 3;
      } else if ((term.bytes[i] & 0b11111000) == 0b11110000) {
        postfix[i] = term.bytes[i];
        postfix[i + 1] = term.bytes[i + 1];
        postfix[i + 2] = term.bytes[i + 2];
        postfix[i + 3] = term.bytes[i + 3];
        i += 4;
      } else if ((term.bytes[i] & 0b11111100) == 0b11111000) {
        postfix[i] = term.bytes[i];
        postfix[i + 1] = term.bytes[i + 1];
        postfix[i + 2] = term.bytes[i + 2];
        postfix[i + 3] = term.bytes[i + 3];
        postfix[i + 4] = term.bytes[i + 4];
        i += 5;
      } else if ((term.bytes[i] & 0b11111110) == 0b11111100) {
        postfix[i] = term.bytes[i];
        postfix[i + 1] = term.bytes[i + 1];
        postfix[i + 2] = term.bytes[i + 2];
        postfix[i + 3] = term.bytes[i + 3];
        postfix[i + 4] = term.bytes[i + 4];
        postfix[i + 5] = term.bytes[i + 5];
        i += 6;
      } else {
        return "";
      }
    }
    try {
      return new String(Arrays.copyOfRange(postfix, start, i), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return "";
    }

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
    if ((tokenParentId == null) || !provideParentId) {
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
    if (tokenPayload == null) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Sets the payload.
   *
   * @param payload
   *          the new payload
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

  /**
   * Creates the automata.
   *
   * @param prefix
   *          the prefix
   * @param valueList
   *          the value list
   * @return the list
   * @throws IOException 
   */
  public static List<CompiledAutomaton> createAutomata(String prefix,
      List<String> valueList) throws IOException {
    List<CompiledAutomaton> list = new ArrayList<CompiledAutomaton>();
    int step = 500;
    for (int i = 0; i < valueList.size(); i += step) {
      int localStep = step;
      boolean success=false;
      CompiledAutomaton compiledAutomaton = null;
      while(!success) {
        success=true;
        int next = Math.min(valueList.size(), i + localStep);
        List<Automaton> listAutomaton = new ArrayList<Automaton>();
        for (int j = i; j < next; j++) {
          String value = valueList.get(j);
          value = value.replaceAll("([\\\"\\)\\(\\<\\>\\.\\@\\#\\]\\[\\{\\}])",
              "\\\\\\1");
          listAutomaton
              .add((new RegExp(prefix + MtasToken.DELIMITER + value + "\u0000*"))
                  .toAutomaton());
        }
        Automaton automaton = Operations.union(listAutomaton);
        try {
          compiledAutomaton = new CompiledAutomaton(automaton);
        } catch (TooComplexToDeterminizeException e) {
          success=false;
          if(localStep>1) {
            localStep/=2;
          } else {
            throw new IOException("TooComplexToDeterminizeException");
          }
        }
      }  
      list.add(compiledAutomaton);
    }
    return list;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    String text = "";
    text += "[" + String.format("%05d", getId()) + "] ";
    text += ((getRealOffsetStart() == null) ? "[-------,-------]"
        : "[" + String.format("%07d", getRealOffsetStart()) + "-"
            + String.format("%07d", getRealOffsetEnd()) + "]");
    text += (provideRealOffset ? "  " : "* ");
    text += ((getOffsetStart() == null) ? "[-------,-------]"
        : "[" + String.format("%07d", getOffsetStart()) + "-"
            + String.format("%07d", getOffsetEnd()) + "]");
    text += (provideOffset ? "  " : "* ");
    if (getPositionLength() == null) {
      text += String.format("%11s", "");
    } else if (getPositionStart().equals(getPositionEnd())) {
      text += String.format("%11s", "[" + getPositionStart() + "]");
    } else if ((getPositions() == null) || (getPositions().length == (1
        + getPositionEnd() - getPositionStart()))) {
      text += String.format("%11s",
          "[" + getPositionStart() + "-" + getPositionEnd() + "]");
    } else {
      text += String.format("%11s", Arrays.toString(getPositions()));
    }
    text += ((getParentId() == null) ? "[-----]"
        : "[" + String.format("%05d", getParentId()) + "]");
    text += (provideParentId ? "  " : "* ");
    BytesRef payload = getPayload();
    text += (payload == null) ? "[------] "
        : "["
            + String
                .format("%.4f",
                    PayloadHelper.decodeFloat(Arrays.copyOfRange(payload.bytes,
                        payload.offset, (payload.offset + payload.length))))
            + "] ";
    text += String.format("%25s", "[" + getPrefix() + "]") + " ";
    text += ((getPostfix() == null) ? "---" : "[" + getPostfix() + "]") + " ";
    return text;
  }

}
