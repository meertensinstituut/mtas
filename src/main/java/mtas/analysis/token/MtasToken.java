package mtas.analysis.token;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

public abstract class MtasToken {
  private static final Log log = LogFactory.getLog(MtasToken.class);

  public static final String DELIMITER = "\u0001";
  public static final String regexpPrePostFix = "(.*)" + DELIMITER
      + "(.[^\u0000]*)";
  public static final Pattern patternPrePostFix = Pattern
      .compile(regexpPrePostFix);

  private Integer tokenId;
  private Long tokenRef = null;
  private Long termRef = null;
  private Integer prefixId = null;

  protected String tokenType = null;

  private Integer tokenParentId = null;
  private String tokenValue = null;
  private MtasPosition tokenPosition = null;
  private MtasOffset tokenOffset = null;
  private MtasOffset tokenRealOffset = null;
  private BytesRef tokenPayload = null;
  private Boolean provideOffset = true;
  private Boolean provideRealOffset = true;
  private Boolean provideParentId = true;

  protected MtasToken(Integer tokenId, String value) {
    this.tokenId = tokenId;
    setType();
    setValue(value);
  }

  protected MtasToken(Integer tokenId, String prefix, String postfix) {
    Objects.requireNonNull(prefix, "prefix is obligatory");
    this.tokenId = tokenId;
    setType();
    if (postfix != null) {
      setValue(prefix + DELIMITER + postfix);
    } else {
      setValue(prefix + DELIMITER);
    }
  }

  protected MtasToken(Integer tokenId, String value, Integer position) {
    this(tokenId, value);
    addPosition(position);
  }

  protected MtasToken(Integer tokenId, String prefix, String postfix,
      Integer position) {
    this(tokenId, prefix, postfix);
    addPosition(position);
  }

  final public void setTokenRef(Long ref) {
    tokenRef = ref;
  }

  final public Long getTokenRef() {
    return tokenRef;
  }

  final public void setTermRef(Long ref) {
    termRef = ref;
  }

  final public Long getTermRef() {
    return termRef;
  }

  final public void setPrefixId(int id) {
    prefixId = id;
  }

  final public int getPrefixId() throws IOException {
    if (prefixId != null) {
      return prefixId;
    } else {
      throw new IOException("no prefixId");
    }
  }

  final public void setId(Integer id) {
    tokenId = id;
  }

  final public Integer getId() {
    return tokenId;
  }

  final public void setParentId(Integer id) {
    tokenParentId = id;
  }

  final public Integer getParentId() {
    return tokenParentId;
  }

  final public void setProvideParentId(Boolean provide) {
    provideParentId = provide;
  }

  final public boolean getProvideParentId() {
    return provideParentId;
  }

  protected void setType() {
    throw new IllegalArgumentException("Type not implemented");
  }

  final public String getType() {
    return tokenType;
  }

  final public void addPosition(int position) {
    if (tokenPosition == null) {
      tokenPosition = new MtasPosition(position);
    } else {
      tokenPosition.add(position);
    }
  }

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

  final public void addPositions(int[] positions) {
    if (positions != null && positions.length > 0) {
      if (tokenPosition == null) {
        tokenPosition = new MtasPosition(positions);
      } else {
        tokenPosition.add(positions);
      }
    }
  }

  final public void addPositions(Set<Integer> list) {
    int[] positions = ArrayUtils
        .toPrimitive(list.toArray(new Integer[list.size()]));
    addPositions(positions);
  }

  final public Boolean checkPositionType(String type) {
    if (tokenPosition == null) {
      return false;
    } else {
      return tokenPosition.checkType(type);
    }
  }

  final public Integer getPositionStart() {
    return tokenPosition == null ? null : tokenPosition.getStart();
  }

  final public Integer getPositionEnd() {
    return tokenPosition == null ? null : tokenPosition.getEnd();
  }

  final public Integer getPositionLength() {
    return tokenPosition == null ? null : tokenPosition.getLength();
  }

  final public int[] getPositions() {
    return tokenPosition == null ? null : tokenPosition.getPositions();
  }

  final public Boolean checkOffset() {
    return !((tokenOffset == null) || !provideOffset);
  }

  final public Boolean checkRealOffset() {
    if ((tokenRealOffset == null) || !provideRealOffset) {
      return false;
    } else if (tokenOffset == null) {
      return true;
    } else {
      return !(tokenOffset.getStart() == tokenRealOffset.getStart()
        && tokenOffset.getEnd() == tokenRealOffset.getEnd());
    }
  }

  final public void setOffset(Integer start, Integer end) {
    if ((start == null) || (end == null)) {
      // do nothing
    } else if (start > end) {
      throw new IllegalArgumentException("Start offset after end offset");
    } else {
      tokenOffset = new MtasOffset(start, end);
    }
  }

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

  final public void setProvideOffset(Boolean provide) {
    provideOffset = provide;
  }

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

  final public void setProvideRealOffset(Boolean provide) {
    provideRealOffset = provide;
  }

  final public boolean getProvideOffset() {
    return provideOffset;
  }

  final public boolean getProvideRealOffset() {
    return provideRealOffset;
  }

  final public Integer getOffsetStart() {
    return tokenOffset == null ? null : tokenOffset.getStart();
  }

  final public Integer getOffsetEnd() {
    return tokenOffset == null ? null : tokenOffset.getEnd();
  }

  final public Integer getRealOffsetStart() {
    return tokenRealOffset == null ? null : tokenRealOffset.getStart();
  }

  final public Integer getRealOffsetEnd() {
    return tokenRealOffset == null ? null : tokenRealOffset.getEnd();
  }

  public void setValue(String value) {
    tokenValue = value;
  }

  public static String getPrefixFromValue(String value) {
    if (value == null) {
      return null;
    } else if (value.contains(DELIMITER)) {
      String[] list = value.split(DELIMITER);
      if (list != null && list.length > 0) {
        return list[0].replaceAll("\u0000", "");
      } else {
        return null;
      }
    } else {
      return value.replaceAll("\u0000", "");
    }
  }

  public static String getPostfixFromValue(String value) {
    String postfix = "";
    Matcher m = patternPrePostFix.matcher(value);
    if (m.find()) {
      postfix = m.group(2);

    }
    return postfix;
  }

  public static String getPostfixFromValue(BytesRef term) {
    int i = term.offset;
    int length = term.offset + term.length;
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
    return new String(Arrays.copyOfRange(postfix, start, i),
        StandardCharsets.UTF_8);
  }

  public String getValue() {
    return tokenValue;
  }

  public String getPrefix() {
    return getPrefixFromValue(tokenValue);
  }

  public String getPostfix() {
    return getPostfixFromValue(tokenValue);
  }

  final public Boolean checkParentId() {
    return !((tokenParentId == null) || !provideParentId);
  }

  final public Boolean checkPayload() {
    return tokenPayload != null;
  }

  public void setPayload(BytesRef payload) {
    tokenPayload = payload;
  }

  public BytesRef getPayload() {
    return tokenPayload;
  }

  public static Map<String, Automaton> createAutomatonMap(String prefix,
      List<String> valueList, Boolean filter) {
    HashMap<String, Automaton> automatonMap = new HashMap<>();
    if (valueList != null) {
      for (String item : valueList) {
        if (filter) {
          item = item.replaceAll("([\\\"\\)\\(\\<\\>\\.\\@\\#\\]\\[\\{\\}])",
              "\\\\$1");
        }
        automatonMap.put(item,
            new RegExp(prefix + MtasToken.DELIMITER + item + "\u0000*")
                .toAutomaton());
      }
    }
    return automatonMap;
  }

  public static Map<String, ByteRunAutomaton> byteRunAutomatonMap(
      Map<String, Automaton> automatonMap) {
    HashMap<String, ByteRunAutomaton> byteRunAutomatonMap = new HashMap<>();
    if (automatonMap != null) {
      for (Entry<String, Automaton> entry : automatonMap.entrySet()) {
        byteRunAutomatonMap.put(entry.getKey(),
            new ByteRunAutomaton(entry.getValue()));
      }
    }
    return byteRunAutomatonMap;
  }

  public static List<CompiledAutomaton> createAutomata(String prefix,
      String regexp, Map<String, Automaton> automatonMap) throws IOException {
    List<CompiledAutomaton> list = new ArrayList<>();
    Automaton automatonRegexp = null;
    if (regexp != null) {
      RegExp re = new RegExp(prefix + MtasToken.DELIMITER + regexp + "\u0000*");
      automatonRegexp = re.toAutomaton();
    }
    int step = 500;
    List<String> keyList = new ArrayList<>(automatonMap.keySet());
    for (int i = 0; i < keyList.size(); i += step) {
      int localStep = step;
      boolean success = false;
      CompiledAutomaton compiledAutomaton = null;
      while (!success) {
        success = true;
        int next = Math.min(keyList.size(), i + localStep);
        List<Automaton> listAutomaton = new ArrayList<>();
        for (int j = i; j < next; j++) {
          listAutomaton.add(automatonMap.get(keyList.get(j)));
        }
        Automaton automatonList = Operations.union(listAutomaton);
        Automaton automaton;
        if (automatonRegexp != null) {
          automaton = Operations.intersection(automatonList, automatonRegexp);
        } else {
          automaton = automatonList;
        }
        try {
          compiledAutomaton = new CompiledAutomaton(automaton);
        } catch (TooComplexToDeterminizeException e) {
          log.debug(e);
          success = false;
          if (localStep > 1) {
            localStep /= 2;
          } else {
            throw new IOException("TooComplexToDeterminizeException");
          }
        }
      }
      list.add(compiledAutomaton);
    }
    return list;
  }

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
