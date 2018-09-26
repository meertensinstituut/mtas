package mtas.parser.function.util;

import mtas.codec.util.CodecUtil;
import mtas.parser.function.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashSet;

public abstract class MtasFunctionParserFunction {
  private static Log log = LogFactory.getLog(MtasFunctionParserFunction.class);

  protected MtasFunctionParserFunction[] parserDoubles;
  protected MtasFunctionParserFunction[] parserLongs;
  protected Double[] constantDoubles;
  protected long[] constantLongs;
  protected String dataType = null;
  protected boolean sumRule = false;
  protected boolean needPositions = false;
  protected Integer degree = null;
  protected HashSet<Integer> needArgument = new HashSet<Integer>();

  private boolean defined = false;

  public final MtasFunctionParserFunctionResponse getResponse(long[] args, long n) {
    if (dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
      try {
        long l = getValueLong(args, n);
        return new MtasFunctionParserFunctionResponseLong(l, true);
      } catch (IOException e) {
        log.debug(e);
        return new MtasFunctionParserFunctionResponseLong(0, false);
      }
    } else if (dataType.equals(CodecUtil.DATA_TYPE_DOUBLE)) {
      try {
        double d = getValueDouble(args, n);
        return new MtasFunctionParserFunctionResponseDouble(d, true);
      } catch (IOException e) {
        log.debug(e);
        return new MtasFunctionParserFunctionResponseDouble(0, false);
      }
    } else {
      return null;
    }
  }

  public abstract double getValueDouble(long[] args, long n) throws IOException;

  public abstract long getValueLong(long[] args, long n) throws IOException;

  public void close() throws ParseException {
    defined = true;
  }

  public final String getType() {
    return dataType;
  }

  public final Boolean sumRule() {
    return sumRule;
  }

  public final Boolean needPositions() {
    return needPositions;
  }

  public final Boolean needArgument(int i) {
    return needArgument.contains(i);
  }

  public final int needArgumentsNumber() {
    int number = 0;
    for (int i : needArgument) {
      number = Math.max(number, (i + 1));
    }
    return number;
  }

  public final HashSet<Integer> needArgument() {
    return needArgument;
  }

  protected final boolean defined() {
    return defined;
  }
}
