package mtas.parser.function.util;

import java.io.IOException;
import java.util.ArrayList;

import mtas.codec.util.CodecUtil;
import mtas.parser.function.ParseException;

public class MtasFunctionParserFunctionBasic
    extends MtasFunctionParserFunction {

  private String firstType;
  private int firstId;

  private ArrayList<MtasFunctionParserFunction> tmpParserLongs = new ArrayList<MtasFunctionParserFunction>();
  private ArrayList<MtasFunctionParserFunction> tmpParserDoubles = new ArrayList<MtasFunctionParserFunction>();
  private ArrayList<Long> tmpConstantLongs = new ArrayList<Long>();
  private ArrayList<Double> tmpConstantDoubles = new ArrayList<Double>();

  private int number;
  private String[] operatorList;
  private String[] typeList;
  private int[] idList;

  private ArrayList<String> tmpOperatorList = new ArrayList<String>();
  private ArrayList<String> tmpTypeList = new ArrayList<String>();
  private ArrayList<Integer> tmpIdList = new ArrayList<Integer>();

  public final static String BASIC_OPERATOR_ADD = "add";
  public final static String BASIC_OPERATOR_SUBTRACT = "subtract";
  public final static String BASIC_OPERATOR_MULTIPLY = "multiply";
  public final static String BASIC_OPERATOR_DIVIDE = "divide";
  public final static String BASIC_OPERATOR_POWER = "power";

  public MtasFunctionParserFunctionBasic(MtasFunctionParserItem item)
      throws ParseException {
    sumRule=true;
    String type = item.getType();
    MtasFunctionParserFunction parser;
    firstType = type;
    switch (type) {
    case MtasFunctionParserItem.TYPE_N:
      firstId = 0;
      dataType = CodecUtil.DATA_TYPE_LONG;
      needPositions = true;
      break;
    case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
      firstId = tmpConstantLongs.size();
      dataType = CodecUtil.DATA_TYPE_LONG;
      tmpConstantLongs.add(item.getValueLong());
      break;
    case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
      firstId = tmpConstantDoubles.size();
      dataType = CodecUtil.DATA_TYPE_DOUBLE;
      tmpConstantDoubles.add(item.getValueDouble());
      break;
    case MtasFunctionParserItem.TYPE_ARGUMENT:
      firstType = type;
      firstId = item.getId();
      dataType = CodecUtil.DATA_TYPE_LONG;
      needArgument.add(item.getId());
      break;
    case MtasFunctionParserItem.TYPE_PARSER_LONG:
      parser = item.getParser();
      parser.close();
      if (parser.getType().equals(CodecUtil.DATA_TYPE_LONG)) {
        firstId = tmpParserLongs.size();
        tmpParserLongs.add(parser);
        sumRule = parser.sumRule();
        dataType = CodecUtil.DATA_TYPE_LONG;
        needPositions = needPositions?needPositions:parser.needPositions();
        needArgument.addAll(parser.needArgument);
      } else {
        throw new ParseException("incorrect dataType");
      }
      break;
    case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
      parser = item.getParser();
      parser.close();
      if (parser.getType().equals(CodecUtil.DATA_TYPE_DOUBLE)) {
        firstId = tmpParserDoubles.size();
        tmpParserDoubles.add(parser);
        sumRule = parser.sumRule();
        dataType = CodecUtil.DATA_TYPE_DOUBLE;
        needPositions = needPositions?needPositions:parser.needPositions();
        needArgument.addAll(parser.needArgument);
      } else {
        throw new ParseException("incorrect dataType");
      }
      break;
    default:
      throw new ParseException("unknown type");
    }    
  }

  @Override
  public void close() throws ParseException {
    if (!defined()) {
      super.close();
      if (tmpParserLongs.size() > 0) {
        parserLongs = new MtasFunctionParserFunction[tmpParserLongs
            .size()];
        parserLongs = tmpParserLongs.toArray(parserLongs);
      }
      if (tmpParserDoubles.size() > 0) {
        parserDoubles = new MtasFunctionParserFunction[tmpParserDoubles.size()];
        parserDoubles = tmpParserDoubles.toArray(parserDoubles);
      }
      if (tmpConstantLongs.size() > 0) {
        constantLongs = new long[tmpConstantLongs.size()];
        for (int i = 0; i < tmpConstantLongs.size(); i++) {
          constantLongs[i] = tmpConstantLongs.get(i);
        }
      }
      if (tmpConstantDoubles.size() > 0) {
        constantDoubles = new Double[tmpConstantDoubles.size()];
        for (int i = 0; i < tmpConstantDoubles.size(); i++) {
          constantDoubles[i] = tmpConstantDoubles.get(i);
        }
      }
      if (firstType == null) {
        throw new ParseException("incorrect definition: no firstType");
      }
      if (tmpOperatorList.size() > 0) {
        number = tmpOperatorList.size();
        if ((tmpTypeList.size() != number) || (tmpIdList.size() != number)) {
          throw new ParseException("incorrect definition additional items");
        } else {
          operatorList = new String[number];
          operatorList = tmpOperatorList.toArray(operatorList);
          typeList = new String[number];
          typeList = tmpTypeList.toArray(typeList);
          idList = new int[number];
          for (int i = 0; i < number; i++) {
            idList[i] = tmpIdList.get(i).intValue();
          }          
        }
      } else {
        number = 0;
        operatorList = null;
        typeList = null;
        idList = null;
      }
    }
  }

  public void add(MtasFunctionParserItem item) throws ParseException {
    basic(BASIC_OPERATOR_ADD, item);
  }

  public void subtract(MtasFunctionParserItem item) throws ParseException {
    basic(BASIC_OPERATOR_SUBTRACT, item);
  }

  public void multiply(MtasFunctionParserItem item) throws ParseException {
    basic(BASIC_OPERATOR_MULTIPLY, item);
  }

  public void divide(MtasFunctionParserItem item) throws ParseException {
    basic(BASIC_OPERATOR_DIVIDE, item);
  }

  public void power(MtasFunctionParserItem item) throws ParseException {
    basic(BASIC_OPERATOR_POWER, item);
  }

  private void basic(String operator, MtasFunctionParserItem item)
      throws ParseException {
    if (!defined()) {
      String type = item.getType();
      MtasFunctionParserFunction parser;
      tmpOperatorList.add(operator);
      if (operator.equals(BASIC_OPERATOR_DIVIDE)) {
        dataType = CodecUtil.DATA_TYPE_DOUBLE;
      }
      sumRule = sumRule?operator.equals(BASIC_OPERATOR_ADD)||operator.equals(BASIC_OPERATOR_SUBTRACT):false;
      switch (type) {
      case MtasFunctionParserItem.TYPE_N:
        tmpTypeList.add(type);
        tmpIdList.add(0);
        needPositions = true;
        break;
      case MtasFunctionParserItem.TYPE_ARGUMENT:
        tmpTypeList.add(type);
        tmpIdList.add(item.getId());
        needArgument.add(item.getId());
        break;
      case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
        tmpTypeList.add(type);
        tmpIdList.add(tmpConstantLongs.size());
        tmpConstantLongs.add(item.getValueLong());
        break;
      case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
        tmpTypeList.add(type);
        tmpIdList.add(tmpConstantDoubles.size());
        dataType = CodecUtil.DATA_TYPE_DOUBLE;
        tmpConstantDoubles.add(item.getValueDouble());
        break;
      case MtasFunctionParserItem.TYPE_PARSER_LONG:
        tmpTypeList.add(type);
        tmpIdList.add(tmpParserLongs.size());
        parser = item.getParser();
        parser.close();
        tmpParserLongs.add(parser);
        sumRule = sumRule?parser.sumRule():false;
        needPositions = needPositions?needPositions:parser.needPositions();
        needArgument.addAll(parser.needArgument);
        break;
      case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
        tmpTypeList.add(type);
        tmpIdList.add(tmpParserDoubles.size());
        dataType = CodecUtil.DATA_TYPE_DOUBLE;
        parser = item.getParser();
        parser.close();
        tmpParserDoubles.add(parser);
        sumRule = sumRule?parser.sumRule():false;
        needPositions = needPositions?needPositions:parser.needPositions();
        needArgument.addAll(parser.needArgument);
        break;
      default:
        throw new ParseException("incorrect type");
      }
    } else {
      throw new ParseException("already defined");
    }
  }

  @Override
  public double getValueDouble(long[] args, long n) throws IOException {
    double sum;
    switch (firstType) {
    case MtasFunctionParserItem.TYPE_ARGUMENT:
      sum = args[firstId];
      break;
    case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
      sum = parserDoubles[firstId].getValueDouble(args, n);
      break;
    case MtasFunctionParserItem.TYPE_PARSER_LONG:
      sum = parserLongs[firstId].getValueLong(args, n);
      break;
    case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
      sum = constantDoubles[firstId];
      break;
    case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
      sum = constantLongs[firstId];
      break;
    case MtasFunctionParserItem.TYPE_N:
      sum = n;
      break;
    default:
      throw new IOException("no first value");
    }
    for (int i = 0; i < number; i++) {
      switch (operatorList[i]) {
      case BASIC_OPERATOR_ADD:
        switch (typeList[i]) {
        case MtasFunctionParserItem.TYPE_ARGUMENT:
          sum += args[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
          sum += parserDoubles[idList[i]].getValueDouble(args, n);
          break;
        case MtasFunctionParserItem.TYPE_PARSER_LONG:
          sum += parserLongs[idList[i]].getValueLong(args, n);
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
          sum += constantDoubles[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
          sum += constantLongs[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_N:
          sum += n;
          break;
        default:
          throw new IOException("unknown type");
        }
        break;
      case BASIC_OPERATOR_SUBTRACT:
        switch (typeList[i]) {
        case MtasFunctionParserItem.TYPE_ARGUMENT:
          sum -= args[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
          sum -= parserDoubles[idList[i]].getValueDouble(args, n);
          break;
        case MtasFunctionParserItem.TYPE_PARSER_LONG:
          sum -= parserLongs[idList[i]].getValueLong(args, n);
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
          sum -= constantDoubles[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_LONG:          
          sum -= constantLongs[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_N:
          sum -= n;
          break;
        default:
          throw new IOException("unknown type");
        }
        break;
      case BASIC_OPERATOR_MULTIPLY:
        switch (typeList[i]) {
        case MtasFunctionParserItem.TYPE_ARGUMENT:
          sum *= args[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
          sum *= parserDoubles[idList[i]].getValueDouble(args, n);
          break;
        case MtasFunctionParserItem.TYPE_PARSER_LONG:
          sum *= parserLongs[idList[i]].getValueLong(args, n);
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
          sum *= constantDoubles[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
          sum *= constantLongs[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_N:
          sum *= n;
          break;
        default:
          throw new IOException("unknown type");
        }
        break;
      case BASIC_OPERATOR_DIVIDE:
        double v;
        switch (typeList[i]) {
        case MtasFunctionParserItem.TYPE_ARGUMENT:
          v = args[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
          v = parserDoubles[idList[i]].getValueDouble(args, n);
          break;
        case MtasFunctionParserItem.TYPE_PARSER_LONG:
          v = parserLongs[idList[i]].getValueLong(args, n);
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
          v = constantDoubles[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
          v = constantLongs[idList[i]];
          break;
        case MtasFunctionParserItem.TYPE_N:
          v = n;
          break;
        default:
          throw new IOException("unknown type");
        }
        if (v != 0) {
          sum /= v;
        } else {
          throw new IOException("division by zero");
        }
        break;
      case BASIC_OPERATOR_POWER:
        switch (typeList[i]) {
        case MtasFunctionParserItem.TYPE_ARGUMENT:
          sum = Math.pow(sum,args[idList[i]]);
          break;
        case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
          sum = Math.pow(sum,parserDoubles[idList[i]].getValueDouble(args, n));
          break;
        case MtasFunctionParserItem.TYPE_PARSER_LONG:
          sum = Math.pow(sum,parserLongs[idList[i]].getValueLong(args, n));
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
          sum = Math.pow(sum,constantDoubles[idList[i]]);
          break;
        case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
          sum = Math.pow(sum,constantLongs[idList[i]]);
          break;
        case MtasFunctionParserItem.TYPE_N:
          sum = Math.pow(sum,n);
          break;
        default:
          throw new IOException("unknown type");
        }
        break;
      default:
        throw new IOException("unknown operator");
      }
    }
    return sum;
  }

  @Override
  public long getValueLong(long[] args, long n) throws IOException {
    try {
      long sum;
      switch (firstType) {
      case MtasFunctionParserItem.TYPE_ARGUMENT:
        sum = args[firstId];
        break;
      case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
        sum = (long) parserDoubles[firstId].getValueDouble(args, n);
        break;
      case MtasFunctionParserItem.TYPE_PARSER_LONG:
        sum = parserLongs[firstId].getValueLong(args, n);
        break;
      case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
        sum = constantDoubles[firstId].longValue();
        break;
      case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
        sum = constantLongs[firstId];
        break;
      case MtasFunctionParserItem.TYPE_N:
        sum = n;
        break;
      default:
        throw new IOException("no first value");
      }
      for (int i = 0; i < number; i++) {
        switch (operatorList[i]) {
        case BASIC_OPERATOR_ADD:
          switch (typeList[i]) {
          case MtasFunctionParserItem.TYPE_ARGUMENT:
            sum += args[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
            sum += (long) parserDoubles[idList[i]].getValueDouble(args, n);
            break;
          case MtasFunctionParserItem.TYPE_PARSER_LONG:
            sum += parserLongs[idList[i]].getValueLong(args, n);
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
            sum += constantDoubles[idList[i]].longValue();
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
            sum += constantLongs[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_N:
            sum += n;
            break;
          default:
            throw new IOException("unknown type");
          }
          break;
        case BASIC_OPERATOR_SUBTRACT:
          switch (typeList[i]) {
          case MtasFunctionParserItem.TYPE_ARGUMENT:
            sum -= args[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
            sum -= (long) parserDoubles[idList[i]].getValueDouble(args, n);
            break;
          case MtasFunctionParserItem.TYPE_PARSER_LONG:
            sum -= parserLongs[idList[i]].getValueLong(args, n);
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
            sum -= constantDoubles[idList[i]].longValue();
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
            sum -= constantLongs[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_N:
            sum -= n;
            break;
          default:
            throw new IOException("unknown type");
          }
          break;
        case BASIC_OPERATOR_MULTIPLY:
          switch (typeList[i]) {
          case MtasFunctionParserItem.TYPE_ARGUMENT:
            sum *= args[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
            sum *= (long) parserDoubles[idList[i]].getValueDouble(args, n);
            break;
          case MtasFunctionParserItem.TYPE_PARSER_LONG:
            sum *= parserLongs[idList[i]].getValueLong(args, n);
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
            sum *= constantDoubles[idList[i]].longValue();
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
            sum *= constantLongs[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_N:
            sum *= n;
            break;
          default:
            throw new IOException("unknown type");
          }
          break;
        case BASIC_OPERATOR_DIVIDE:
          long v;
          switch (typeList[i]) {
          case MtasFunctionParserItem.TYPE_ARGUMENT:
            v = args[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
            v = (long) parserDoubles[idList[i]].getValueDouble(args, n);
            break;
          case MtasFunctionParserItem.TYPE_PARSER_LONG:
            v = parserLongs[idList[i]].getValueLong(args, n);
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
            v = constantDoubles[idList[i]].longValue();
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
            v = constantLongs[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_N:
            v = n;
            break;
          default:
            throw new IOException("unknown type");
          }
          if(v!=0) {
            sum /= v;
          } else {
            throw new IOException("division by zero");
          }
          break;
        case BASIC_OPERATOR_POWER:
          switch (typeList[i]) {
          case MtasFunctionParserItem.TYPE_ARGUMENT:
            sum = sum^args[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_PARSER_DOUBLE:
            sum = sum^(long) parserDoubles[idList[i]].getValueDouble(args, n);
            break;
          case MtasFunctionParserItem.TYPE_PARSER_LONG:
            sum = sum^parserLongs[idList[i]].getValueLong(args, n);
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE:
            sum = sum^constantDoubles[idList[i]].longValue();
            break;
          case MtasFunctionParserItem.TYPE_CONSTANT_LONG:
            sum = sum^constantLongs[idList[i]];
            break;
          case MtasFunctionParserItem.TYPE_N:
            sum = sum^n;
            break;
          default:
            throw new IOException("unknown type");
          }
          break;
        default:
          throw new IOException("unknown operator");
        }
      }
      return sum;
    } catch (java.lang.ArithmeticException e) {
      throw new IOException(e.getMessage());
    }
  }
  
  @Override
  public String toString() {
    String text = "?";
    if(firstType!=null) {
      text = toString(firstType, firstId);
      for(int i=0; i<tmpOperatorList.size(); i++) {
        String operator = tmpOperatorList.get(i);
        if(operator.equals(BASIC_OPERATOR_ADD)) {
          text+=" + ";
        } else if(operator.equals(BASIC_OPERATOR_SUBTRACT)) {
          text+=" - ";
        } else if(operator.equals(BASIC_OPERATOR_MULTIPLY)) {
          text+=" * ";
        } else if(operator.equals(BASIC_OPERATOR_DIVIDE)) {
          text+=" / ";
        } else if(operator.equals(BASIC_OPERATOR_POWER)) {
          text+=" ^ ";
        } else {
          text+=" ? ";
        }
        text+=toString(tmpTypeList.get(i), tmpIdList.get(i));
      }
    }
    return text;
  }
  
  private String toString(String type, int id) {
    if(type.equals(MtasFunctionParserItem.TYPE_CONSTANT_LONG)) {
      return tmpConstantLongs.get(id).toString();
    } else if(type.equals(MtasFunctionParserItem.TYPE_CONSTANT_DOUBLE)) {
      return tmpConstantDoubles.get(id).toString();
    } else if(type.equals(MtasFunctionParserItem.TYPE_PARSER_LONG)) {
      return "("+tmpParserLongs.get(id).toString()+")";
    } else if(type.equals(MtasFunctionParserItem.TYPE_PARSER_DOUBLE)) {
      return "("+tmpParserDoubles.get(id).toString()+")";
    } else if(type.equals(MtasFunctionParserItem.TYPE_ARGUMENT)) {
      return "$q"+id;
    } else if(type.equals(MtasFunctionParserItem.TYPE_N)) {
      return "$n";
    } else{
      return "..";
    }
  }

}
