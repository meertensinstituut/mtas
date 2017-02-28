package mtas.parser;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Random;

import mtas.parser.function.MtasFunctionParser;
import mtas.parser.function.ParseException;
import mtas.parser.function.util.MtasFunctionParserFunction;
import mtas.parser.function.util.MtasFunctionParserFunctionResponse;
import mtas.parser.function.util.MtasFunctionParserFunctionResponseDouble;
import mtas.parser.function.util.MtasFunctionParserFunctionResponseLong;

public class MtasFunctionParserTest {

  Random generator = new Random();

  private void testFunction(MtasFunctionParserFunction pf, long[] args, int n,
      MtasFunctionParserFunctionResponse r) {
    assertEquals(pf + "\tn:" + n + "\targs:" + Arrays.toString(args),
        pf.getResponse(args, n), r);
  }

  private long[] getArgs(int n, int min, int max) {
    long[] args = new long[n];
    for (int i = 0; i < n; i++) {
      args[i] = min + generator.nextInt((1 + max - min));
    }
    return args;
  }

  private int getN(int min, int max) {
    return min + generator.nextInt((1 + max - min));
  }

  @org.junit.Test
  public void basicTestFunction1() {
    String function = null;
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    try {
      for (int i = 0; i < 1000; i++) {
        int n = getN(0, 10000);
        int k = generator.nextInt(10);
        function = "$q" + k;
        p = new MtasFunctionParser(
            new BufferedReader(new StringReader(function)));
        pf = p.parse();
        args = getArgs(1 + k + generator.nextInt(20), -1000, 1000);
        testFunction(pf, args, n,
            new MtasFunctionParserFunctionResponseLong(args[k], true));
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void basicTestFunction2() {
    String function = null;
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    function = "$n";
    p = new MtasFunctionParser(new BufferedReader(new StringReader(function)));
    try {
      pf = p.parse();
      for (int i = 0; i < 1000; i++) {
        int n = getN(0, 10000);
        int k = generator.nextInt(10);
        args = getArgs(k + generator.nextInt(20), -1000, 1000);
        testFunction(pf, args, n,
            new MtasFunctionParserFunctionResponseLong(n, true));
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void basicTestFunction3() {
    String function = null;
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    try {
      for (int i = 0; i < 1000; i++) {
        int n = getN(0, 10);
        int i1 = generator.nextInt(100) - 50;
        int o0 = generator.nextInt(4);
        int k1 = generator.nextInt(10);
        int o1 = generator.nextInt(4);
        int k2 = generator.nextInt(10);
        int o2 = generator.nextInt(4);
        int k3 = generator.nextInt(10);
        int o3 = generator.nextInt(4);
        int k4 = generator.nextInt(10);
        int o4 = generator.nextInt(4);
        int k5 = generator.nextInt(10);
        int o5 = generator.nextInt(4);
        int k6 = generator.nextInt(3);
        int o6 = generator.nextInt(4);
        function = i1 + " " + getOperator(o0) + " $q" + k1 + " "
            + getOperator(o1) + " $q" + k2 + " " + getOperator(o2) + " $q" + k3
            + " " + getOperator(o3) + " $q" + k4 + " " + getOperator(o4) + " $q"
            + k5 + " " + getOperator(o5) + " $n " + getOperator(o6) + " $q"
            + k6;
        p = new MtasFunctionParser(
            new BufferedReader(new StringReader(function)));
        pf = p.parse();
        args = getArgs(10 + generator.nextInt(20), -10, 10);
        Object answer = null;
        try {
          answer = compute(o0, i1, args[k1]);
          answer = answer instanceof Double
              ? compute(o1, (double) answer, args[k2])
              : compute(o1, (int) answer, args[k2]);
          answer = answer instanceof Double
              ? compute(o2, (double) answer, args[k3])
              : compute(o2, (int) answer, args[k3]);
          answer = answer instanceof Double
              ? compute(o3, (double) answer, args[k4])
              : compute(o3, (int) answer, args[k4]);
          answer = answer instanceof Double
              ? compute(o4, (double) answer, args[k5])
              : compute(o4, (int) answer, args[k5]);
          answer = answer instanceof Double 
              ? compute(o5, (double) answer, n)
              : compute(o5, (int) answer, n);
          answer = answer instanceof Double
              ? compute(o6, (double) answer, args[k6])
              : compute(o6, (int) answer, args[k6]);
          if (answer instanceof Double) {
            testFunction(pf, args, n,
                new MtasFunctionParserFunctionResponseDouble((double) answer,
                    true));
          } else {
            testFunction(pf, args, n,
                new MtasFunctionParserFunctionResponseLong((int) answer,
                    true));
          }
        } catch (IOException | IllegalArgumentException e) {
          if (answer != null && answer instanceof Double) {
            testFunction(pf, args, n,
                new MtasFunctionParserFunctionResponseDouble((double) answer,
                    false));
          } else {
            testFunction(pf, args, n,
                new MtasFunctionParserFunctionResponseDouble((int) 0, false));
          }
        }
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void basicTestFunction4() {
    String function = null;
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    try {
      int n = getN(0, 10000);
      int k1 = generator.nextInt(10);
      function = "100/$q" + k1;
      p = new MtasFunctionParser(
          new BufferedReader(new StringReader(function)));
      pf = p.parse();
      args = getArgs(10 + generator.nextInt(20), 100, 1000);
      double answer = 100.0 / args[k1];
      testFunction(pf, args, n,
          new MtasFunctionParserFunctionResponseDouble(answer, true));
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void basicTestFunction5() {
    String function = null;
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    try {
      function = "$n+100/$q0";
      p = new MtasFunctionParser(
          new BufferedReader(new StringReader(function)));
      pf = p.parse();
      args = new long[] { 0 };
      testFunction(pf, args, 10,
          new MtasFunctionParserFunctionResponseDouble(0, false));
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void basicTestFunction6() {
    String function = null;
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    try {
      for (int i = 0; i < 1000; i++) {
        int n = getN(0, 10000);
        int k = generator.nextInt(10);
        function = "$n+1.3+2.6/$q" + k;
        p = new MtasFunctionParser(
            new BufferedReader(new StringReader(function)));
        pf = p.parse();
        args = getArgs(10 + generator.nextInt(20), -1000, 1000);
        double answer = (args[k] != 0) ? (n + 1.3 + 2.6) / args[k] : 0;
        testFunction(pf, args, n,
            new MtasFunctionParserFunctionResponseDouble(answer, args[k] != 0));
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void basicTestFunction7() {
    String function = null;
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    try {
      for (int i = 0; i < 1000; i++) {
        int n = getN(0, 10000);
        int k1 = generator.nextInt(10);
        int k2 = generator.nextInt(10);
        int k3 = generator.nextInt(10);
        function = "$n * ($q" + k1 + "+$q" + k2 + ")/$q" + k3;
        p = new MtasFunctionParser(
            new BufferedReader(new StringReader(function)));
        pf = p.parse();
        args = getArgs(10 + generator.nextInt(20), -1000, 1000);
        double answer = (args[k3] != 0)
            ? (double) (n * (args[k1] + args[k2])) / args[k3] : 0;
        testFunction(pf, args, n, new MtasFunctionParserFunctionResponseDouble(
            answer, args[k3] != 0));
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void basicTestFunction8() {
    String function = null;
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    try {
      for (int i = 0; i < 100000; i++) {
        int n = getN(0, 10000);
        int k1 = generator.nextInt(10);
        int k2 = generator.nextInt(10);
        int k3 = generator.nextInt(10);
        int k4 = generator.nextInt(10);
        function = "1+(($q" + k1 + "+$q" + k2 + ")/($q" + k3 + "+$q" + k4
            + "))-$n";
        p = new MtasFunctionParser(
            new BufferedReader(new StringReader(function)));
        pf = p.parse();
        args = getArgs(10 + generator.nextInt(20), -1000, 1000);
        double answer = (args[k3] + args[k4] != 0)
            ? ((double) ((args[k1] + args[k2]))
                / (double) ((args[k3] + args[k4]))) + 1 - n
            : 0;
        testFunction(pf, args, n, new MtasFunctionParserFunctionResponseDouble(
            answer, (args[k3] + args[k4]) != 0));
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @org.junit.Test
  public void basicTestFunction9() {
    String function = null;
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    try {
      int n = getN(0, 100);
      int k1 = generator.nextInt(10);
      function = "$n^$q" + k1;
      p = new MtasFunctionParser(
          new BufferedReader(new StringReader(function)));
      pf = p.parse();
      args = getArgs(10 + generator.nextInt(20), 0, 2);
      long answer = n ^ args[k1];
      testFunction(pf, args, n,
          new MtasFunctionParserFunctionResponseLong(answer, true));
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
  
  @org.junit.Test
  public void basicTestFunction10() {
    String function = "(1+2)/3";
    MtasFunctionParser p;
    MtasFunctionParserFunction pf;
    long[] args = null;
    try {
      int n = getN(0, 100);
      int k1 = generator.nextInt(10);
      int k2 = generator.nextInt(10);
      int k3 = generator.nextInt(10);
      function = "("+k1+" + "+k2+")/($q0 + 1 + "+k3+" - 2)";
      p = new MtasFunctionParser(
          new BufferedReader(new StringReader(function)));
      pf = p.parse();
      args = getArgs(10 + generator.nextInt(20), 0, 2);
      if((args[0]+1+k3-2)!=0) {
        double answer = (double)(k1+k2) / (args[0]+1+k3-2);
        testFunction(pf, args, n,
          new MtasFunctionParserFunctionResponseDouble(answer, true));
      }  
    } catch (ParseException e) {
      e.printStackTrace();
    }  
  }
  
  private Object compute(int op, long v1, long v2) throws IOException {
    if (op == 0) {
      Long s;
      s = (long) (v1 + v2);
      if (s > Integer.MAX_VALUE || s < Integer.MIN_VALUE) {
        throw new IOException("too big");
      } else {
        return s.intValue();
      }
    } else if (op == 1) {
      Long s;
      s = (long) (v1 - v2);
      if (s > Integer.MAX_VALUE || s < Integer.MIN_VALUE) {
        throw new IOException("too big");
      } else {
        return s.intValue();
      }
    } else if (op == 2) {
      Long s;
      s = (long) (v1 * v2);
      if (s > Integer.MAX_VALUE || s < Integer.MIN_VALUE) {
        throw new IOException("too big");
      } else {
        return s.intValue();
      }
    } else if (op == 3) {
      if (v2 == 0) {
        throw new IllegalArgumentException("division by zero");
      } else {
        return (double) v1 / v2;
      }
    } else if (op == 4) {
      Long s;
      s = (long) (v1 ^ v2);
      if (s > Integer.MAX_VALUE || s < Integer.MIN_VALUE) {
        throw new IOException("too big");
      } else {
        return s.intValue();
      }
    } else {
      throw new IOException("unknown operator");
    }
  }

  private Double compute(int op, long v1, double v2) throws IOException {
    return compute(op, (double) v1, v2);
  }

  private Double compute(int op, double v1, long v2) throws IOException {
    return compute(op, v1, (double) v2);
  }

  private Double compute(int op, double v1, double v2) throws IOException {
    if (op == 0) {
      return v1 + v2;
    } else if (op == 1) {
      return v1 - v2;
    } else if (op == 2) {
      return v1 * v2;
    } else if (op == 3) {
      if (v2 == 0) {
        throw new IllegalArgumentException("division by zero");
      } else {
        return v1 / v2;
      }
    } else if (op == 4) {
      return Math.pow(v1, v2);
    } else {
      throw new IOException("unknown operator");
    }
  }

  private String getOperator(int op) {
    if (op == 0) {
      return "+";
    } else if (op == 1) {
      return "-";
    } else if (op == 2) {
      return "*";
    } else if (op == 3) {
      return "/";
    } else if (op == 4) {
      return "^";
    } else {
      return "?";
    }
  }

}
