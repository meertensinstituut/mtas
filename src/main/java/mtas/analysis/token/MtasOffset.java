package mtas.analysis.token;

public class MtasOffset {
  private int mtasOffsetStart;
  private int mtasOffsetEnd;

  public MtasOffset(int start, int end) {
    mtasOffsetStart = start;
    mtasOffsetEnd = end;
  }

  public void add(int start, int end) {
    mtasOffsetStart = Math.min(mtasOffsetStart, start);
    mtasOffsetEnd = Math.max(mtasOffsetEnd, end);
  }

  public int getStart() {
    return mtasOffsetStart;
  }

  public int getEnd() {
    return mtasOffsetEnd;
  }

  @Override
  public String toString() {
    return "[" + mtasOffsetStart + "-" + mtasOffsetEnd + "]";
  }
}
