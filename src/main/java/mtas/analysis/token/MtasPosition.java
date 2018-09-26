package mtas.analysis.token;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang.ArrayUtils;

public class MtasPosition {
  public static final String POSITION_SINGLE = "single";
  public static final String POSITION_RANGE = "range";
  public static final String POSITION_SET = "set";

  private String mtasPositionType;
  private int mtasPositionStart;
  private int mtasPositionEnd;
  private int[] mtasPositionList = null;

  public MtasPosition(int position) {
    mtasPositionType = POSITION_SINGLE;
    mtasPositionStart = position;
  }

  public MtasPosition(int start, int end) {
    if (start == end) {
      mtasPositionType = POSITION_SINGLE;
      mtasPositionStart = start;
    } else {
      mtasPositionType = POSITION_RANGE;
      mtasPositionStart = start;
      mtasPositionEnd = end;
    }
  }

  public MtasPosition(int[] positions) {
    SortedSet<Integer> list = new TreeSet<>();
    for (int p : positions) {
      list.add(p);
    }
    if (list.size() == 1) {
      mtasPositionType = POSITION_SINGLE;
      mtasPositionStart = list.first();
    } else {
      mtasPositionType = POSITION_SET;
      mtasPositionList = ArrayUtils
          .toPrimitive(list.toArray(new Integer[list.size()]));
      mtasPositionStart = list.first();
      mtasPositionEnd = list.last();
      if (mtasPositionList.length == (1 + mtasPositionEnd
          - mtasPositionStart)) {
        mtasPositionType = POSITION_RANGE;
        mtasPositionList = null;
      }
    }
  }

  public Boolean checkType(String type) {
    if (mtasPositionType == null) {
      return false;
    } else {
      return mtasPositionType.equals(type);
    }
  }

  public Integer getStart() {
    return mtasPositionType == null ? null : mtasPositionStart;
  }

  public Integer getEnd() {
    if (mtasPositionType.equals(POSITION_RANGE)
        || mtasPositionType.equals(POSITION_SET)) {
      return mtasPositionEnd;
    } else if (mtasPositionType.equals(POSITION_SINGLE)) {
      return mtasPositionStart;
    } else {
      return null;
    }
  }

  public int[] getPositions() {
    return (mtasPositionType.equals(POSITION_SET))
      ? mtasPositionList.clone() : null;
  }

  public Integer getLength() {
    if (mtasPositionType.equals(POSITION_SINGLE)) {
      return 1;
    } else if (mtasPositionType.equals(POSITION_RANGE)
        || mtasPositionType.equals(POSITION_SET)) {
      return 1 + mtasPositionEnd - mtasPositionStart;
    } else {
      return null;
    }
  }

  public void add(int[] positions) {
    SortedSet<Integer> list = new TreeSet<>();
    for (int p : positions) {
      list.add(p);
    }
    if (mtasPositionType.equals(POSITION_SINGLE)) {
      mtasPositionType = POSITION_SET;
      list.add(mtasPositionStart);
    } else if (mtasPositionType.equals(POSITION_RANGE)) {
      mtasPositionType = POSITION_SET;
      for (int i = mtasPositionStart; i <= mtasPositionEnd; i++) {
        list.add(i);
      }
    } else if (mtasPositionType.equals(POSITION_SET)) {
      for (int p : mtasPositionList) {
        list.add(p);
      }
    }
    mtasPositionList = ArrayUtils
        .toPrimitive(list.toArray(new Integer[list.size()]));
    mtasPositionStart = list.first();
    mtasPositionEnd = list.last();
    if (list.size() == 1) {
      mtasPositionType = POSITION_SINGLE;
      mtasPositionList = null;
    } else if (list.size() == (1 + mtasPositionEnd - mtasPositionStart)) {
      mtasPositionType = POSITION_RANGE;
      mtasPositionList = null;
    }
  }

  public void add(int position) {
    if (mtasPositionType.equals(POSITION_SINGLE)) {
      if (position != mtasPositionStart) {
        if (position == (mtasPositionStart + 1)) {
          mtasPositionType = POSITION_RANGE;
          mtasPositionEnd = position;
        } else if (position == (mtasPositionStart - 1)) {
          mtasPositionType = POSITION_RANGE;
          mtasPositionEnd = mtasPositionStart;
          mtasPositionStart = position;
        } else {
          mtasPositionType = POSITION_SET;
          SortedSet<Integer> list = new TreeSet<>();
          list.add(position);
          list.add(mtasPositionStart);
          mtasPositionList = ArrayUtils
              .toPrimitive(list.toArray(new Integer[list.size()]));
          mtasPositionStart = list.first();
          mtasPositionEnd = list.last();
        }
      }
    } else {
      SortedSet<Integer> list = new TreeSet<>();
      if (mtasPositionType.equals(POSITION_RANGE)) {
        mtasPositionType = POSITION_SET;
        for (int i = mtasPositionStart; i <= mtasPositionEnd; i++) {
          list.add(i);
        }
        list.add(position);
      } else if (mtasPositionType.equals(POSITION_SET)) {
        for (int p : mtasPositionList) {
          list.add(p);
        }
        list.add(position);
      }
      mtasPositionList = ArrayUtils
          .toPrimitive(list.toArray(new Integer[list.size()]));
      mtasPositionStart = list.first();
      mtasPositionEnd = list.last();
      if (list.size() == (1 + mtasPositionEnd - mtasPositionStart)) {
        mtasPositionType = POSITION_RANGE;
        mtasPositionList = null;
      }
    }
  }

  @Override
  public String toString() {
    if (mtasPositionType == null) {
      return "[null]";
    } else if (mtasPositionType.equals(POSITION_SINGLE)) {
      return "[" + mtasPositionStart + "]";
    } else if (mtasPositionType.equals(POSITION_RANGE)) {
      return "[" + mtasPositionStart + "-" + mtasPositionEnd + "]";
    } else if (mtasPositionType.equals(POSITION_SET)) {
      return Arrays.toString(mtasPositionList);
    } else {
      return "[unknown]";
    }
  }

}
