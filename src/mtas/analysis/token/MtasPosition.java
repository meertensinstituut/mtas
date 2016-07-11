package mtas.analysis.token;

import java.util.TreeSet;

/**
 * The Class MtasPosition.
 */
public class MtasPosition {
  
  /** The position single. */
  public static String POSITION_SINGLE = "single";
  
  /** The position range. */
  public static String POSITION_RANGE = "range";
  
  /** The position set. */
  public static String POSITION_SET = "set";
  
  /** The mtas position type. */
  private String mtasPositionType;
  
  /** The mtas position start. */
  private int mtasPositionStart;
  
  /** The mtas position end. */
  private int mtasPositionEnd;
  
  /** The mtas position list. */
  TreeSet<Integer> mtasPositionList;
  
  /**
   * Instantiates a new mtas position.
   *
   * @param position the position
   */
  public MtasPosition(int position) {
    mtasPositionType = POSITION_SINGLE;
    mtasPositionStart = position;
  }
  
  /**
   * Instantiates a new mtas position.
   *
   * @param start the start
   * @param end the end
   */
  public MtasPosition(int start, int end) {
    if(start==end) {
      mtasPositionType = POSITION_SINGLE;
      mtasPositionStart = start;
    } else {
      mtasPositionType = POSITION_RANGE;
      mtasPositionStart = start;
      mtasPositionEnd = end;
    }  
  }
  
  /**
   * Instantiates a new mtas position.
   *
   * @param positions the positions
   */
  public MtasPosition(TreeSet<Integer> positions) {
    if(positions.size()==1) {
      mtasPositionType = POSITION_SINGLE;
      mtasPositionStart = positions.first();
    } else {
      mtasPositionType = POSITION_SET;
      mtasPositionList = new TreeSet<Integer>();
      mtasPositionList.addAll(positions);
      mtasPositionStart = mtasPositionList.first();
      mtasPositionEnd = mtasPositionList.last();
      if(mtasPositionList.size() == (1+mtasPositionEnd - mtasPositionStart)) {
        mtasPositionType = POSITION_RANGE;
        mtasPositionList = null;
      }
    }
  }
  
  /**
   * Check type.
   *
   * @param type the type
   * @return the boolean
   */
  public Boolean checkType(String type) {
    if(mtasPositionType==null) {
      return false;
    } else {
      return mtasPositionType.equals(type);
    }
  }
  
  /**
   * Gets the start.
   *
   * @return the start
   */
  public Integer getStart() {
    return mtasPositionType.equals(null)?null:mtasPositionStart;
  }
  
  /**
   * Gets the end.
   *
   * @return the end
   */
  public Integer getEnd() {
    if(mtasPositionType.equals(POSITION_RANGE)||mtasPositionType.equals(POSITION_SET)) {
      return mtasPositionEnd;
    } else if(mtasPositionType.equals(POSITION_SINGLE)) {
      return mtasPositionStart;
    } else {
      return null;
    }
  }

  /**
   * Gets the positions.
   *
   * @return the positions
   */
  public TreeSet<Integer> getPositions() {
    if(mtasPositionType.equals(POSITION_SET)) {
      return mtasPositionList;
    } else {
      return null;
    }
  }
  
  /**
   * Gets the length.
   *
   * @return the length
   */
  public Integer getLength() {
    if(mtasPositionType.equals(POSITION_SINGLE)) {
      return 1;      
    } else if(mtasPositionType.equals(POSITION_RANGE)||mtasPositionType.equals(POSITION_SET)) {
      return 1+mtasPositionEnd-mtasPositionStart;      
    } else {
      return null;
    }
  }
  
  /**
   * Adds the.
   *
   * @param positions the positions
   */
  public void add(TreeSet<Integer> positions) {
    if(mtasPositionType.equals(POSITION_SINGLE)) {
      mtasPositionType = POSITION_SET;
      mtasPositionList = new TreeSet<Integer>();
      mtasPositionList.add(mtasPositionStart);      
    } else if(mtasPositionType.equals(POSITION_RANGE)) {
      mtasPositionType = POSITION_SET;
      mtasPositionList = new TreeSet<Integer>();
      for(int i=mtasPositionStart; i<=mtasPositionEnd; i++) {
        mtasPositionList.add(i);  
      }
    }
    if(mtasPositionType.equals(POSITION_SET)) {
      for(int position: positions) {
        if(!mtasPositionList.contains(position)) {
          mtasPositionList.add(position);
        }  
      }
      mtasPositionStart = mtasPositionList.first();
      mtasPositionEnd = mtasPositionList.last();
      if(mtasPositionList.size() == 1) {
        mtasPositionType = POSITION_SINGLE;
        mtasPositionList = null;
      } else if(mtasPositionList.size() == (1+mtasPositionEnd - mtasPositionStart)) {
        mtasPositionType = POSITION_RANGE;
        mtasPositionList = null;
      }
    }
  }
  
  /**
   * Adds the.
   *
   * @param position the position
   */
  public void add(int position) {
    if(mtasPositionType.equals(POSITION_SINGLE)) {
      if(position!=mtasPositionStart) {
        if(position==(mtasPositionStart+1)) {
          mtasPositionType=POSITION_RANGE;
          mtasPositionEnd=position;
        } else if(position==(mtasPositionStart-1)) {
          mtasPositionType=POSITION_RANGE;
          mtasPositionEnd=mtasPositionStart;
          mtasPositionStart=position;
        } else {
          mtasPositionType = POSITION_SET;
          mtasPositionList = new TreeSet<Integer>();
          mtasPositionList.add(position);
          mtasPositionList.add(mtasPositionStart);
          mtasPositionStart = mtasPositionList.first();
          mtasPositionEnd = mtasPositionList.last();
        }
      }
    } else {
      if(mtasPositionType.equals(POSITION_RANGE)) {
        mtasPositionType = POSITION_SET;
        mtasPositionList = new TreeSet<Integer>();
        for(int i=mtasPositionStart; i<=mtasPositionEnd; i++) {
          mtasPositionList.add(i);  
        }
      }
      if(mtasPositionType.equals(POSITION_SET)) {
        if(!mtasPositionList.contains(position)) {
          mtasPositionList.add(position);
          mtasPositionStart = mtasPositionList.first();
          mtasPositionEnd = mtasPositionList.last();
          if(mtasPositionList.size() == (1+mtasPositionEnd - mtasPositionStart)) {
            mtasPositionType = POSITION_RANGE;
            mtasPositionList = null;
          }
        }
      }
    }      
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    if(mtasPositionType==null) {
      return "[null]";
    } else if(mtasPositionType.equals(POSITION_SINGLE)) {      
      return "["+mtasPositionStart+"]";
    } else if(mtasPositionType.equals(POSITION_RANGE)) {
      return "["+mtasPositionStart+"-"+mtasPositionEnd+"]";
    } else if(mtasPositionType.equals(POSITION_SET)) {
      return mtasPositionList.toArray().toString();
    } else {
      return "[unknown]";
    }
  }
  
  
}
