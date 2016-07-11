package mtas.codec.tree;

import java.util.TreeMap;
import java.util.TreeSet;
import java.io.IOException;
import java.util.Map.Entry;

import mtas.analysis.token.MtasPosition;
import mtas.analysis.token.MtasToken;

/**
 * The Class MtasTree.
 *
 * @param <N> the number type
 */
abstract public class MtasTree<N extends MtasTreeNode<N>> {
  
  final public static byte SINGLE_POSITION_TREE = 1;
  final public static byte STORE_ADDITIONAL_ID = 2;

  /** The root. */
  protected N root;
  
  /** The closed. */
  private Boolean closed;
  
  /** The single point. */
  protected Boolean singlePoint; 
  
  /** Additional id. */
  protected Boolean storePrefixId; 

  public MtasTree(boolean singlePoint, boolean storePrefixId) {
    root = null;
    closed = false;
    this.singlePoint = singlePoint;    
    this.storePrefixId = storePrefixId;    
  }
  
  /**
   * Adds the id from doc.
   *
   * @param docId the doc id
   * @param reference the reference
   */
  final public void addIdFromDoc(Integer docId, Long reference) { 
    if(!closed && (docId!=null)) {      
      addSinglePoint(docId, 0, docId, reference);
    }
  }
    
  /**
   * Adds the parent from token.
   *
   * @param <T> the generic type
   * @param token the token
   * @throws IOException 
   */
  final public <T> void addParentFromToken(MtasToken<T> token) throws IOException { 
    if(!closed && (token!=null)) {      
      if(token.checkParentId()) {
        addSinglePoint(token.getParentId(), token.getPrefixId(), token.getId(), token.getTokenRef());
      }
    }
  }
  
  final public <T> void addPositionAndObjectFromToken(MtasToken<T> token) throws IOException {   
    addPositionFromToken(token, token.getTokenRef());
  }
  
//  final public <T> void addPositionAndTermFromToken(MtasToken<T> token) {   
//    addPositionFromToken(token, token.getTermRef());
//  }
  
  /**
   * Adds the position from token.
   *
   * @param <T> the generic type
   * @param token the token
   * @throws IOException 
   */
  final private <T> void addPositionFromToken(MtasToken<T> token, Long ref) throws IOException {
    int prefixId = storePrefixId?token.getPrefixId():0;        
    if(!closed && (token!=null)) {
      if(token.checkPositionType(MtasPosition.POSITION_SINGLE)) {
        addSinglePoint(token.getPositionStart(), prefixId, token.getId(), ref);
      } else if(token.checkPositionType(MtasPosition.POSITION_RANGE)) {
        addRange(token.getPositionStart(), token.getPositionEnd(), prefixId, token.getId(), ref);  
      } else if(token.checkPositionType(MtasPosition.POSITION_SET)) {
        //split set into minimum number of single points and ranges
        TreeMap<Integer,Integer> list = new TreeMap<Integer,Integer>();
        TreeSet<Integer> positions = token.getPositions();
        Integer lastPoint = null;
        Integer startPoint = null;      
        for (int position : positions) {
          if(lastPoint==null) {
            startPoint = position;
            lastPoint = position;
          } else if((position-lastPoint)!=1) {
            list.put(startPoint, lastPoint);
            startPoint = position;          
          }
          lastPoint = position;
        }
        if(lastPoint!=null) {
          list.put(startPoint, lastPoint);
        }
        for (Entry<Integer, Integer> entry : list.entrySet()) {
          if(entry.getKey().equals(entry.getValue())) {
            addSinglePoint(entry.getKey(), prefixId, token.getId(), ref);
          } else {
            addRange(entry.getKey(), entry.getValue(), prefixId, token.getId(), ref);
          }
        }
      }
    }
  }
  
  /**
   * Close.
   *
   * @return the n
   */
  final public N close() {
    if(root==null) {
      addRangeEmpty(0,0,0);
    }
    closed = true;
    return root;
  } 

  /**
   * Adds the token single point.
   *
   * @param position the position
   * @param id the id
   * @param ref the ref
   */
  abstract protected void addSinglePoint(int position, int additionalId, Integer id, Long ref);
  
  /**
   * Adds the token range.
   *
   * @param left the left
   * @param right the right
   * @param id the id
   * @param ref the ref
   */
  abstract protected void addRange(int left, int right, int additionalId, Integer id, Long ref);
  
  /**
   * Adds the token range empty.
   *
   * @param left the left
   * @param right the right
   */
  abstract protected void addRangeEmpty(int left, int right, int additionalId);

  final public boolean isSinglePoint() {
    return singlePoint;
  } 
  
  final public boolean isStorePrefixId() {
    return storePrefixId;    
  }
  
  /**
   * Prints the balance.
   */
  final public void printBalance() {
    printBalance(1, root);
  }

  /**
   * Prints the balance.
   *
   * @param p the p
   * @param n the n
   */
  final private void printBalance(Integer p, N n) {    
    if(n!=null) {
      printBalance((p+1), n.leftChild);
      System.out.print(String.format("%"+(3*p)+"s", ""));
      if(n.left==n.right) {
        System.out.println("["+n.left+"] ("+n.max+") : "+n.ids.size()+" tokens");
      } else {
        System.out.println("["+n.left+"-"+n.right+"] ("+n.max+") : "+n.ids.size()+" tokens");        
      }
      printBalance((p+1), n.rightChild);
    }
  }

}
