package mtas.codec.tree;

/**
 * The Class MtasTreeNodeId.
 */
public class MtasTreeNodeId implements Comparable<MtasTreeNodeId> {
  
  /** The ref. */
  public Long ref;
  
  /** The additional id. */
  public int additionalId;
  
  /**
   * Instantiates a new mtas tree node id.
   *
   * @param ref the ref
   * @param additionalId the additional id
   */
  public MtasTreeNodeId(long ref, int additionalId) {
    this.ref = ref;
    this.additionalId = additionalId;
  }

  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(MtasTreeNodeId o) {
    return ref.compareTo(o.ref);
  }
  
}
