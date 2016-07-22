package mtas.codec.tree;

public class MtasTreeNodeId implements Comparable<MtasTreeNodeId> {

  /** The ref. */
  public Long ref;

  /** The additional id. */
  public int additionalId;

  /** The additional ref. */
  public long additionalRef;

  /**
   * Instantiates a new mtas tree node id.
   *
   * @param ref
   *          the ref
   * @param additionalId
   *          the additional id
   */
  public MtasTreeNodeId(long ref, int additionalId, long additionalRef) {
    this.ref = ref;
    this.additionalId = additionalId;
    this.additionalRef = additionalRef;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(MtasTreeNodeId o) {
    return ref.compareTo(o.ref);
  }

}
