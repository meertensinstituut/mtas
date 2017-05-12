package mtas.codec.tree;

import mtas.search.spans.MtasSpanMatchAllQuery;

/**
 * The Class MtasTreeNodeId.
 */
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
   * @param additionalRef
   *          the additional ref
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

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasTreeNodeId that = (MtasTreeNodeId) obj;
    return ref.equals(that.ref) && additionalId == that.additionalId
        && additionalRef == that.additionalRef;
  }
  
  @Override
  public int hashCode() {
    int h = this.getClass().getSimpleName().hashCode();
    h = (h * 3) ^ ref.hashCode();
    h = (h * 5) ^ additionalId;
    h = (h * 7) ^ (int) additionalRef;
    return h;
  }  

}