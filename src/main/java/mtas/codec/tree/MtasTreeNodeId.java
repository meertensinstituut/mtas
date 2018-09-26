package mtas.codec.tree;

public class MtasTreeNodeId implements Comparable<MtasTreeNodeId> {
  public Long ref;
  public int additionalId;
  public long additionalRef;

  public MtasTreeNodeId(long ref, int additionalId, long additionalRef) {
    this.ref = ref;
    this.additionalId = additionalId;
    this.additionalRef = additionalRef;
  }

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
