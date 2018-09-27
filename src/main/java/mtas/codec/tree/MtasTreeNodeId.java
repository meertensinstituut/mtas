package mtas.codec.tree;

import java.util.Objects;

/**
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public class MtasTreeNodeId implements Comparable<MtasTreeNodeId> {
  public long ref;
  public int additionalId;
  public long additionalRef;

  public MtasTreeNodeId(long ref, int additionalId, long additionalRef) {
    this.ref = ref;
    this.additionalId = additionalId;
    this.additionalRef = additionalRef;
  }

  @Override
  public int compareTo(MtasTreeNodeId o) {
    return Long.compare(ref, o.ref);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final MtasTreeNodeId that = (MtasTreeNodeId) obj;
    return ref == that.ref &&
      additionalId == that.additionalId &&
      additionalRef == that.additionalRef;
  }

  @Override
  public int hashCode() {
    return Objects.hash(ref, additionalId, additionalRef);
  }
}
