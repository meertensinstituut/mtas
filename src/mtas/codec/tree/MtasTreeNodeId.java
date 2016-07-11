package mtas.codec.tree;

public class MtasTreeNodeId implements Comparable<MtasTreeNodeId> {
  
  public Long ref;
  public int additionalId;
  
  public MtasTreeNodeId(long ref, int additionalId) {
    this.ref = ref;
    this.additionalId = additionalId;
  }

  @Override
  public int compareTo(MtasTreeNodeId o) {
    return ref.compareTo(o.ref);
  }
  
}
