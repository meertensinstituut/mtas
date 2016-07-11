package mtas.codec.tree;

/**
 * The Class MtasAVLTreeNode.
 */
public class MtasAVLTreeNode extends MtasTreeNode<MtasAVLTreeNode> {

    /** The balance. */
    public int balance;
    
    /** The parent. */
    public MtasAVLTreeNode parent;
    
    // node with start and end position
    /**
     * Instantiates a new mtas avl tree node.
     *
     * @param left the left
     * @param right the right
     * @param parent the parent
     */
    public MtasAVLTreeNode(int left, int right, MtasAVLTreeNode parent) {
      super(left, right);
      this.parent = parent;
    }

    /* (non-Javadoc)
     * @see mtas.codec.tree.MtasTreeNode#self()
     */
    @Override
    protected MtasAVLTreeNode self() {
      return self();
    }    

  }
