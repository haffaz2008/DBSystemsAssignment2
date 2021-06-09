package Tree;

import BaseClasses.BaseDbItemInterface;

import java.util.Iterator;

public class Root <TKey extends Comparable<TKey> & BaseDbItemInterface<TKey> , TValue extends BaseDbItemInterface<TValue>>{
    protected IndexNode<TKey, TValue> root;

    public Root(TKey keyType, TValue valueType) {
        this.root = new Leaf<TKey, TValue>(keyType,valueType);
    }
    public IndexNode<TKey, TValue> getNode(){
        return this.root;
    }
    public void setRoot(IndexNode<TKey, TValue> root){
        this.root = root;
    }

    /**
     * Insert a new key and its associated value into the B+ tree.
     */
    public void insert(TKey key, TValue value) {
        Leaf<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        leaf.insertKey(key, value);

        if (leaf.isOverflow()) {
            IndexNode<TKey, TValue> n = leaf.dealOverflow();
            if (n != null)
                this.root = n;
        }
    }

    /**
     * Search a key value on the tree and return its associated value.
     */
    public TValue search(TKey key) {
        Leaf<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        int index = leaf.search(key);
        return (index == -1) ? null : leaf.getValue(index);
    }

    /**
     * Delete a key and its associated value from the tree.
     */
    public void delete(TKey key) {
        Leaf<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        if (leaf.delete(key) && leaf.isUnderflow()) {
            IndexNode<TKey, TValue> n = leaf.dealUnderflow();
            if (n != null)
                this.root = n;
        }
    }

    /**
     * Search the leaf node which should contain the specified key
     */
    private Leaf<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
        IndexNode<TKey, TValue> node = this.root;
        while (node.getNodeType() == NodeType.InnerNode) {
            int nextNode = node.search(key);
            node = ((InnerNode<TKey, TValue>)node).getChild(nextNode);
        }

        return (Leaf<TKey, TValue>)node;
    }

    public Iterator<IndexNode<TKey,TValue>> iterator() {
        Stats<TKey,TValue> stats = this.stats();
        return stats.nodes.iterator();
    }

    public Stats<TKey,TValue> stats(){
        Stats<TKey,TValue> stats = new Stats<>();
        this.root.fillIterator(stats, 0);
        return stats;
    }

}
