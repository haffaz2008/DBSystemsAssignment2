package Tree;


import BaseClasses.BaseDbItemInterface;
import Overall.Deserialize;
import Overall.Serialize;

import java.io.UnsupportedEncodingException;

public class Leaf<TKey extends Comparable<TKey> & BaseDbItemInterface<TKey>, TValue extends BaseDbItemInterface<TValue>>  extends IndexNode<TKey, TValue> {

    private Object[] values;

    public Leaf(TKey keyType, TValue valueType) {
        super(keyType, valueType);
        this.allKeys = new Object[KEY_SIZE];
        this.values = new Object[VALUE_SIZE];
    }

    @SuppressWarnings("unchecked")
    public TValue getValue(int index) {
        return (TValue)this.values[index];
    }

    public void setValue(int index, TValue value) {
        this.values[index] = value;
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.LeafNode;
    }

    @Override
    public int search(TKey key) {
        for (int i = 0; i < this.getKeyCount(); ++i) {
            int cmp = this.getKey(i).compareTo(key);
            if (cmp == 0) {
                return i;
            }
            else if (cmp > 0) {
                return -1;
            }
        }

        return -1;
    }


    /* The codes below are used to support insertion operation */

    public void insertKey(TKey key, TValue value) {
        int index = 0;
        while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
            ++index;
        this.insertAt(index, key, value);
    }

    private void insertAt(int index, TKey key, TValue value) {
        // move space for the new key
        for (int i = this.getKeyCount() - 1; i >= index; --i) {
            this.setKey(i + 1, this.getKey(i));
            this.setValue(i + 1, this.getValue(i));
        }

        // insert new key and value
        this.setKey(index, key);
        this.setValue(index, value);
        ++this.keyCount;
    }


    /**
     * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
     */
    @Override
    protected IndexNode<TKey,TValue> split() {
        int midIndex = this.getKeyCount() / 2;

        Leaf<TKey, TValue> newRNode = this.newInstance();
        for (int i = midIndex; i < this.getKeyCount(); ++i) {
            newRNode.setKey(i - midIndex, this.getKey(i));
            newRNode.setValue(i - midIndex, this.getValue(i));
            this.setKey(i, null);
            this.setValue(i, null);
        }
        newRNode.keyCount = this.getKeyCount() - midIndex;
        this.keyCount = midIndex;

        return newRNode;
    }

    @Override
    protected IndexNode<TKey,TValue> pushUpKey(TKey key, IndexNode<TKey,TValue> leftChild, IndexNode<TKey,TValue> rightNode) {
        throw new UnsupportedOperationException();
    }




    /* The codes below are used to support deletion operation */

    public boolean delete(TKey key) {
        int index = this.search(key);
        if (index == -1)
            return false;

        this.deleteAt(index);
        return true;
    }

    private void deleteAt(int index) {
        int i = index;
        for (i = index; i < this.getKeyCount() - 1; ++i) {
            this.setKey(i, this.getKey(i + 1));
            this.setValue(i, this.getValue(i + 1));
        }
        this.setKey(i, null);
        this.setValue(i, null);
        --this.keyCount;
    }

    @Override
    protected void processChildrenTransfer(IndexNode<TKey,TValue> borrower, IndexNode<TKey,TValue> lender, int borrowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected IndexNode<TKey,TValue> processChildrenFusion(IndexNode<TKey,TValue> leftChild, IndexNode<TKey,TValue> rightChild) {
        throw new UnsupportedOperationException();
    }

    /**
     * Notice that the key sunk from parent is be abandoned.
     */
    @Override
    protected void fusionWithSibling(TKey sinkKey, IndexNode<TKey,TValue> rightSibling) {
        Leaf<TKey, TValue> siblingLeaf = (Leaf<TKey, TValue>)rightSibling;

        int j = this.getKeyCount();
        for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
            this.setKey(j + i, siblingLeaf.getKey(i));
            this.setValue(j + i, siblingLeaf.getValue(i));
        }
        this.keyCount += siblingLeaf.getKeyCount();

        this.setRightSibling(siblingLeaf.rightSibling);
        if (siblingLeaf.rightSibling != null)
            siblingLeaf.rightSibling.setLeftSibling(this);
    }

    @Override
    protected TKey transferFromSibling(TKey sinkKey, IndexNode<TKey,TValue> sibling, int borrowIndex) {
        Leaf<TKey, TValue> siblingNode = (Leaf<TKey, TValue>)sibling;

        this.insertKey(siblingNode.getKey(borrowIndex), siblingNode.getValue(borrowIndex));
        siblingNode.deleteAt(borrowIndex);

        return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
    }

    @Override
    Leaf<TKey,TValue> newInstance() {
        return new Leaf<TKey,TValue>(this.keyType, this.valueType);
    }

    @Override
    public int getSize() {
        return super.getSize() + this.valueListSize();
    }

    protected int valueListSize(){
        return VALUE_SIZE * this.valueType.getSize();
    }

    protected int valueListOffset(){
        return super.getSize();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Leaf<TKey, TValue> deserialize(byte[] DATA) throws UnsupportedEncodingException {
        super.deserialize(DATA);
        this.values = Deserialize.array(
                Deserialize.bytes(DATA, this.valueListSize(), this.valueListOffset()), this.valueType
        );
        return (Leaf<TKey,TValue>) this.clone();
    }

    @Override
    public byte[] serialize() throws UnsupportedEncodingException {
        byte[] DATA = new byte[this.getSize()];
        Serialize.bytes(super.serialize(), super.getSize(), 0, DATA);
        Serialize.bytes(
                Serialize.array(this.values, this.valueListSize()),
                this.valueListSize(),
                this.valueListOffset(),
                DATA
        );
        return DATA;
    }

    @Override
    public void fillIterator(Stats<TKey,TValue> items, int depth) {
        items.addNode(this, depth);
    }

    @Override
    public String toString() {
        return String.format("(leaf) size: %d  id %s keys %s", this.getSize(), this.key.toString(), Serialize.arrayToString(this.allKeys, false));
    }
    @Override
    public String detailedJsonString() {
        return Serialize.arrayToString(this.values, false);
    }

    @Override
    public IndexNode<TKey, TValue> load() {
        return this.loader.loadLeafNode(this);
    }
}

