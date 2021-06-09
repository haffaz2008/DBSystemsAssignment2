package Tree;

import BaseClasses.BaseDbItemInterface;
import Overall.Serialize;

import java.io.UnsupportedEncodingException;

public class InnerNode<TKey extends Comparable<TKey> & BaseDbItemInterface<TKey>, TValue extends BaseDbItemInterface<TValue>>  extends IndexNode<TKey,TValue>  {
    protected Object[] children;

    public InnerNode(TKey keyType, TValue valueType) {
        super(keyType, valueType);
        this.allKeys = new Object[KEY_SIZE];
        this.children = new Object[CHILDREN_SIZE];
    }

    //#region bTree
    @SuppressWarnings("unchecked")
    public IndexNode<TKey,TValue> getChild(int index) {
        IndexNode<TKey,TValue> node = (IndexNode<TKey,TValue>)this.children[index];
        if(node != null && !node.isLoaded){
            node.load();
        }
        return node;
    }

    public void setChild(int index, IndexNode<TKey,TValue> child) {
        this.children[index] = child;
        if (child != null)
            child.setParent(this);
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.InnerNode;
    }

    @Override
    public int search(TKey key) {
        int index = 0;
        int keyCount = this.getKeyCount();
        for (index = 0; index + 1 < keyCount;) {

            int cmp = this.getKey(index).compareTo(key);
            if (cmp == 0) {
                return index + 1;
            }
            else if (cmp > 0) {
                return index;
            }
            index++;
        }

        return index;
    }


    /* The codes below are used to support insertion operation */

    private void insertAt(int index, TKey key, IndexNode<TKey,TValue> leftChild, IndexNode<TKey,TValue> rightChild) {
        // move space for the new key
        for (int i = this.getKeyCount() + 1; i > index; --i) {
            this.setChild(i, this.getChild(i - 1));
        }
        for (int i = this.getKeyCount(); i > index; --i) {
            this.setKey(i, this.getKey(i - 1));
        }

        // insert the new key
        this.setKey(index, key);
        this.setChild(index, leftChild);
        this.setChild(index + 1, rightChild);
        this.keyCount += 1;
    }

    /**
     * When splits a internal node, the middle key is kicked out and be pushed to parent node.
     */
    @Override
    protected IndexNode<TKey,TValue> split() {
        int midIndex = this.getKeyCount() / 2;

        InnerNode<TKey,TValue> newRNode = this.newInstance();
        for (int i = midIndex + 1; i < this.getKeyCount(); ++i) {
            newRNode.setKey(i - midIndex - 1, this.getKey(i));
            this.setKey(i, null);
        }
        for (int i = midIndex + 1; i <= this.getKeyCount(); ++i) {
            newRNode.setChild(i - midIndex - 1, this.getChild(i));
            newRNode.getChild(i - midIndex - 1).setParent(newRNode);
            this.setChild(i, null);
        }
        this.setKey(midIndex, null);
        newRNode.keyCount = this.getKeyCount() - midIndex - 1;
        this.keyCount = midIndex;

        return newRNode;
    }

    @Override
    protected IndexNode<TKey,TValue> pushUpKey(TKey key, IndexNode<TKey,TValue> leftChild, IndexNode<TKey,TValue> rightNode) {
        // find the target position of the new key
        int index = this.search(key);

        // insert the new key
        this.insertAt(index, key, leftChild, rightNode);

        // check whether current node need to be split
        if (this.isOverflow()) {
            return this.dealOverflow();
        }
        else {
            return this.getParent() == null ? this : null;
        }
    }


    public IndexNode<TKey,TValue> dealOverflow() {
        int midIndex = this.getKeyCount() / 2;
        TKey upKey = this.getKey(midIndex);

        IndexNode<TKey,TValue> newRNode = this.split();

        if (this.getParent() == null) {
            this.setParent(new InnerNode<TKey,TValue>(this.keyType, this.valueType));
        }
        newRNode.setParent(this.getParent());

        // maintain links of sibling nodes
        newRNode.setLeftSibling(this);
        newRNode.setRightSibling(this.rightSibling);
        if (this.getRightSibling() != null)
            this.getRightSibling().setLeftSibling(newRNode);
        this.setRightSibling(newRNode);

        // push up a key to parent internal node
        return this.getParent().pushUpKey(upKey, this, newRNode);
    }


    /* The codes below are used to support delete operation */

    private void deleteAt(int index) {
        int i = 0;
        for (i = index; i < this.getKeyCount() - 1; ++i) {
            this.setKey(i, this.getKey(i + 1));
            this.setChild(i + 1, this.getChild(i + 2));
        }
        this.setKey(i, null);
        this.setChild(i + 1, null);
        --this.keyCount;
    }


    @Override
    protected void processChildrenTransfer(IndexNode<TKey,TValue> borrower, IndexNode<TKey,TValue> lender, int borrowIndex) {
        int borrowerChildIndex = 0;
        while (borrowerChildIndex < this.getKeyCount() + 1 && this.getChild(borrowerChildIndex) != borrower)
            ++borrowerChildIndex;

        if (borrowIndex == 0) {
            // borrow a key from right sibling
            TKey upKey = borrower.transferFromSibling(this.getKey(borrowerChildIndex), lender, borrowIndex);
            this.setKey(borrowerChildIndex, upKey);
        }
        else {
            // borrow a key from left sibling
            TKey upKey = borrower.transferFromSibling(this.getKey(borrowerChildIndex - 1), lender, borrowIndex);
            this.setKey(borrowerChildIndex - 1, upKey);
        }
    }

    @Override
    protected IndexNode<TKey,TValue> processChildrenFusion(IndexNode<TKey,TValue> leftChild, IndexNode<TKey,TValue> rightChild) {
        int index = 0;
        while (index < this.getKeyCount() && this.getChild(index) != leftChild)
            ++index;
        TKey sinkKey = this.getKey(index);

        // merge two children and the sink key into the left child node
        leftChild.fusionWithSibling(sinkKey, rightChild);

        // remove the sink key, keep the left child and abandon the right child
        this.deleteAt(index);

        // check whether need to propagate borrow or fusion to parent
        if (this.isUnderflow()) {
            if (this.getParent() == null) {
                // current node is root, only remove keys or delete the whole root node
                if (this.getKeyCount() == 0) {
                    leftChild.setParent(null);
                    return leftChild;
                }
                else {
                    return null;
                }
            }

            return this.dealUnderflow();
        }

        return null;
    }


    @Override
    protected void fusionWithSibling(TKey sinkKey, IndexNode<TKey,TValue> rightSibling) {
        InnerNode<TKey,TValue> rightSiblingNode = (InnerNode<TKey,TValue>)rightSibling;

        int j = this.getKeyCount();
        this.setKey(j++, sinkKey);

        for (int i = 0; i < rightSiblingNode.getKeyCount(); ++i) {
            this.setKey(j + i, rightSiblingNode.getKey(i));
        }
        for (int i = 0; i < rightSiblingNode.getKeyCount() + 1; ++i) {
            this.setChild(j + i, rightSiblingNode.getChild(i));
        }
        this.keyCount += 1 + rightSiblingNode.getKeyCount();

        this.setRightSibling(rightSiblingNode.rightSibling);
        if (rightSiblingNode.rightSibling != null)
            rightSiblingNode.rightSibling.setLeftSibling(this);
    }

    @Override
    protected TKey transferFromSibling(TKey sinkKey, IndexNode<TKey,TValue> sibling, int borrowIndex) {
        InnerNode<TKey,TValue> siblingNode = (InnerNode<TKey,TValue>)sibling;

        TKey upKey = null;
        if (borrowIndex == 0) {
            // borrow the first key from right sibling, append it to tail
            int index = this.getKeyCount();
            this.setKey(index, sinkKey);
            this.setChild(index + 1, siblingNode.getChild(borrowIndex));
            this.keyCount += 1;

            upKey = siblingNode.getKey(0);
            siblingNode.deleteAt(borrowIndex);
        }
        else {
            // borrow the last key from left sibling, insert it to head
            this.insertAt(0, sinkKey, siblingNode.getChild(borrowIndex + 1), this.getChild(0));
            upKey = siblingNode.getKey(borrowIndex);
            siblingNode.deleteAt(borrowIndex);
        }

        return upKey;
    }

    //#endregion

    protected int childListSize(){
        return CHILDREN_SIZE * NODE_LOOKUP_SIZE;
    }

    protected int childListOffset() {
        return super.getSize();
    }

    @Override
    public int getSize() {
        return super.getSize() + this.childListSize();
    }

    @Override
    InnerNode<TKey,TValue> newInstance() {
        return new InnerNode<TKey,TValue>(this.keyType,this.valueType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public InnerNode<TKey, TValue> deserialize(byte[] DATA) throws UnsupportedEncodingException {
        super.deserialize(DATA);
        int offset = this.childListOffset();
        int index = 0;
        while(offset + NODE_LOOKUP_SIZE < DATA.length){
            this.children[index] = this.initialize(
                    this.deserializeNullCheck(DATA, offset),
                    this.deserializeLookupKey(DATA, offset),
                    this.deserializeNodeType(DATA, offset)
            );
            offset += NODE_LOOKUP_SIZE;
            index += 1;
        }
        return (InnerNode<TKey,TValue>) this.clone();
    }


    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize() throws UnsupportedEncodingException {
        byte[] DATA = new byte[this.getSize()];
        Serialize.bytes(super.serialize(), super.getSize(), 0, DATA);

        int offset = this.childListOffset();
        for( Object obj :  this.children){
            IndexNode<TKey,TValue> item = (IndexNode<TKey,TValue>) obj;
            byte[] TMP = this.serializeLookup(item);
            if(offset + TMP.length > this.getSize()){
                throw new UnsupportedEncodingException("Serialize Array Error Array Size > Specified Size");
            }
            System.arraycopy(TMP, 0, DATA, offset, TMP.length);
            offset += TMP.length;
        }
        return DATA;
    }

    @Override
    public String toString() {
        return String.format("(inner) size: %d  key %s ", this.getSize(), this.key.toString());
    }

    @Override
    public String detailedJsonString() {
        return Serialize.arrayToString(this.children, false);

    }

    @SuppressWarnings("unchecked")
    @Override
    public void fillIterator(Stats<TKey,TValue> items ,int depth) {
        items.addNode(this, depth);
        for (int i = 0; i < this.children.length; i ++) {
            Object obj = this.getChild(i);
            if(obj != null){
                IndexNode<TKey,TValue> node = (IndexNode<TKey,TValue>) obj;
                node.fillIterator(items, depth + 1);
            }
        }
    }

    @Override
    public IndexNode<TKey, TValue> load() {
        return this.loader.loadInnerNode(this);
    }
}
