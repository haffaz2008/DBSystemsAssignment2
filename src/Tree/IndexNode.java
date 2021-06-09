package Tree;

import BaseClasses.BaseDbItemInterface;
import BaseClasses.BaseItem;
import BaseClasses.Key;
import Overall.Deserialize;
import Overall.Serialize;

import java.io.UnsupportedEncodingException;

public abstract class IndexNode <TKey extends Comparable<TKey> & BaseDbItemInterface<TKey>, TValue extends BaseDbItemInterface<TValue>>  extends BaseItem<IndexNode<TKey,TValue>> {
    protected final static int LEAFORDER = 3500;
    protected final static int INNERORDER = 3500;
    protected final static int KEY_SIZE = INNERORDER + 1;
    protected final static int CHILDREN_SIZE = INNERORDER + 2;
    protected final static int VALUE_SIZE = KEY_SIZE;
    public static final int NODE_TYPE_SIZE = 4;
    public static final int NODE_NULL_CHECK_SIZE = 4;
    public static final int NODE_LOOKUP_SIZE = NODE_TYPE_SIZE + NODE_NULL_CHECK_SIZE + Key.CONTENT_SIZE;

    public static final int NODE_NULL_CHECK_OFFSET = 0;
    public static final int NODE_TYPE_OFFSET = NODE_NULL_CHECK_OFFSET + NODE_NULL_CHECK_SIZE;
    public static final int NODE_LOOKUP_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;

    public static final int PARENT_NODE_SIZE = NODE_LOOKUP_SIZE;
    public static final int LEFT_SIBLING_SIZE = NODE_LOOKUP_SIZE;
    public static final int RIGHT_SIBLING_SIZE = NODE_LOOKUP_SIZE;

    public static final int PARENT_NODE_OFFSET = 0;
    public static final int LEFT_SIBLING_OFFSET = PARENT_NODE_OFFSET + PARENT_NODE_SIZE;
    public static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + LEFT_SIBLING_SIZE;
    public static final int KEYS_OFFSET = RIGHT_SIBLING_OFFSET + RIGHT_SIBLING_SIZE;

    protected TKey keyType;
    protected TValue valueType;
    protected IndexLoader<TKey, TValue> loader;
    protected boolean isLoaded = true;
    protected Object[] allKeys;
    protected int keyCount;
    protected IndexNode<TKey,TValue> parentNode;
    protected IndexNode<TKey,TValue> leftSibling;
    protected IndexNode<TKey,TValue> rightSibling;


    public int getKeyCount() {
        if(this.keyCount == -1){
            this.keyCount = 0;
            for(Object key: this.allKeys){
                if(key == null){ return this.keyCount; }
                this.keyCount += 1;
            }
        }
        return this.keyCount;
    }

    public TKey getKey(int index) {
        return (TKey)this.allKeys[index];
    }
    public void setKey(int index, TKey key) {
        this.allKeys[index] = key;
    }
    public IndexNode<TKey,TValue> getParent() {
        return this.parentNode;
    }
    public void setParent(IndexNode<TKey,TValue> parent) {
        this.parentNode = parent;
    }
    public abstract NodeType getNodeType();
    public abstract int search(TKey key);
    public boolean isOverflow() {
        return this.getKeyCount() == this.allKeys.length;
    }
    protected abstract IndexNode<TKey,TValue> split();
    protected abstract IndexNode<TKey,TValue> pushUpKey(TKey key, IndexNode<TKey,TValue> leftChild, IndexNode<TKey,TValue> rightNode);
    public boolean isUnderflow() {
        return this.getKeyCount() < (this.allKeys.length / 2);
    }
    public boolean canLendAKey() {
        return this.getKeyCount() > (this.allKeys.length / 2);
    }
    public IndexNode<TKey,TValue> getLeftSibling() {
        if (this.leftSibling != null && this.leftSibling.getParent() == this.getParent())
            return this.leftSibling;
        return null;
    }
    public void setLeftSibling(IndexNode<TKey,TValue> sibling) {
        this.leftSibling = sibling;
    }

    public IndexNode<TKey,TValue> getRightSibling() {
        if (this.rightSibling != null && this.rightSibling.getParent() == this.getParent())
            return this.rightSibling;
        return null;
    }
    public void setRightSibling(IndexNode<TKey,TValue> silbling) {
        this.rightSibling = silbling;
    }

    public IndexNode<TKey,TValue> dealUnderflow() {
        if (this.getParent() == null)
            return null;
        IndexNode<TKey,TValue> leftSibling = this.getLeftSibling();
        if (leftSibling != null && leftSibling.canLendAKey()) {
            this.getParent().moveChild(this, leftSibling, leftSibling.getKeyCount() - 1);
            return null;
        }
        IndexNode<TKey,TValue> rightSibling = this.getRightSibling();
        if (rightSibling != null && rightSibling.canLendAKey()) {
            this.getParent().moveChild(this, rightSibling, 0);
            return null;
        }
        if (leftSibling != null) {
            return this.getParent().childMerge(leftSibling, this);
        }
        else {
            return this.getParent().childMerge(this, rightSibling);
        }
    }

    protected abstract void moveChild(IndexNode<TKey,TValue> borrower, IndexNode<TKey,TValue> lender, int borrowIndex);

    protected abstract IndexNode<TKey,TValue> childMerge(IndexNode<TKey,TValue> leftChild, IndexNode<TKey,TValue> rightChild);

    protected abstract void mergeWithNeighbour(TKey sinkKey, IndexNode<TKey,TValue> rightSibling);

    protected abstract TKey getFromNeighbour(TKey sinkKey, IndexNode<TKey,TValue> sibling, int borrowIndex);
    public IndexNode<TKey,TValue> dealOverflow() {
        int midIndex = this.getKeyCount() / 2;
        TKey upKey = this.getKey(midIndex);
        IndexNode<TKey,TValue> newRNode = this.split();
        if (this.getParent() == null) {
            this.setParent(new InnerNode<TKey,TValue>(this.keyType, this.valueType));
        }
        newRNode.setParent(this.getParent());
        newRNode.setLeftSibling(this);
        newRNode.setRightSibling(this.rightSibling);
        if (this.getRightSibling() != null)
            this.getRightSibling().setLeftSibling(newRNode);
        this.setRightSibling(newRNode);
        return this.getParent().pushUpKey(upKey, this, newRNode);
    }
    abstract IndexNode<TKey,TValue> newInstance();
    IndexNode( TKey keyType, TValue valueType) {
        super();
        this.keyType = keyType;
        this.keyCount = 0;
        this.valueType = valueType;
        this.parentNode = null;
        this.leftSibling = null;
        this.rightSibling = null;
    }
    protected int keyListSize(){
        return KEY_SIZE * this.keyType.getSize();
    }
    @Override
    public int getSize() {
        return this.keyListSize() + NODE_LOOKUP_SIZE * 3;
    }
    public abstract IndexNode<TKey,TValue> load();
    public IndexNode<TKey,TValue> initialize(boolean isNull, Key key, NodeType nodeType){
        if(isNull) { return null; }
        IndexNode<TKey,TValue> node = null;
        if(nodeType == NodeType.LeafNode){
            if(this.loader.leafStore.cache.containsKey(key)){
                node = this.loader.leafStore.cache.get(key);
            }else{
                node = new Leaf<TKey, TValue>(this.keyType, this.valueType);
                node.key = key;
                node.loader = this.loader;
                node.isLoaded = false;
                this.loader.leafStore.cache.put(key, node);
            }
        }else if(nodeType == NodeType.InnerNode){
            if(this.loader.innerStore.cache.containsKey(key)){
                node = this.loader.innerStore.cache.get(key);
            }else{
                node = new InnerNode<TKey, TValue>(this.keyType, this.valueType);
                node.key = key;
                node.loader = this.loader;
                node.isLoaded = false;
                this.loader.innerStore.cache.put(key, node);
            }
        }
        return node;
    }
    protected byte[] serializeLookup(IndexNode<TKey, TValue> node)
            throws UnsupportedEncodingException
    {
        byte[] DATA = new byte[NODE_LOOKUP_SIZE];
        if(node == null){ return DATA; }
        Serialize.integer(1, NODE_NULL_CHECK_SIZE , NODE_NULL_CHECK_OFFSET, DATA);
        Serialize.integer(node.getNodeType().toInt(), NODE_TYPE_SIZE, NODE_TYPE_OFFSET,DATA);
        Serialize.bytes(node.getKey().serialize(), node.getKey().getSize(), NODE_LOOKUP_OFFSET, DATA);
        return DATA;
    }

    @Override
    public byte[] serialize() throws UnsupportedEncodingException {
        byte[] DATA = new byte[this.getSize()];
        Serialize.bytes(this.serializeLookup(this.parentNode), PARENT_NODE_SIZE, PARENT_NODE_OFFSET, DATA);
        Serialize.bytes(this.serializeLookup(this.leftSibling), LEFT_SIBLING_SIZE, LEFT_SIBLING_OFFSET, DATA);
        Serialize.bytes(this.serializeLookup(this.rightSibling), RIGHT_SIBLING_SIZE, RIGHT_SIBLING_OFFSET, DATA);
        Serialize.bytes(
                Serialize.array(this.allKeys, this.keyListSize() ),
                this.keyListSize(),
                KEYS_OFFSET,
                DATA
        );
        return DATA;
    }
    protected boolean deserializeNullCheck(byte[] rec, int offset)
            throws UnsupportedEncodingException
    {
        int val = Deserialize.integer(rec, NODE_NULL_CHECK_SIZE, offset + NODE_NULL_CHECK_OFFSET);
        return val == 0;
    }
    protected NodeType deserializeNodeType(byte[] rec, int offset)
            throws UnsupportedEncodingException
    {
        return NodeType.fromInt(
                Deserialize.integer(rec, NODE_TYPE_SIZE, offset + NODE_TYPE_OFFSET)
        );
    }

    protected Key deserializeLookupKey(byte[] rec, int offset)
            throws UnsupportedEncodingException
    {
        return new Key().deserialize(
                Deserialize.bytes(rec, NODE_LOOKUP_SIZE, offset + NODE_LOOKUP_OFFSET)
        );
    }
    @SuppressWarnings("unchecked")
    @Override
    public IndexNode<TKey,TValue> deserialize(byte[] DATA) throws UnsupportedEncodingException {
        this.parentNode = this.initialize(
                this.deserializeNullCheck(DATA, PARENT_NODE_OFFSET),
                this.deserializeLookupKey(DATA, PARENT_NODE_OFFSET),
                this.deserializeNodeType(DATA, PARENT_NODE_OFFSET)
        );
        this.leftSibling = this.initialize(
                this.deserializeNullCheck(DATA, LEFT_SIBLING_OFFSET),
                this.deserializeLookupKey(DATA, LEFT_SIBLING_OFFSET),
                this.deserializeNodeType(DATA, LEFT_SIBLING_OFFSET)
        );
        this.rightSibling = this.initialize(
                this.deserializeNullCheck(DATA, RIGHT_SIBLING_OFFSET),
                this.deserializeLookupKey(DATA, RIGHT_SIBLING_OFFSET),
                this.deserializeNodeType(DATA, RIGHT_SIBLING_OFFSET)
        );
        this.allKeys = Deserialize.array(
                Deserialize.bytes(DATA, this.keyListSize(), KEYS_OFFSET),
                this.keyType
        );
        this.keyCount = -1;
        this.keyCount = this.getKeyCount();

        return (IndexNode<TKey,TValue>) this.clone();
    }
    public String lookUpInFile(IndexNode<TKey, TValue> node){
        if(node == null) return "{dbKey:null,type:null}";
        return "{ dbKey:"+ node.key.toDisplayString()+ ",type:"+node.getNodeType() +" }";
    }

    public String toReadableString(){
        return "{"+
                " ,dbKey:"+this.key.toDisplayString()+
                " ,type:"+this.getNodeType()+ ""+
                " ,parentNode:"+this.lookUpInFile(this.parentNode)+
                " ,keyCount:"+this.keyCount+""+
                "}";
    }

    public abstract String detailedReadableString();
    @Override
    public boolean equals(Object obj) {
        if(obj == this){
            return true;
        }
        if(obj instanceof IndexNode<?,?>){
            try {
                IndexNode<?,?> casted = (IndexNode<?,?>) obj;
                if(casted.getNodeType() == this.getNodeType() && super.equals(obj)){
                    return true;
                }
            }catch(Exception e){}
        }
        return false;
    }

}
