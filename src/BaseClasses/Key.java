package BaseClasses;

import Overall.Deserialize;
import Overall.Serialize;

import java.io.UnsupportedEncodingException;

public class Key implements BaseDbItemInterface<Key>{
    public static final int RID_SIZE = 4;
    public static final int PID_SIZE = 4;
    public static final int RID_OFFSET = 0;
    public static final int PID_OFFSET = 4;
    public static final int CONTENT_SIZE = RID_SIZE + PID_SIZE;
    private int rId;
    private int pageId;

    public Key(){}
    public Key(int pageId, int rid) {
        this.rId = rid;
        this.pageId =pageId;
    }
    public int getRId(){
        return this.rId;
    }

    public int getPageId() {
        return this.pageId;
    }

    public int getIndex(int pageSize, int recordSize){
        return this.rId * recordSize + this.pageId * pageSize;
    }

    @Override
    public int getSize() {
        return RID_SIZE + PID_SIZE;
    }

    @Override
    public byte[] serialize() throws UnsupportedEncodingException {
        byte[] record = new byte[this.getSize()];
        Serialize.integer( this.rId , RID_SIZE, RID_OFFSET , record);
        Serialize.integer(this.pageId, PID_SIZE, PID_OFFSET, record);
        return record;
    }

    @Override
    public Key deserialize(byte[] DATA) throws UnsupportedEncodingException {
        Key key = new Key();
        key.rId = Deserialize.integer(DATA, RID_SIZE, RID_OFFSET);
        key.pageId = Deserialize.integer(DATA, PID_SIZE, PID_OFFSET);
        return key;
    }
    @Override
    public String toString() {
        return String.format("JSON(dbkey)=>{ \"rId\": %d , \"pageId\": %d }", this.rId, this.pageId);
    }
    public String toJsonString() {
        return String.format(
                "{rId:%d,pageId:%d}"
                , this.rId, this.pageId);
    }
}
