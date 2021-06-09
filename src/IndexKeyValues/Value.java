package IndexKeyValues;

import BaseClasses.BaseDbItemInterface;
import BaseClasses.Key;
import Overall.Deserialize;
import Overall.Serialize;

import java.io.UnsupportedEncodingException;

public class Value implements BaseDbItemInterface<Value> {
    public Key searchKey;

    public Value (int pageId , int rid)
    {
        this.searchKey = new Key(pageId,rid);
    }
    public Value (Key searchKey)
    {
        this.searchKey = searchKey;
    }

    public Key getKey()
    {return this.searchKey;}

    @Override
    public int getSize() {
        return 8;
    }

    @Override
    public byte[] serialize() throws UnsupportedEncodingException {
        byte[] DATA = new byte[this.getSize()];
        Serialize.bytes(this.searchKey.serialize(), this.searchKey.getSize(), 0, DATA);
        return DATA;
    }

    @Override
    public Value deserialize(byte[] DATA) throws UnsupportedEncodingException {
        return new Value(new Key().deserialize(
                Deserialize.bytes(DATA, Key.CONTENT_SIZE, 0)
        ));
    }
    @Override
    public String toString() {
        return String.format("{ \"rId\": %d , \"pageId\": %d }", this.searchKey.getRId(), this.searchKey.getPageId());
    }
}
