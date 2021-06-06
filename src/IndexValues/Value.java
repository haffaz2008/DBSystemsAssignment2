package IndexValues;

import BaseClasses.BaseDbItem;

import java.io.UnsupportedEncodingException;

public class Value implements BaseDbItem<Value> {
    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public byte[] serialize() throws UnsupportedEncodingException {
        return new byte[0];
    }

    @Override
    public Value deserialize(byte[] DATA) throws UnsupportedEncodingException {
        return null;
    }
}
