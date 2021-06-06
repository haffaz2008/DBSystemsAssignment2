package BaseClasses;

import java.io.UnsupportedEncodingException;

public interface BaseDbItem<T> extends Cloneable {
    public int getSize();
    public byte[] serialize() throws UnsupportedEncodingException;
    public T deserialize(byte[] DATA) throws UnsupportedEncodingException;
}
