package IndexKeyValues;

import BaseClasses.BaseDbItemInterface;
import Overall.Deserialize;
import Overall.Serialize;

import java.io.UnsupportedEncodingException;

public class StringKey implements BaseDbItemInterface<StringKey>,Comparable<StringKey> {
    private String value;
    public StringKey(String value){
        this.value = value;
    }





    @Override
    public int getSize() {
        return 38;
    }

    @Override
    public byte[] serialize() throws UnsupportedEncodingException {
        return Serialize.string(this.value, this.getSize());
    }

    @Override
    public StringKey deserialize(byte[] DATA) throws UnsupportedEncodingException {
        return new StringKey(Deserialize.string(DATA));
    }

    @Override
    public int compareTo(StringKey o) {
        return this.value.compareTo(o.value);
    }
    @Override
    public String toString() {
        return "\""+this.value+"\"";
    }
}
