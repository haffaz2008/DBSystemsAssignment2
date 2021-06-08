package IndexKeyValues;

import java.io.UnsupportedEncodingException;

import BaseClasses.BaseDbItemInterface;
import Overall.Deserialize;
import Overall.Serialize;

public class IntegerKey implements BaseDbItemInterface<IntegerKey>,Comparable<IntegerKey> {
    private Integer value;
    public IntegerKey(int value)
    {
        this.value =value;
    }

    public int getSize() {
        return 4;
    }

    public byte[] serialize() throws UnsupportedEncodingException {
        return Serialize.integer(this.value, this.getSize());
    }

    @Override
    public IntegerKey deserialize(byte[] DATA) throws UnsupportedEncodingException {
        return new IntegerKey(Deserialize.integer(DATA));
    }

    @Override
    public int compareTo(IntegerKey o) {
        return this.value.compareTo(o.value);
    }
}
