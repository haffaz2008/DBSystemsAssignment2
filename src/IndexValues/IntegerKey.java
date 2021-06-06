package IndexValues;

import java.io.UnsupportedEncodingException;
import Overall.Deserialize;
import Overall.Serialize;

public class IntegerKey implements Comparable<IntegerKey> {
    private int value;
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
    public int compareTo(IntegerKey o) {
        return 0;
    }
}
