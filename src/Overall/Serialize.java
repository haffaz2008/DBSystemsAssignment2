package Overall;

import BaseClasses.BaseDbItemInterface;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;



public class Serialize {
    public static String ENCODING = "utf-8";

    /**
     * Will copy an array of bytes to the provided byte[] at the given offset if the size of the entry[].length > the size those bytes will be dropped
     * @param entry
     * @param size
     * @param OFFSET
     * @param rec
     */
    public static void bytes(byte[] entry, int size , int OFFSET, byte[] rec){
        System.arraycopy(entry, 0, rec, OFFSET, entry.length);
    }

    /**
     * Will copy a string to a byte[] if the string is larger than the size provided those bytes will be lost
     * @param entry
     * @param size
     * @param OFFSET
     * @param rec
     * @throws UnsupportedEncodingException
     */
    public static void string(String entry, int size, int OFFSET, byte[] rec)
            throws UnsupportedEncodingException
    {
        //System.out.println("copyString("+entry+","+size+","+OFFSET+")");
        byte[] DATA = Serialize.string(entry, size);
        System.arraycopy(DATA, 0, rec, OFFSET, DATA.length);
    }

    /**
     * Will first convert String to in then copy that int to a byte[] at the given offset
     * @param entryS
     * @param size
     * @param OFFSET
     * @param rec
     * @throws UnsupportedEncodingException
     */
    public static void integer(String entryS, int size, int OFFSET, byte[] rec)
            throws UnsupportedEncodingException
    {
        int entry = Integer.parseInt(entryS);
        integer(entry, size, OFFSET, rec);
    }

    /**
     * Copies int to byte[] at the given offset
     * @param entry
     * @param size
     * @param OFFSET
     * @param rec
     */
    public static void integer(int entry, int size, int OFFSET, byte[] rec)
    {
        byte[] DATA = Serialize.integer(entry, size);
        System.arraycopy(DATA, 0, rec, OFFSET, DATA.length);
    }

    /**
     * Converts int to byte[]
     * @param entry
     * @param size
     * @return
     */
    public static byte[] integer(int entry, int size)
    {
        ByteBuffer bBuffer =ByteBuffer.allocate(size);
        bBuffer.putInt(entry);
        return bBuffer.array();
    }

    /**
     * Converts String To Byte[]
     * @param entry
     * @param size
     * @return
     * @throws UnsupportedEncodingException
     */
    public static byte[] string(String entry, int size)
            throws UnsupportedEncodingException
    {
        byte[] DATA = new byte[size];
        byte[] SRC = entry.trim().getBytes(ENCODING);
        if (entry != "")
        {
            System.arraycopy(SRC, 0,
                    DATA, 0, SRC.length);
        }
        return DATA;
    }

    /**
     * This will convert an array of Idbentities to a byte[] and will throw if the array is larger than the size
     * @param <T extends Idbentity<T> >
     * @param items
     * @param ////size
     * @return
     * @throws UnsupportedEncodingException
     */
    @SuppressWarnings("unchecked")
    public static <T extends BaseDbItemInterface<T>> byte[]  array(Object[] items, int size)
            throws UnsupportedEncodingException
    {
        byte[] DATA = new byte[size];
        int offset = 0;
        for(Object item : items){
            if(item == null){ return DATA; }
            byte[] TMP = ((T) item).serialize();
            if(offset + TMP.length > size){
                throw new UnsupportedEncodingException(String.format("Array serialize out of bounds items[%d] size[%d] itemSize[%d] sizeRequired[%d]", items.length, size, TMP.length, TMP.length * items.length));
            }
            System.arraycopy(TMP, 0, DATA, offset, TMP.length);
            offset += TMP.length;
        }
        return DATA;
    }


    public static <T extends Object> String arrayToString(T[] items, boolean returns){
        String tmp = "[";
        if(returns){tmp += "\n";}
        String token = "";
        for(T val: items){
            if(val == null){
                tmp += token + "[null]";
            }else{
                tmp += token + val.toString();
            }
            token = ",";
            if(returns){ token =token + "\n   ";}
        }
        tmp += "]";
        return tmp;
    }
}


