package Overall;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;


public class Deserialize {

    /**
     * This will copy the bytes out of the record and try and convert those bytes to an int
     * @param record
     * @param size
     * @param offset
     * @return int
     * @throws UnsupportedEncodingException
     */
    public static int integer(byte[] record, int size, int offset )
            throws UnsupportedEncodingException
    {
        byte[] OUT = new byte[size];
        System.arraycopy(record, offset , OUT, 0, size);
        return Deserialize.integer(OUT);
    }

    /**
     * This will convert a byte[] to an integer
     * @param record
     * @return int
     */
    public static int integer(byte[] record){
        return ByteBuffer.wrap(record).getInt();
    }

    /**
     * This will copy the bytes out of the record and try and convert those bytes to a string
     * @param record
     * @param size
     * @param offset
     * @return String
     * @throws UnsupportedEncodingException
     */
    public static String string(byte[] record, int size, int offset)
            throws UnsupportedEncodingException
    {
        byte[] OUT = new byte[size];
        System.arraycopy(record, offset , OUT, 0 , size);
        return Deserialize.string(OUT);

    }

    /**
     * This will convert a byte[] to a string
     * @param record
     * @return String
     */
    public static String string(byte[] record){
        return new String(record);
    }

    /**
     * This will copy a sub byte[] out of the record
     * @param record
     * @param size
     * @param offset
     * @return sub byte[]
     */
    public static byte[] bytes(byte[] record, int size, int offset){
        byte[] OUT = new byte[size];
        System.arraycopy(record, offset, OUT, 0, size);
        return OUT;
    }


    /**
     * This will convert an array of  to a byte[] and will throw if the array is larger than the size
     * @param <T extends Idbentity<T> >
     * @param ///items
     * @param ///size
     * @return
     * @throws UnsupportedEncodingException
     */
//    public static <T extends Idbentity<T> & Cloneable > Object[]  array(byte[] record, T type)
//            throws UnsupportedEncodingException
//    {
//        Object[] items = new Object[record.length / type.getSize()];
//        int offset = 0;
//        int index = 0;
//        while(offset + type.getSize() < record.length){
//            byte[] TMP = new byte[type.getSize()];
//            System.arraycopy(record, offset, TMP, 0, TMP.length);
//            items[index] = type.deserialize(TMP);
//            offset += TMP.length;
//            index += 1;
//        }
//        return items;
//    }



}
