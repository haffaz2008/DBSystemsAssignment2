package DummyDBClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DummyDBCreator {
    private String datastorePath = "";
    private long datastoreSize = -1;
    private RandomAccessFile raf;

    public DummyDBCreator(String datastorePath) {
        this.datastorePath = datastorePath;
    }

    public long getDataStoreSize() {

        if(this.datastoreSize !=  -1 ) { return this.datastoreSize; }
        try {
            this.datastoreSize = raf.length();
        } catch( IOException e){
            System.out.println("Error Loading Data Store File Size. "+e.toString());
        }
        return this.datastoreSize;
    }

    public void connect()
            throws FileNotFoundException
    {
        if(!this.isInitialized()){
            File file = new File(this.datastorePath);
            this.raf = new RandomAccessFile(file, "rw");
            this.datastoreSize = this.getDataStoreSize();
        }
    }

    public boolean isInitialized(){
        return this.raf != null;
    }

    public void close()
    {
        if(this.raf != null){
            try{
                this.raf.close();
                this.raf = null;
            }catch(IOException e){
                System.out.println("Error Closing Connection ["+e.toString()+"]");
            }
        }
    }

    public byte[] read(long index, int size)
            throws IOException
    {
        this.raf.seek(index);
        byte[] DATA = new byte[size];
        this.raf.read(DATA);
        return DATA;
    }
    public void write(long index, byte[] data)
            throws IOException
    {
        this.raf.seek(index);
        this.raf.write(data);
    }
    public void append(byte[] data)
            throws IOException
    {
        this.raf.seek(this.datastoreSize);
        this.raf.write(data);
        this.datastoreSize += data.length;
    }

    public void swap(long fromIndex,long toIndex, int size)
            throws IOException
    {
        byte[] src = this.read(fromIndex, size);
        byte[] dest = this.read(toIndex, size);
        this.write(toIndex, src);
        this.write(fromIndex, dest);
    }

    public void truncate(int size)
            throws IOException
    {
        this.raf.setLength(this.datastoreSize - size);
        this.datastoreSize -= size;
    }

}
