package DummyDBClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DummyDBCreator {
    private String datastorePath = "";
    private long datastoreSize = -1;
    private RandomAccessFile file;

    public DummyDBCreator(String datastorePath) {
        this.datastorePath = datastorePath;
    }

    public long getDataStoreSize() {

        if(this.datastoreSize !=  -1 ) { return this.datastoreSize; }
        try {
            this.datastoreSize = file.length();
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
            this.file = new RandomAccessFile(file, "rw");
            this.datastoreSize = this.getDataStoreSize();
        }
    }

    public boolean isInitialized(){
        return this.file != null;
    }

    public void close()
    {
        if(this.file != null){
            try{
                this.file.close();
                this.file = null;
            }catch(IOException e){
                System.out.println("Error Closing Connection ["+e.toString()+"]");
            }
        }
    }

    public byte[] read(long index, int size)
            throws IOException
    {
        this.file.seek(index);
        byte[] DATA = new byte[size];
        this.file.read(DATA);
        return DATA;
    }
    public void write(long index, byte[] data)
            throws IOException
    {
        this.file.seek(index);
        this.file.write(data);
    }
    public void append(byte[] data)
            throws IOException
    {
        this.file.seek(this.datastoreSize);
        this.file.write(data);
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
        this.file.setLength(this.datastoreSize - size);
        this.datastoreSize -= size;
    }

}
