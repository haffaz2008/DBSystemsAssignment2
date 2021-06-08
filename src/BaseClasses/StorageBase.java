package BaseClasses;

public abstract class StorageBase<T> implements StorageBaseItem<T> {
    public static final int EOF_PAGENUM_SIZE = 4;
    public static final int DB_META_PADDING = EOF_PAGENUM_SIZE;
    private int pageSize;
    public Key key;
    public Object clone() {
        try{
            return super.clone();
        } catch( CloneNotSupportedException e){
            // Handel Exception here to stop the need for exception passing through all functions
            System.out.println(e);
            System.exit(1);
            return new Object();
        }
    }
    public StorageBase() {
        this.key = new Key(0, 0);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this){
            return true;
        }
        if(obj instanceof StorageBase){
            try {
                StorageBase<?> cast = (StorageBase<?>) obj;
                if(cast.key.getPageId() == this.key.getPageId() && cast.key.getRId() == this.key.getRId()){
                    return true;
                }
            }catch(Exception e){}
        }
        return false;
    }
    @Override
    public long getIndex() {
        return this.key.getIndex(this.getSize(), pageSize);
    }

    public Key getKey() {
        return this.key ;
    }

    public void setKey(Key key) {
        this.key = key;
    }

}
