package ItemSerializer;

import BaseClasses.Key;
import BaseClasses.StorageBase;
import Overall.Serialize;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BaseItemSerializer <StorageItem extends StorageBase<StorageItem>> extends StorageBase<BaseItemSerializer<StorageItem>> {
    public List<StorageItem> storageItems = new ArrayList<StorageItem>();
    protected StorageItem type;
    protected int size;

    public BaseItemSerializer(int size, StorageItem type){
        super();
        this.type = type;
        this.size = size;

    }

    @Override
    public byte[] serialize() throws UnsupportedEncodingException {
        byte[] PAGE = new byte[this.getSize()];

        int recordOffset = 0;
        int recordId = 0;
        for(StorageItem entity : this.storageItems){
            entity.setKey(new Key(this.key.getPageId(),recordId));
            byte[] bRecord = entity.serialize();
            if(bRecord.length != entity.getSize()){
                throw new UnsupportedEncodingException("Error Serialize Data is not the same size as returned by getSize()");
            }
            Serialize.bytes(bRecord, bRecord.length, recordOffset, PAGE);
            recordOffset += bRecord.length;
            recordId += 1;
        }
        return PAGE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public BaseItemSerializer<StorageItem> deserialize(byte[] DATA) throws UnsupportedEncodingException {
        BaseItemSerializer<StorageItem> page = (BaseItemSerializer<StorageItem>) this.clone();
        System.out.println("deserialize Page ");
        int recCount = 0;
        int recordLen = 0;
        int rid = 0;
        boolean isNextRecord = true;
        while (isNextRecord) {
            byte[] bRecord = new byte[type.getSize()];
            byte[] bRid = new byte[Key.RID_SIZE];
            System.arraycopy(DATA, recordLen, bRecord, 0, type.getSize());
            System.arraycopy(bRecord, 0, bRid, 0, Key.RID_SIZE);
            rid = ByteBuffer.wrap(bRid).getInt();
            System.out.println(String.format("Loading Record start:%d end:%d rid:%d rSize:%d pSize:%d ", recordLen, recordLen + type.getSize() , rid, type.getSize(), page.getSize() ));
            if (rid != recCount) {
                isNextRecord = false;
                System.out.println("Stopping last record");
            } else {
                StorageItem dto = this.type.deserialize(bRecord);
                System.out.println("adding record");
                page.storageItems.add(dto);
            }
            recordLen += type.getSize();
            recCount++;
            if( recordLen + type.getSize() > DATA.length){
                System.out.println("Stopping load next record will be out side of range");
                isNextRecord = false;
            }
        }
        return page;
    }

    @Override
    public long getIndex() {
        return this.getSize() * this.key.getPageId();
    }

    @Override
    public String toString() {
        return String.format("JSON(dbPage)=>{ \"pageId\": %d, \"pageLength\": %d, \"pageSize\": %d }", this.key.getPageId(),this.storageItems.size() , this.getSize() );
    }
    @Override
    public Object clone() {
        return new BaseItemSerializer<StorageItem>(this.size, this.type);
    }

    @Override
    public int getSize() {
        return this.size;
    }

}
