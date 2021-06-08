package Loaders;

import BaseClasses.Key;
import BaseClasses.StorageBase;
import DummyDBClient.DummyDBCreator;
import ItemSerializer.BaseItemSerializer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

public class BaseItemLoader<Item extends StorageBase<Item>, TPage extends BaseItemSerializer<Item>> extends DummyDBCreator implements Iterable<Item> {
    private StorageBase<TPage> pageType;
    private StorageBase<Item> entityType;



    public static class EntityIterator<TEntity extends StorageBase<TEntity>, TPage extends BaseItemSerializer<TEntity>>
            implements Iterator<TEntity> {
        private BaseItemLoader<TEntity, TPage> loader;
        private TEntity nextEntity;
        private boolean doneloading = false;

        EntityIterator(BaseItemLoader<TEntity, TPage> loader) {
            this.loader = loader;
        }

        @Override
        public boolean hasNext() {
            Key nextKey = this.loader.getNextKey();
            try {
                this.loader.validateKey(nextKey);
                return true && this.doneloading == false;
            } catch (Exception e) {
                return false;
            }

        }

        @Override
        public TEntity next() {
            try {
                Key key;
                TEntity entity;

                if(nextEntity == null){
                    key = this.loader.getNextKey();
                    entity = this.loader.findEntity(key);
                    this.loader.setLastKey(key);
                    key =this.loader.getNextKey();
                    this.nextEntity = this.loader.findEntity(key);
                    this.loader.setLastKey(key);
                }else{
                    entity = this.nextEntity;
                    key = this.loader.getNextKey();
                    this.nextEntity = this.loader.findEntity(key);
                    this.loader.setLastKey(key);
                }
                if(nextEntity.key.getPageId() != key.getPageId() && nextEntity.key.getRId() != key.getRId()){
                    this.doneloading = true;
                }

                return entity;

            } catch (Exception e) {
                System.out.println("Error Loading Next Record From Entity Store");
                System.out.println(e.toString());
                System.exit(1);
            }

            return null;
        }

    }


    public BaseItemLoader(String datastorePath, StorageBase<TPage> pageType, StorageBase<Item> entityType) {
        super(datastorePath);
        this.pageType = pageType;
        this.entityType = entityType;
    }

    protected void validateKey(Key key) throws Exception {
        boolean valid = true;
        if (!this.isInitialized()) {
            System.out.println("DB connection is not initialized");
            valid = false;
        }
        if (this.getDataStoreSize() < key.getPageId() * this.pageType.getSize()) {
            System.out.println("PageId is larger that the datastore size "+key.toString());
            valid = false;
        }
        if (this.pageType.getSize() < (key.getRId() + 1) * this.entityType.getSize()) {
            System.out.println("Page Size"+this.pageType.getSize());
            System.out.println("Offset "+(key.getRId() + 1) * this.entityType.getSize());
            System.out.println("Rid is larger than the datastore page size "+ key.toString());
            valid = false;
        }
        if (!valid) {
            throw new Exception("Query is not valid "+key.toString());
        }

    }

    public long getIndex(Key key) {
        return key.getIndex(this.pageType.getSize(), this.entityType.getSize());
    }

    public long getPageIndex(Key key) {
        return key.getIndex(this.pageType.getSize(), 0);
    }
    public Map<Key,Item> cache = new HashMap<>();
    public Item findEntity(Key key) throws FileNotFoundException, IOException, Exception {
        this.validateKey(key);
        if(this.cache.containsKey(key)){
            return this.cache.get(key);
        }
        byte[] RECORD = this.read(this.getIndex(key), this.entityType.getSize());
        Item dto = this.entityType.deserialize(RECORD);
        this.cache.put(key, dto);
        return dto;
    }

    public Item saveEntity(Item entity) throws FileNotFoundException, IOException, Exception {
        this.validateKey(entity.getKey());
        byte[] data = entity.serialize();
        this.write(this.getIndex(entity.getKey()), data);
        return entity;
    }

    public TPage findPage(Key key) throws IOException {
        long pageIndex = this.getPageIndex(key);
        System.out.println("Getting page at index " + pageIndex);
        byte[] PAGE = this.read(pageIndex, this.pageType.getSize());
        return this.pageType.deserialize(PAGE);
    }

    public TPage savePage(TPage page) throws UnsupportedEncodingException, IOException {
        long pageIndex = this.getPageIndex(page.getKey());
        byte[] PAGE = page.serialize();
        this.write(pageIndex, PAGE);
        return page;
    }

    private TPage cachedPage;
    private Key cachedKey;
    protected Key getNextKey(){
        if(this.cachedKey == null){ return new Key(0, 0); }
        if (this.pageType.getSize() < (this.cachedKey.getRId() + 2) * this.entityType.getSize()) {
            return new Key(this.cachedKey.getPageId() + 1, 0);
        }
        return new Key(this.cachedKey.getPageId(), this.cachedKey.getRId() + 1);
    }
    protected void setLastKey(Key key) {
        this.cachedKey = key;
    }
    @SuppressWarnings("unchecked")
    public void insertEntity(Item entity)
            throws UnsupportedEncodingException, IOException
    {
        Key key = this.getNextKey();
        // Is Next Page
        if(this.cachedKey == null || key.getPageId() > this.cachedKey.getPageId() ) {
            if(this.cachedPage != null){
                this.savePage(this.cachedPage);
                this.cachedPage = null;
            }
            this.cachedPage = (TPage) this.pageType.clone();
            this.cachedPage.setKey(key);
        }
        entity.setKey(key);
        this.cachedPage.storageItems.add(entity);
        this.setLastKey(key);
    }

    @Override
    public void close() {
        if(this.cachedPage != null && this.cachedPage.storageItems.size() > 0){
            try{
                this.savePage(this.cachedPage);

            }catch(Exception e){
                System.out.println("Was unable to save page cache from last insert!");
                System.out.println(this.cachedPage.storageItems.size());
                System.out.println(e.toString());
                e.printStackTrace();
            }
            this.cachedPage = null;
        }
        super.close();
    }
    @Override
    public Iterator<Item> iterator() {
        return new EntityIterator<Item, TPage>(this);
    }
}
