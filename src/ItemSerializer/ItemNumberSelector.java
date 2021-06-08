package ItemSerializer;

import BaseClasses.StorageBase;

public class ItemNumberSelector<Item extends StorageBase<Item>> extends BaseItemSerializer<Item> {
    public ItemNumberSelector(int size, Item type) {
        super(size, type);
    }
    @Override
    public int getSize() {
        return this.size * this.type.getSize();
    }

    @Override
    public Object clone() {
        return new ItemNumberSelector<Item>(this.size, this.type);
    }


}
