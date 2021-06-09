package Tree;

import BaseClasses.BaseDbItemInterface;
import ItemSerializer.BaseItemSerializer;
import ItemSerializer.ItemNumberSelector;
import Loaders.BaseItemLoader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class IndexLoader <TKey extends Comparable<TKey> & BaseDbItemInterface<TKey>, TValue extends BaseDbItemInterface<TValue>> {

    public BaseItemLoader<IndexNode<TKey,TValue> , BaseItemSerializer<IndexNode<TKey,TValue>>> leafStore;
    public BaseItemLoader<IndexNode<TKey,TValue> ,BaseItemSerializer<IndexNode<TKey,TValue>>> innerStore;
    private Leaf<TKey,TValue> leafType;
    private ItemNumberSelector<IndexNode<TKey,TValue>> leafPageType;
    private InnerNode<TKey,TValue> innerType;
    private ItemNumberSelector<IndexNode<TKey,TValue>> innerPageType;


    public InnerNode<TKey,TValue> loadInnerNode(InnerNode<TKey,TValue> unloadedNode)
    {

        try {
            byte[] DATA = this.innerStore.read(
                    this.innerStore.getIndex(unloadedNode.getKey()),
                    unloadedNode.getSize()
            );
            unloadedNode.isLoaded = true;
            return (InnerNode<TKey,TValue>) unloadedNode.deserialize(DATA);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    public Leaf<TKey,TValue> loadLeafNode(Leaf<TKey,TValue> unloadedNode)
    {

        try {
            byte[] DATA;
            DATA = this.leafStore.read(
                    this.leafStore.getIndex(unloadedNode.getKey()),
                    unloadedNode.getSize()
            );

            unloadedNode.isLoaded = true;
            return (Leaf<TKey,TValue>) unloadedNode.deserialize(DATA);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;

    }

    public void addLeafNode(Leaf<TKey,TValue> loadedNode) throws UnsupportedEncodingException, IOException
    {
        this.leafStore.insertEntity(loadedNode);
    }

    public void addInnerNode(InnerNode<TKey,TValue> loadedNode) throws UnsupportedEncodingException, IOException
    {
        this.innerStore.insertEntity(loadedNode);
    }

    public IndexLoader(String leafNodeStore, String innerNodeStore, int pageQnt, Root<TKey,TValue> root) {

        this.leafType = new Leaf<TKey,TValue>(root.root.keyType, root.root.valueType);
        this.leafPageType = new ItemNumberSelector<>(pageQnt, this.leafType);
        this.leafStore = new BaseItemLoader<>(leafNodeStore , this.leafPageType,  this.leafType);

        this.innerType = new InnerNode<TKey, TValue>(root.root.keyType, root.root.valueType);
        this.innerPageType = new ItemNumberSelector<>(pageQnt, this.innerType);
        this.innerStore = new BaseItemLoader<>(innerNodeStore , this.innerPageType, this.innerType);


    }







}
