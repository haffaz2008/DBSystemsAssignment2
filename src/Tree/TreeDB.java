package Tree;

import BaseClasses.BaseDbItemInterface;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

public class TreeDB <TKey extends Comparable<TKey> & BaseDbItemInterface<TKey> , TValue extends BaseDbItemInterface<TValue>> extends IndexLoader<TKey, TValue> {
    private Root<TKey,TValue> root;
    public TreeDB(String datastorePath, int pageSize,Root<TKey,TValue> root) {
        super(datastorePath+".leaf.heap", datastorePath+".inner.heap", pageSize, root);
        this.root = root;
    }


    public void save() throws UnsupportedEncodingException, IOException {
        Iterator<IndexNode<TKey,TValue>> iter = root.iterator();
        while(iter.hasNext()){
            IndexNode<TKey,TValue> node = iter.next();


            if(node.getNodeType() == NodeType.InnerNode){
                this.addInnerNode((InnerNode<TKey,TValue>) node);
            }else if(node.getNodeType() == NodeType.LeafNode){
                this.addLeafNode((Leaf<TKey,TValue>) node);
            }else{
                throw new UnsupportedEncodingException("Unknown Node type");
            }
        }
    }

    public void log(){
        Stats<TKey,TValue> stats = root.stats();
        System.out.println(stats.toString());
        System.out.println(stats.root.detailedJsonString());
        for(IndexNode<TKey,TValue> node: stats.nodes){
            int depth = stats.getDepth(node);
            String padding = "";
            for(int i = 0; i < depth; i ++){
                padding += "    ";
            }
            System.out.println(padding +"("+ depth+ ")"+ node.toJsonString());
        }
    }

}
