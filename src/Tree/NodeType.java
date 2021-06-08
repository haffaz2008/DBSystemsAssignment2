package Tree;

import java.util.HashMap;
import java.util.Map;

public enum NodeType {

        InnerNode(0),
        LeafNode(1);
        private final Integer dbValue;

        private NodeType(Integer dbValue){
            this.dbValue = dbValue;
        }

        public int toInt(){
            return this.dbValue;
        }

        public static final Map<Integer, NodeType> deserializer = new HashMap<>();

        static {
            for (NodeType value : values()) {
                deserializer.put(value.dbValue, value);
            }
        }

        public static NodeType fromInt(Integer dbValue) {
            return deserializer.get(dbValue);
        }

        public String toString(){
            if(this.dbValue==0){
                return "InnerNode";
            }else{
                return "LeafNode ";
            }
        }
}
