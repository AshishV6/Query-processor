package mapping;


import org.apache.commons.lang3.tuple.Triple;

import java.util.HashMap;

public class ThreeOperandsCodes {
    public static HashMap<Integer,Triple<Integer,Integer,Integer>> INDEXSEQMAP = new HashMap<>();
//    Map containing assigned codes for all possible orders of operands.
    public static void getPopulated(){
        HashMap<Integer, Triple<Integer,Integer,Integer>> CodeMap = INDEXSEQMAP;

        CodeMap.put(1,Triple.of(0,1,2));
        CodeMap.put(2,Triple.of(0,2,1));
        CodeMap.put(3,Triple.of(1,0,2));
        CodeMap.put(4,Triple.of(1,2,0));
        CodeMap.put(5,Triple.of(2,1,0));
        CodeMap.put(6,Triple.of(2,0,1));
    }

}