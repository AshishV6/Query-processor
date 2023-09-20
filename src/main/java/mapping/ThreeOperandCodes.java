package mapping;

import org.apache.commons.lang3.tuple.Triple;

import java.util.HashMap;
import java.util.Map;

public class ThreeOperandCodes {

    public static Map<Integer, Triple<Integer, Integer, Integer>> CODE_MAP = new HashMap<>();

    public static void populateCodeMap() {

        Map<Integer, Triple<Integer, Integer, Integer>> codeMap = CODE_MAP;

        codeMap.put(1, Triple.of(0, 1, 2));
        codeMap.put(2, Triple.of(0, 2, 1));
        codeMap.put(3, Triple.of(1, 0, 2));
        codeMap.put(4, Triple.of(1, 2, 0));
        codeMap.put(5, Triple.of(2, 0, 1));
        codeMap.put(6, Triple.of(2, 1, 0));
    }
}