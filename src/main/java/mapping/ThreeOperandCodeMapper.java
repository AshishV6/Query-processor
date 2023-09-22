package mapping;

import org.apache.calcite.sql.SqlOperator;

import java.util.HashMap;

public class ThreeOperandCodeMapper {

    public static HashMap<SqlOperator,Integer> Operator_Map = new HashMap<>();

//    Map that maps specific operator of 3 operands to it's code.
    public static void populateOperatorMap(){
        HashMap<SqlOperator,Integer> OpMap = Operator_Map;
        OpMap.put(SqlStdOperatorTablePlus.DATE_DIFF,4);
    }
}
