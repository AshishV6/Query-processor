package mapping;

import java.util.HashMap;


import operatorMapper.OperatorMMap;
import org.apache.calcite.sql.SqlOperator;


public class CommonIdentifier {
    public static void getPopulated() {
        HashMap<SqlOperator, SqlOperator> OperatorMap = OperatorMMap.Operator_Map;

//        OperatorMap.put(, "ADDITION");
//        OperatorMap.put("SUB", "SUBTRACTION");
        OperatorMap.put(SqlStdOperatorTablePlus.LEN, SqlStdOperatorTablePlus.LOC);
        OperatorMap.put(SqlStdOperatorTablePlus.CHAR_LEN, SqlStdOperatorTablePlus.LOC);
    }

}
