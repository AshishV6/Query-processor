package mapping;

import org.apache.calcite.sql.SqlOperator;

import java.util.HashMap;

public class ThreeOperandCodeMap {

    public static HashMap<SqlOperator, Integer> OPERATOR_CODE = new HashMap<>();
    public static void populateOperatorCodeMap() {

        HashMap<SqlOperator, Integer> operatorMap = OPERATOR_CODE;

        operatorMap.put(SqlStdOperatorTablePlus.date_diff_databricks, 4);
    }

}
