package mapping;

import java.util.HashMap;


import operatorMapper.OperatorMMap;
import org.apache.calcite.sql.SqlOperator;


public class CommonIdentifier {
    public static void getPopulated() {
        HashMap<SqlOperator, SqlOperator> OperatorMap = OperatorMMap.Operator_Map;

//here we are mapping functions in different sqls of same functionality to function in e6data sql
//in the example below; LEN is of lets say databricks sql and CHARLEN is of postgre sql and we are mapping both to LOC which is of e6data sql

        OperatorMap.put(SqlStdOperatorTablePlus.LEN, SqlStdOperatorTablePlus.LOC);
        OperatorMap.put(SqlStdOperatorTablePlus.CHAR_LEN, SqlStdOperatorTablePlus.LOC);
    }

}
