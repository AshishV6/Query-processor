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
        OperatorMap.put(SqlStdOperatorTablePlus.datediff,SqlStdOperatorTablePlus.DATE_DIFF);
        OperatorMap.put(SqlStdOperatorTablePlus.TO_TIMESTAMP,SqlStdOperatorTablePlus.to_timestamp);
        OperatorMap.put(SqlStdOperatorTablePlus.TO_UNIXTIMESTAMP,SqlStdOperatorTablePlus.TO_UNIX_TIMESTAMP);
        OperatorMap.put(SqlStdOperatorTablePlus.dateadd,SqlStdOperatorTablePlus.date_add);
        OperatorMap.put(SqlStdOperatorTablePlus.date_format,SqlStdOperatorTablePlus.format_date);
        OperatorMap.put(SqlStdOperatorTablePlus.date_format1,SqlStdOperatorTablePlus.format_timestamp);
        OperatorMap.put(SqlStdOperatorTablePlus.timestampdiff,SqlStdOperatorTablePlus.timestamp_diff);
        OperatorMap.put(SqlStdOperatorTablePlus.timestampadd,SqlStdOperatorTablePlus.timestamp_add);
        OperatorMap.put(SqlStdOperatorTablePlus.date_trunc,SqlStdOperatorTablePlus.date_trunc);
        OperatorMap.put(SqlStdOperatorTablePlus.from_unixtime,SqlStdOperatorTablePlus.from_unixtime);
        OperatorMap.put(SqlStdOperatorTablePlus.from_utc_timestamp,SqlStdOperatorTablePlus.datetime);
        OperatorMap.put(SqlStdOperatorTablePlus.datepart,SqlStdOperatorTablePlus.date_part);
    }

}
