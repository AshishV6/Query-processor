package mapping;

import org.apache.calcite.sql.SqlOperator;

import java.util.HashMap;


public class KeywordMapper {

    public static HashMap<SqlOperator, SqlOperator> OPERATOR_MAP = new HashMap<>();
    public static void populateOperatorMap() {

        HashMap<SqlOperator, SqlOperator> operatorMap = OPERATOR_MAP;

        operatorMap.put(SqlStdOperatorTablePlus.date_format, SqlStdOperatorTablePlus.format_date);
        operatorMap.put(SqlStdOperatorTablePlus.from_utc_timestamp, SqlStdOperatorTablePlus.datetime);
        operatorMap.put(SqlStdOperatorTablePlus.timestampdiff, SqlStdOperatorTablePlus.timestamp_diff);
    }
}
