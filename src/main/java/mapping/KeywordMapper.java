package mapping;

import org.apache.calcite.sql.SqlOperator;

import java.util.HashMap;


public class KeywordMapper {

    public static HashMap<SqlOperator, SqlOperator> OPERATOR_MAP = new HashMap<>();
    public static void populateOperatorMap() {

        HashMap<SqlOperator, SqlOperator> operatorMap = OPERATOR_MAP;

        operatorMap.put(SqlStdOperatorTablePlus.date_diff_databricks, SqlStdOperatorTablePlus.date_diff_e6data);
        operatorMap.put(SqlStdOperatorTablePlus.dateadd, SqlStdOperatorTablePlus.date_add);
        operatorMap.put(SqlStdOperatorTablePlus.timestampadd, SqlStdOperatorTablePlus.timestamp_add);
        operatorMap.put(SqlStdOperatorTablePlus.timestampdiff, SqlStdOperatorTablePlus.timestamp_diff);
        operatorMap.put(SqlStdOperatorTablePlus.from_utc_timestamp, SqlStdOperatorTablePlus.datetime);
        operatorMap.put(SqlStdOperatorTablePlus.date_format, SqlStdOperatorTablePlus.format_date);
        operatorMap.put(SqlStdOperatorTablePlus.datepart, SqlStdOperatorTablePlus.date_part);
        operatorMap.put(SqlStdOperatorTablePlus.date_trunc_bigquery, SqlStdOperatorTablePlus.date_trunc_e6data);
        operatorMap.put(SqlStdOperatorTablePlus.format_datetime, SqlStdOperatorTablePlus.format_timestamp);
        operatorMap.put(SqlStdOperatorTablePlus.format_date_bigquery, SqlStdOperatorTablePlus.format_date_e6data);
        operatorMap.put(SqlStdOperatorTablePlus.UNIX_SECONDS, SqlStdOperatorTablePlus.to_unix_timestamp);
        operatorMap.put(SqlStdOperatorTablePlus.TIMESTAMP, SqlStdOperatorTablePlus.to_timestamp);
        operatorMap.put(SqlStdOperatorTablePlus.DATETIME, SqlStdOperatorTablePlus.timestamp_diff);
        operatorMap.put(SqlStdOperatorTablePlus.date_diff_bigquery, SqlStdOperatorTablePlus.date_diff_e6data);
        operatorMap.put(SqlStdOperatorTablePlus.to_unixtime, SqlStdOperatorTablePlus.to_unix_timestamp);
        operatorMap.put(SqlStdOperatorTablePlus.from_unixtime, SqlStdOperatorTablePlus.from_unixtime_withunit);

    }
}
