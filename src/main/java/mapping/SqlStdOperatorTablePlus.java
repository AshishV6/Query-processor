package mapping;

import mapping.dateAddFunctions.date_add;
import mapping.dateAddFunctions.dateadd;
import mapping.dateDiffFunctions.date_diff_databricks;
import mapping.dateDiffFunctions.date_diff_e6data;
import mapping.datePartFunctions.date_part;
import mapping.datePartFunctions.datepart;
import mapping.dateTimeFunctions.datetime;
import mapping.dateTimeFunctions.from_utc_timestamp;
import mapping.dateTruncFunctions.date_trunc_bigquery;
import mapping.formatDateFunctions.format_date_bigquery;
import mapping.formatDateFunctions.format_date_e6data;
import mapping.formatTimestampFunctions.format_datetime;
import mapping.formateDateFunctions.date_format;
import mapping.formateDateFunctions.format_date;
import mapping.formateDateFunctions.format_timestamp;
import mapping.lengthFunctions.CharLen;
import mapping.lengthFunctions.Len;
import mapping.timeStampAddFunctions.timestamp_add;
import mapping.timeStampAddFunctions.timestampadd;
import mapping.timestampDiffFunctions.timestamp_diff;
import mapping.timestampDiffFunctions.timestampdiff;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

public class SqlStdOperatorTablePlus extends SqlStdOperatorTable {

    private static @MonotonicNonNull SqlStdOperatorTablePlus instance;


    /**
     * Returns the standard operator table, creating it if necessary.
     */
    public static synchronized SqlStdOperatorTablePlus instance() {
        if (instance == null) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't initialize the
            // table until the constructor of the sub-class has completed.
            instance = new SqlStdOperatorTablePlus();
            instance.init();
        }
        return instance;
    }



    public static final SqlFunction ABS_CUSTOM =
            new SqlFunction(
                    "ABS_CUSTOM",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.NUMERIC_OR_INTERVAL,
                    SqlFunctionCategory.NUMERIC);



    public static final SqlFunction SQRT_CUSTOM =
            new SqlFunction(
                    "SQRT_CUSTOM",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    SqlFunctionCategory.NUMERIC);



    public static final SqlFunction POWER_CUSTOM =
            new SqlFunction(
                    "POWER_CUSTOM",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);



    public static final SqlFunction STR_TO_INT =
            new SqlFunction(
                    "STR_TO_INT",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER,
                    null,
                    OperandTypes.STRING,
                    SqlFunctionCategory.STRING);



    public static final SqlFunction ROUND_CUSTOM =
            new SqlFunction(
                    "ROUND_CUSTOM",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_OPTIONAL_INTEGER,
                    SqlFunctionCategory.NUMERIC);



    public static final SqlFunction DATETIME =
            new SqlFunction(
                    "DATETIME",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.TIMESTAMP_NULLABLE,
                    null,
                    OperandTypesPlus.DATETIME_STRING,
                    SqlFunctionCategory.TIMEDATE);

    public static final SqlFunction CONVERT_TIMEZONE =
            new SqlFunction(
                    "CONVERT_TIMEZONE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.TIMESTAMP_NULLABLE,
                    null,
                    OperandTypesPlus.STRING_DATETIME,
                    SqlFunctionCategory.TIMEDATE);



    public static final SqlFunction ADD =
            new SqlFunction(
                    "ADD",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);



    public static final SqlFunction PLUS =
            new SqlFunction(
                    "PLUS",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);



    public static final SqlFunction SUM =
            new SqlFunction(
                    "SUM",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);


    public static final SqlFunction MINUS =
            new SqlFunction(
                    "MINUS",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);


    public static final SqlFunction SUBTRACT =
            new SqlFunction(
                    "SUBTRACT",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);


    public static final SqlFunction LEN =  new Len();

    public static final SqlFunction CHAR_LEN =  new CharLen();


    public static final SqlFunction datetime =  new datetime();

    public static final SqlFunction from_utc_timestamp =  new from_utc_timestamp();


    public static final SqlFunction date_format =  new date_format();
    public static final SqlFunction format_date =  new format_date();



    public static final SqlFunction date_diff_databricks = new date_diff_databricks();
    public static final SqlFunction date_diff_e6data =new date_diff_e6data();


    public static final SqlFunction dateadd = new dateadd();
    public static final SqlFunction date_add = new date_add();


    public static final SqlFunction timestamp_add =  new timestamp_add();
    public static final SqlFunction timestampadd =  new timestampadd();


    public static final SqlFunction timestamp_diff =  new timestamp_diff();
    public static final SqlFunction timestampdiff =  new timestampdiff();


    public static final SqlFunction datepart = new datepart();
    public static final SqlFunction date_part = new date_part();


    public static final SqlFunction date_trunc_e6data = new date_diff_e6data();
    public static final SqlFunction date_trunc_bigquery = new date_trunc_bigquery();


    public static final SqlFunction format_timestamp = new format_timestamp();
    public static final SqlFunction format_datetime = new format_datetime();


    public static final SqlFunction format_date_e6data = new format_date_e6data();
    public static final SqlFunction format_date_bigquery = new format_date_bigquery();



}
