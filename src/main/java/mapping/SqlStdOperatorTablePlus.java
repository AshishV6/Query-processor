package mapping;

import DateAdd.date_add;
import DateAdd.dateadd;
import FormatDate.date_format;
import FormatDate.format_date;
import FormatTimestamp.date_format1;
import FormatTimestamp.format_timestamp;
import TimestampAdd.timestamp_add;
import TimestampAdd.timestampadd;
import current_date.current_date;
import dateTrunc.date_trunc;
import datepart.datepart;
import datepart.date_part;

import fromUnixTime.from_unixtime;
import fromutctimestamp.datetime;
import fromutctimestamp.from_utc_timestamp;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import timestampDiff.timestamp_diff;
import timestampDiff.timestampdiff;
import to_unix_timestamp.*;


public class
SqlStdOperatorTablePlus extends SqlStdOperatorTable {

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

    public static final SqlFunction LOC =
            new SqlFunction(
                    "LOC", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER_NULLABLE, null, OperandTypesPlus.STRING_INTEGER, SqlFunctionCategory.NUMERIC);

    public static final SqlFunction DATE_DIFF =
            new SqlFunction(
                    "date_diff",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.or(OperandTypesPlus.DATETIME_DATETIME,
                            OperandTypesPlus.DATETIME_DATETIME_STRING),
                    SqlFunctionCategory.NUMERIC
            );

    public static final SqlFunction datediff =
            new SqlFunction(
                    "datediff",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypesPlus.DATETIME_DATETIME,
                    SqlFunctionCategory.NUMERIC
            );

    //    TO_TIMESTAMP IN DATABRICKS WHERE OPERANDS ARE OPTIONAL
    public static final SqlFunction TO_TIMESTAMP =
            new SqlFunction(
                    "to_timeStamp",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.TIMESTAMP,
                    null,
                    OperandTypes.or(OperandTypes.STRING
                            , OperandTypes.STRING_STRING),
                    SqlFunctionCategory.TIMEDATE
            ) {
                @Override
                public void unparse(SqlWriter writer, SqlCall call,
                                    int leftPrec, int rightPrec) {
                    SqlWriter.Frame frame = writer.startList("TO_TIMESTAMP(", ")");
                    try {
                        if (call.operandCount() == 2) {
                            // If there are two operands, unparse only the first one
                            call.operand(0).unparse(writer, leftPrec, rightPrec);
                        } else {
                            // If there is only one operand, unparse it as usual
                            super.unparse(writer, call, leftPrec, rightPrec);
                        }
                    } finally {
                        writer.endList(frame);
                    }
                }
            };

    public static final SqlFunction to_timestamp =
            new SqlFunction(
                    "to_timestamp",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.TIMESTAMP,
                    null,
                    OperandTypes.STRING,
                    SqlFunctionCategory.TIMEDATE
            );

    public static final SqlFunction LEN = new Len();

    public static final SqlFunction CHAR_LEN = new CharLen();

    public static final SqlFunction TO_UNIX_TIMESTAMP = new to_unix_timestamp();

    public static final SqlFunction TO_UNIXTIMESTAMP = new to_unixTimestamp();

    public static final SqlFunction date_add = new date_add();
    public static final SqlFunction dateadd = new dateadd();

    public static final SqlFunction format_date = new format_date();
    public static final SqlFunction date_format = new date_format();

    public static final SqlFunction format_timestamp = new format_timestamp();
    public static final SqlFunction date_format1 = new date_format1();

    public static final SqlFunction timestampdiff = new timestampdiff();
    public static final SqlFunction timestamp_diff = new timestamp_diff();

    public static final SqlFunction timestampadd = new timestampadd();
    public static final SqlFunction timestamp_add = new timestamp_add();

    public static final SqlFunction date_trunc = new date_trunc();

    public static final SqlFunction from_unixtime = new from_unixtime();
    public static final SqlFunction current_date = new current_date();

    public static final SqlFunction datetime = new datetime();
    public static final SqlFunction from_utc_timestamp = new from_utc_timestamp();

    public static final SqlFunction datepart = new datepart();
    public static final SqlFunction date_part = new date_part();
}