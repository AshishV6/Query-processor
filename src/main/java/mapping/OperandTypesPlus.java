package mapping;

import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;

import static org.apache.calcite.sql.type.OperandTypes.family;


    public abstract class OperandTypesPlus {
    private OperandTypesPlus() {
    }


    public static final SqlSingleOperandTypeChecker DATETIME_STRING =
            family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING);

    public static final SqlSingleOperandTypeChecker INTEGER_STRING =
            family(SqlTypeFamily.INTEGER, SqlTypeFamily.STRING);

    public static final SqlSingleOperandTypeChecker STRING_INTEGER =
            family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER);


    public static final SqlSingleOperandTypeChecker STRING_DATETIME =
            family(SqlTypeFamily.STRING, SqlTypeFamily.TIMESTAMP);

    public static final SqlSingleOperandTypeChecker STRING_DATETIME_DATETIME =
            family(SqlTypeFamily.STRING, SqlTypeFamily.DATETIME,SqlTypeFamily.DATETIME);

    public static final SqlSingleOperandTypeChecker DATETIME_DATETIME_STRING =
            family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME, SqlTypeFamily.STRING);

        public static final SqlSingleOperandTypeChecker STRING_INTEGER_DATETIME =
                family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.DATETIME);

        public static final SqlSingleOperandTypeChecker DATETIME_DATETIME =
                family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME);
 }