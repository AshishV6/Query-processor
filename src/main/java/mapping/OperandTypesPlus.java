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
            family(SqlTypeFamily.INTEGER, SqlTypeFamily.STRING);

    public static final SqlSingleOperandTypeChecker STRING_DATETIME =
            family(SqlTypeFamily.STRING, SqlTypeFamily.TIMESTAMP);

    public static final SqlSingleOperandTypeChecker STRING =
            family(SqlTypeFamily.STRING);

    public static final SqlSingleOperandTypeChecker DATETIME_DATETIME_STRING =
            family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING);

    public static final SqlSingleOperandTypeChecker DATETIME_DATETIME_KEYWORD =
            family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING);




 }