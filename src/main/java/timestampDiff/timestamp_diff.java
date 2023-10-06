package timestampDiff;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;


public class timestamp_diff extends SqlFunction {

    public timestamp_diff() {
        super("timestamp_diff",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OperandTypesPlus.DATETIME_DATETIME_STRING,
                NUMERIC);
    }
}
