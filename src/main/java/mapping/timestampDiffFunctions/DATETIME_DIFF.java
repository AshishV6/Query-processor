package mapping.timestampDiffFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;

public class DATETIME_DIFF extends SqlFunction {
    public DATETIME_DIFF() {
        super("DATETIME_DIFF",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OperandTypesPlus.DATETIME_DATETIME_STRING,
                NUMERIC);
    }
}
