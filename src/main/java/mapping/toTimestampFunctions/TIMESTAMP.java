package mapping.toTimestampFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;
import static org.apache.calcite.sql.SqlFunctionCategory.TIMEDATE;

public class TIMESTAMP extends SqlFunction {
    public TIMESTAMP() {
        super("TIMESTAMP",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.DATETIME,
                TIMEDATE);
    }
}
