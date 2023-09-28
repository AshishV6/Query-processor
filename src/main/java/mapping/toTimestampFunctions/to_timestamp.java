package mapping.toTimestampFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;
import static org.apache.calcite.sql.SqlFunctionCategory.TIMEDATE;

public class to_timestamp extends SqlFunction {
    public to_timestamp() {
        super("to_timestamp",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.DATETIME,
                TIMEDATE);
    }
}
