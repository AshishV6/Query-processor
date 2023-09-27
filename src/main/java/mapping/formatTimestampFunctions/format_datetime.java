package mapping.formatTimestampFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;
import static org.apache.calcite.sql.SqlFunctionCategory.TIMEDATE;

public class format_datetime extends SqlFunction {
    public format_datetime() {
        super("format_datetime",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.STRING_DATETIME,
                NUMERIC);
    }
}
