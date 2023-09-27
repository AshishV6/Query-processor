package mapping.formateDateFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;

public class format_timestamp extends SqlFunction {

    public format_timestamp() {
        super(
                "format_timestamp",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.DATETIME_STRING,
                NUMERIC);
    }
}
