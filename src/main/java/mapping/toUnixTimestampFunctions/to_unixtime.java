package mapping.toUnixTimestampFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;

public class to_unixtime extends SqlFunction {
    public to_unixtime() {
        super("to_unixtime",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OperandTypesPlus.DATETIME,
                NUMERIC);
    }
}
