package mapping.toUnixTimestampFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;

public class UNIX_SECONDS extends SqlFunction {
    public UNIX_SECONDS() {
        super("UNIX_SECONDS",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OperandTypesPlus.DATETIME,
                NUMERIC);
    }
}
