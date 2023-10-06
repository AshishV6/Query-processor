package to_unix_timestamp;


import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;

public class to_unix_timestamp extends SqlFunction {
    public to_unix_timestamp() {
        super("to_unix_timestamp",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OperandTypes.DATETIME,
                NUMERIC);
    }
}