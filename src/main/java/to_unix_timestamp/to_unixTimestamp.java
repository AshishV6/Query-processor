package to_unix_timestamp;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;


public class to_unixTimestamp extends SqlFunction {
    public to_unixTimestamp() {
        super("to_unixTimestamp",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OperandTypes.or(OperandTypes.DATETIME,
                        OperandTypesPlus.DATETIME_STRING),
                NUMERIC);
    }
}