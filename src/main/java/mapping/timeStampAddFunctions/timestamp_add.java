package mapping.timeStampAddFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.TIMEDATE;

public class timestamp_add extends SqlFunction {

    public timestamp_add() {
        super("timestamp_add",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.STRING_INTEGER_DATETIME,
                TIMEDATE);
    }
}
