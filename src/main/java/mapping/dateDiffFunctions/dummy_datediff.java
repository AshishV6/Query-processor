package mapping.dateDiffFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;

public class dummy_datediff extends SqlFunction {

    public dummy_datediff() {
        super("dummy_datediff", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypesPlus.STRING_DATETIME_DATETIME, NUMERIC);
    }

}
