package mapping.dateDiffFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;

public class mod_datediff extends SqlFunction {

    public mod_datediff() {
        super("mod_datediff", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypesPlus.DATETIME_DATETIME_STRING, NUMERIC);
    }

}
