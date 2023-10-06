package timestampDiff;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;


public class timestampdiff extends SqlFunction {

    public timestampdiff() {
        super("timestampdiff",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.STRING_DATETIME_DATETIME,
                NUMERIC);
    }

}