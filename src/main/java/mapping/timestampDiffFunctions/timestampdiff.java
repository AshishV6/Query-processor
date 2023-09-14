package mapping.timestampDiffFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.STRING;

public class timestampdiff extends SqlFunction {

    public timestampdiff() {
        super("timestampdiff", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypesPlus.DATETIME_DATETIME_KEYWORD, STRING);
    }

}
