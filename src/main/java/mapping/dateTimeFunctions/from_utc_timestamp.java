package mapping.dateTimeFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.*;

public class from_utc_timestamp extends SqlFunction {

    public from_utc_timestamp() {
        super("from_utc_timestamp", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypesPlus.STRING_DATETIME, STRING);
    }

}
