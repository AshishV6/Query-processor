package mapping.formatDateFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;

public class format_date_e6data extends SqlFunction {
    public format_date_e6data() {
        super("format_date_e6data",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.DATETIME_STRING,
                NUMERIC);
    }
}
