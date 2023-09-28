package mapping.dateDiffFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;

public class date_diff_bigquery extends SqlFunction {
    public date_diff_bigquery() {
        super("date_diff_bigquery",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.DATETIME_DATETIME_STRING,
                NUMERIC);
    }
}
