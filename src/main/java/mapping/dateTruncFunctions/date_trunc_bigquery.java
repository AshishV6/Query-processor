package mapping.dateTruncFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;
import static org.apache.calcite.sql.SqlFunctionCategory.TIMEDATE;

public class date_trunc_bigquery extends SqlFunction {

    public date_trunc_bigquery() {
        super("date_trunc_bigquery",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.DATETIME_STRING,
                TIMEDATE);
    }

}
