package mapping.dateTruncFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import java.sql.Time;

import static org.apache.calcite.sql.SqlFunctionCategory.NUMERIC;
import static org.apache.calcite.sql.SqlFunctionCategory.TIMEDATE;

public class date_trunc_e6data extends SqlFunction {

    public date_trunc_e6data() {
        super("date_trunc_e6data",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.STRING_DATETIME,
                TIMEDATE);
    }

}
