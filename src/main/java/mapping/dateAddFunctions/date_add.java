package mapping.dateAddFunctions;

import mapping.OperandTypesPlus;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.SqlFunctionCategory.TIMEDATE;

public class date_add extends SqlFunction {

    public date_add() {
        super("date_add",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.TIMESTAMP_NULLABLE,
                null,
                OperandTypesPlus.STRING_INTEGER_DATETIME,
                TIMEDATE);
    }

}
