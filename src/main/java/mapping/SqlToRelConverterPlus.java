package mapping;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class SqlToRelConverterPlus extends SqlToRelConverter {

    public SqlToRelConverterPlus(RelOptTable.ViewExpander viewExpander, SqlValidator validator, Prepare.CatalogReader catalogReader, RelOptPlanner planner, RexBuilder rexBuilder, SqlRexConvertletTable convertletTable) {
        super(viewExpander, validator, catalogReader, planner, rexBuilder, convertletTable);
    }



    
}
