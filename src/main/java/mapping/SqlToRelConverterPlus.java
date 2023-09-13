package mapping;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;


public class SqlToRelConverterPlus extends SqlToRelConverter {

    protected final RexBuilder rexBuilder;
    public SqlToRelConverterPlus(RelOptTable.ViewExpander viewExpander, @Nullable SqlValidator validator, Prepare.CatalogReader catalogReader, RelOptCluster cluster, SqlRexConvertletTable convertletTable, SqlToRelConverter.Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
        this.rexBuilder = cluster.getRexBuilder();
    }

    @Override
    public RelRoot convertQuery(
            SqlNode query,
            final boolean needsValidation,
            final boolean top) {

        RelRoot result = super.convertQuery(query,needsValidation,top);

        return result.withRel(result.rel.accept(new RexShuttlePlus()));
    }

    public enum Swappables{
        CharLen
    }

    class RexShuttlePlus extends RexShuttle{

        @Override public RexNode visitCall(RexCall call){

            SqlOperator operator = call.getOperator();
            List<RexNode> operands = call.getOperands();

            if (operator == SqlStdOperatorTablePlus.LEN || operator == SqlStdOperatorTablePlus.CHAR_LEN) {

                RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
                RexBuilder rexBuilder = new RexBuilder(typeFactory);

                if(isInSwappables(operator)){
                    List<RexNode> swappedOperands = new ArrayList<>();
                    swappedOperands.add(operands.get(1));
                    swappedOperands.add(operands.get(0));
                    return rexBuilder.makeCall(SqlStdOperatorTablePlus.LEN, swappedOperands);
                }
                return rexBuilder.makeCall(SqlStdOperatorTablePlus.LEN, operands);
            }
            return super.visitCall(call);
        }
    }



    public boolean isInSwappables(SqlOperator operator) {
        boolean flag = false;
        for (Swappables swappable : Swappables.values()) {
            if (swappable.name().equals(operator.getName())) {
                flag = true;
                break;
            }
        }
        return flag;
    }
}