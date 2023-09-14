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

    public enum lengthPool{
        CharLen
    }

    public enum currentDatePool{
        from_utc_timestamp
    }

    public enum timestampDiffPool{
        timestampdiff
    }

    class RexShuttlePlus extends RexShuttle{

        @Override public RexNode visitCall(RexCall call){

            SqlOperator operator = call.getOperator();

            if (operator == SqlStdOperatorTablePlus.LEN || operator == SqlStdOperatorTablePlus.CHAR_LEN) {

                RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
                RexBuilder rexBuilder = new RexBuilder(typeFactory);

                List<RexNode> operands = call.getOperands();
                if(isInLengthPool(operator)){
                    List<RexNode> swappedOperands = new ArrayList<>();
                    swappedOperands.add(operands.get(1));
                    swappedOperands.add(operands.get(0));
                    return rexBuilder.makeCall(SqlStdOperatorTablePlus.LEN, swappedOperands);
                }
                return rexBuilder.makeCall(SqlStdOperatorTablePlus.LEN, operands);
            }

            else if(operator == SqlStdOperatorTablePlus.current_date || operator == SqlStdOperatorTablePlus.from_utc_timestamp){

                RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
                RexBuilder rexBuilder = new RexBuilder(typeFactory);

                List<RexNode> operands = call.getOperands();
                if(isInCurrentDatePool(operator)){
                    List<RexNode> swappedOperands = new ArrayList<>();
                    swappedOperands.add(operands.get(1));
                    return rexBuilder.makeCall(SqlStdOperatorTablePlus.current_date, swappedOperands);
                }
                return rexBuilder.makeCall(SqlStdOperatorTablePlus.current_date, operands);
            }

            else if(operator == SqlStdOperatorTablePlus.timestamp_diff || operator == SqlStdOperatorTablePlus.timestampdiff){

                RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
                RexBuilder rexBuilder = new RexBuilder(typeFactory);

                List<RexNode> operands = call.getOperands();
                if(isInCurrentDatePool(operator)){
                    List<RexNode> swappedOperands = new ArrayList<>();
                    swappedOperands.add(operands.get(1));
                    return rexBuilder.makeCall(SqlStdOperatorTablePlus.timestamp_diff, swappedOperands);
                }
                return rexBuilder.makeCall(SqlStdOperatorTablePlus.timestamp_diff, operands);
            }
            return super.visitCall(call);
        }
    }



    public boolean isInTimestampDiffPool(SqlOperator operator) {
        boolean flag = false;
        for (timestampDiffPool swappable : timestampDiffPool.values()) {
            if (swappable.name().equals(operator.getName())) {
                flag = true;
                break;
            }
        }
        return flag;
    }



    public boolean isInCurrentDatePool(SqlOperator operator) {
        boolean flag = false;
        for (currentDatePool swappable : currentDatePool.values()) {
            if (swappable.name().equals(operator.getName())) {
                flag = true;
                break;
            }
        }
        return flag;
    }


    public boolean isInLengthPool(SqlOperator operator) {
        boolean flag = false;
        for (lengthPool swappable : lengthPool.values()) {
            if (swappable.name().equals(operator.getName())) {
                flag = true;
                break;
            }
        }
        return flag;
    }
}