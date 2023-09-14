package mapping;

import com.google.common.util.concurrent.Service;
import mapping.CommonIdentifier;
import operatorMapper.OperatorMMap;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.stream.Delta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.*;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;


import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlToRelConverterPlus extends SqlToRelConverter {



    public SqlToRelConverterPlus(RelOptTable.ViewExpander viewExpander, @Nullable SqlValidator validator, Prepare.CatalogReader catalogReader, RelOptCluster cluster, SqlRexConvertletTable convertletTable, SqlToRelConverter.Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    @Override
    public RelRoot convertQuery(
            SqlNode query,
            final boolean needsValidation,
            final boolean top) {

        if(containsCommonOperator(query)){
            RelRoot result = super.convertQuery(query, needsValidation, top);
            return result.withRel(result.rel.accept(new swappableShuttle()));
        }
        return super.convertQuery(query, needsValidation, top);
    }


    class swappableShuttle extends RexShuttle {

        @Override
        public RexNode visitCall(RexCall call) {

            SqlOperator operator = call.getOperator();
            SqlOperator operator1 = call.getOperator();
            CommonIdentifier.getPopulated();
            HashMap<SqlOperator, SqlOperator> opMap = OperatorMMap.Operator_Map;
            if (opMap.containsKey(operator)) {
                operator = opMap.get(operator);
                RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
                RexBuilder rexBuilder = new RexBuilder(typeFactory);
                List<RexNode> operands = call.getOperands();

                if (isInSwappables(operator1)) {
//                 Replace
                    List<RexNode> swappedOperands = new ArrayList<>();
                    swappedOperands.add(operands.get(1));
                    swappedOperands.add(operands.get(0));
                    return rexBuilder.makeCall(operator, swappedOperands);
                }
                return rexBuilder.makeCall(operator, operands);
            }
            return super.visitCall(call);
        }
    }


    public enum Swappables {
        CharLen
    }

    public boolean isInSwappables(SqlOperator operator) {
        for (Swappables swappable : Swappables.values()) {
            if (swappable.name().equals(operator.getName())) {
                return true;
            }
        }
        return false;
    }

    private boolean containsCommonOperator(SqlNode node) {
        try {
            SqlVisitor<Void> visitor = new SqlBasicVisitor<>() {
                @Override
                public Void visit(SqlCall call) {
                    if (isInSwappables(call.getOperator())) {
                        throw new Util.FoundOne(call);
                    }
                    return super.visit(call);
                }
            };
            node.accept(visitor);
            return false;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return true;

        }
    }


}
