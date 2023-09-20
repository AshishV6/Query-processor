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
import org.apache.commons.lang3.tuple.Triple;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
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

        RelRoot result = super.convertQuery(query, needsValidation, top);

        return result.withRel(result.rel.accept(new RexShuttlePlus()));
    }


    class RexShuttlePlus extends RexShuttle {

        @Override
        public RexNode visitCall(RexCall call) {

            RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder rexBuilder = new RexBuilder(typeFactory);

            SqlOperator operator = call.getOperator();
            List<RexNode> operands = call.getOperands();


            //only keyword difference case
            if (isIndiffKeywordPool(operator) && !isIndiffTwoOperandOrderPool(operator)) {
                KeywordMapper.populateOperatorMap();
                HashMap<SqlOperator, SqlOperator> operatorMap = KeywordMapper.OPERATOR_MAP;

                if (operatorMap.containsKey(operator)) {
                    operator = operatorMap.get(operator);
                }

                return rexBuilder.makeCall(operator, operands);
            }

            //only change in operand order (2 operands)
            else if (isIndiffTwoOperandOrderPool(operator) && !isIndiffKeywordPool(operator)) {

                List<RexNode> swappedOperands = new ArrayList<>();
                swappedOperands.add(operands.get(1));
                swappedOperands.add(operands.get(0));

                return rexBuilder.makeCall(operator, swappedOperands);
            }

            //change in both keyword and operand order (2 operands)
            else if (isIndiffKeywordPool(operator) && isIndiffTwoOperandOrderPool(operator)) {

                KeywordMapper.populateOperatorMap();
                HashMap<SqlOperator, SqlOperator> operatorMap = KeywordMapper.OPERATOR_MAP;

                if (operatorMap.containsKey(operator)) {
                    operator = operatorMap.get(operator);
                }
                List<RexNode> swappedOperands = new ArrayList<>();
                swappedOperands.add(operands.get(1));
                swappedOperands.add(operands.get(0));

                return rexBuilder.makeCall(operator, swappedOperands);
            }


            //change in both keyword and operand order (3 operands)
            else if (isIndiffThreeOperandOrderPool(operator)) {
                ThreeOperandCodeMap.populateOperatorCodeMap();
                HashMap<SqlOperator, Integer> operatorCode = ThreeOperandCodeMap.OPERATOR_CODE;

                // Assuming you have access to the RexNode operands, you can swap them based on the operator code.
                List<RexNode> swappedOperands = new ArrayList<>();

                // Check if the operator is timestampdiff and it exists in the operatorCode map.
                if (operatorCode.containsKey(operator)) {
                    int code = operatorCode.get(operator);
                    Triple<Integer, Integer, Integer> operandOrder = ThreeOperandCodes.CODE_MAP.get(code);

                    // Assuming operands is a list of RexNode representing the operands.
                    if (operands.size() == 3) {
                        swappedOperands.add(operands.get(operandOrder.getLeft()));
                        swappedOperands.add(operands.get(operandOrder.getMiddle()));
                        swappedOperands.add(operands.get(operandOrder.getRight()));
                    }
                }

            }
            return super.visitCall(call);
        }


        public enum diffKeywordPool {
            date_format,
            from_utc_timestamp
        }

        public boolean isIndiffKeywordPool(SqlOperator operator) {
            boolean flag = false;
            for (diffKeywordPool mappable : diffKeywordPool.values()) {
                if (mappable.name().equals(operator.getName())) {
                    flag = true;
                    break;
                }
            }
            return flag;
        }

        public enum diffTwoOperandOrderPool {
            CharLen,
            from_utc_timestamp
        }

        public boolean isIndiffTwoOperandOrderPool(SqlOperator operator) {
            boolean flag = false;
            for (diffTwoOperandOrderPool swappable : diffTwoOperandOrderPool.values()) {
                if (swappable.name().equals(operator.getName())) {
                    flag = true;
                    break;
                }
            }
            return flag;
        }


        public enum diffThreeOperandOrderPool {
            timestampdiff

        }

        public boolean isIndiffThreeOperandOrderPool(SqlOperator operator) {
            boolean flag = false;
            for (diffThreeOperandOrderPool swappable : diffThreeOperandOrderPool.values()) {
                if (swappable.name().equals(operator.getName())) {
                    flag = true;
                    break;
                }
            }
            return flag;
        }


    }
}