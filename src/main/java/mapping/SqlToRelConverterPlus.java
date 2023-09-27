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


    static class RexShuttlePlus extends RexShuttle {
        @Override
        public RexNode visitCall(RexCall call) {

            RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder rexBuilder = new RexBuilder(typeFactory);

            SqlOperator operator = call.getOperator();
            List<RexNode> operands = call.getOperands();

            boolean KeywordCheck = isIndiffKeywordPool(operator);
            boolean twoOperandCheck = isIndiffTwoOperandOrderPool(operator);
            boolean threeOperandCheck = isIndiffThreeOperandOrderPool(operator);

            //only keyword difference case
            if (KeywordCheck && !twoOperandCheck && !threeOperandCheck) {

                KeywordMapper.populateOperatorMap();
                HashMap<SqlOperator, SqlOperator> operatorMap = KeywordMapper.OPERATOR_MAP;
                operator = operatorMap.get(operator);

                return rexBuilder.makeCall(operator, operands);
            }

            //change in operand order (2 operands) (with and without keyword change)
            else if (twoOperandCheck) {

                List<RexNode> swappedOperands = new ArrayList<>();
                swappedOperands.add(operands.get(1));
                swappedOperands.add(operands.get(0));

                if (KeywordCheck) {
                    KeywordMapper.populateOperatorMap();
                    HashMap<SqlOperator, SqlOperator> operatorMap = KeywordMapper.OPERATOR_MAP;
                    operator = operatorMap.get(operator);
                }
                return rexBuilder.makeCall(operator, swappedOperands);
            }

            //change in operand order (3 operands) (with and without keyword change)
            else if (threeOperandCheck) {

                ThreeOperandCodeMap.populateOperatorCodeMap();
                ThreeOperandCodes.populateCodeMap();
                HashMap<SqlOperator, Integer> operatorCode = ThreeOperandCodeMap.OPERATOR_CODE;

                List<RexNode> swappedOperands = new ArrayList<>();

                if (operatorCode.containsKey(operator)) {
                    int code = operatorCode.get(operator);
                    Triple<Integer, Integer, Integer> operandOrder = ThreeOperandCodes.CODE_MAP.get(code);
                        swappedOperands.add(operands.get(operandOrder.getLeft()));
                        swappedOperands.add(operands.get(operandOrder.getMiddle()));
                        swappedOperands.add(operands.get(operandOrder.getRight()));
                }

                if (KeywordCheck) {
                    KeywordMapper.populateOperatorMap();
                    HashMap<SqlOperator, SqlOperator> operatorMap = KeywordMapper.OPERATOR_MAP;
                    operator = operatorMap.get(operator);
                }
                return rexBuilder.makeCall(operator, swappedOperands);
            }
            return super.visitCall(call);
        }


        public enum diffKeywordPool {

            dateadd,
            timestampadd,
            timestampdiff,
            from_utc_timestamp,
            date_format,
            datepart,
            mod_datediff,
            date_diff_databricks,
            date_trunc_bigquery,
            format_datetime,
            format_date_bigquery
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
            date_trunc_bigquery,
            format_datetime,
            format_date_bigquery
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
            timestampdiff,
            date_diff_databricks

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