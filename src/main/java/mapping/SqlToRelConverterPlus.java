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
import org.apache.commons.lang3.tuple.Triple;
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
//here we are checking whether our query has the common functions we mapped
//if our query has common functions which we check through query tree traversal and it executes the inner block of if
        if(containsCommonOperator(query)){
            RelRoot result = super.convertQuery(query, needsValidation, top);
            return afterModification(result,query);
        }
        return super.convertQuery(query, needsValidation, top);
    }

    public RelRoot afterModification(RelRoot relRoot, SqlNode query){
        RelNode modifiedNode = applyShuttleRecursively(relRoot.rel);
        return RelRoot.of(modifiedNode,relRoot.validatedRowType,query.getKind());
    }

    public RelNode applyShuttleRecursively(RelNode node){

         node = node.accept(new swappableShuttle());
        List<RelNode> relNodes = node.getInputs();
        if (!relNodes.isEmpty()){
            List<RelNode> modifiedChildNodes = new ArrayList<>();
            for (RelNode childNode : relNodes) {
                RelNode modifiedChild = applyShuttleRecursively(childNode);
                modifiedChildNodes.add(modifiedChild);
            }
            node = node.copy(node.getTraitSet(),modifiedChildNodes);
        }
        return node;
    }

    class swappableShuttle extends RexShuttle {
        @Override
        public RexNode visitCall(RexCall call) {

            SqlOperator operator = call.getOperator();
            SqlOperator operator1 = call.getOperator();
            CommonIdentifier.getPopulated();      //  we are populating the map here.
            HashMap<SqlOperator, SqlOperator> opMap = OperatorMMap.Operator_Map;
            if (opMap.containsKey(operator)) {
                operator = opMap.get(operator);       //  it replaces the operator name of operator that we are mapping
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

                if(isInSwapTrios(operator1)){
                    ThreeOperandCodeMapper.populateOperatorMap();
                    ThreeOperandsCodes.getPopulated();
                    HashMap<SqlOperator,Integer> OperatorMap = ThreeOperandCodeMapper.Operator_Map;
                    int code = OperatorMap.get(operator1);
                    Triple<Integer,Integer,Integer> OperandsOrder = ThreeOperandsCodes.INDEXSEQMAP.get(code);
                            List<RexNode> swappedOperands = new ArrayList<>();
                    swappedOperands.add(operands.get(OperandsOrder.getLeft()));
                    swappedOperands.add(operands.get(OperandsOrder.getMiddle()));
                    swappedOperands.add(operands.get(OperandsOrder.getRight()));
                    return rexBuilder.makeCall(operator,swappedOperands);
                }

                if (isInLastOpTrim(operator1)){
                    if(operands.size()==2){
                        List<RexNode> correctedOperands = new ArrayList<>();
                        correctedOperands.add(operands.get(0));
                        return rexBuilder.makeCall(operator,correctedOperands);}

                    if (operands.size() == 3) {
                        List<RexNode> correctedOperands = new ArrayList<>();
                        correctedOperands.add(operands.get(0));
                        correctedOperands.add(operands.get(1));
                        return rexBuilder.makeCall(operator,correctedOperands);}
                    }

                return rexBuilder.makeCall(operator, operands);
            }
            return super.visitCall(call);
        }
    }

// Enum of operators whose operands need to be swapped
    public enum Swappables {
        CharLen, datediff
    }

    public enum SwapTrios{
        date_diff, timestampdiff
    }

    public enum LastOpTrim {
        to_timeStamp, to_unixTimestamp
    }

    public boolean isInLastOpTrim(SqlOperator operator){
        for (LastOpTrim lastOpTrim : LastOpTrim.values()) {
            if (lastOpTrim.name().equals(operator.getName())) {
                return true;
            }
        }
        return false;
    }

    public boolean isInSwapTrios(SqlOperator operator) {
        for (SwapTrios swappable : SwapTrios.values()) {
            if (swappable.name().equals(operator.getName())) {
                return true;
            }
        }
        return false;
    }


    public boolean isInSwappables(SqlOperator operator) {
        for (Swappables swappable : Swappables.values()) {
            if (swappable.name().equals(operator.getName())) {
                return true;
            }
        }
        return false;
    }


//    method for query tree traversal
    private boolean containsCommonOperator(SqlNode node) {
        try {
            SqlVisitor<Void> visitor = new SqlBasicVisitor<>() {
                @Override
                public Void visit(SqlCall call) {
                    CommonIdentifier.getPopulated();
                    HashMap<SqlOperator,SqlOperator> opMap = OperatorMMap.Operator_Map;
                    if (opMap.containsKey(call.getOperator())) {
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
