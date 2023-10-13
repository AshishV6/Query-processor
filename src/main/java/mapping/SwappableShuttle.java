package mapping;

import mapping.SqlStdOperatorTablePlus;
import mapping.Swappable;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

import static mapping.Swappable.INDEXSEQMAP;


public class SwappableShuttle extends RexShuttle {

    @Override
    public RexNode visitCall(RexCall call) {

        SqlOperator operator = call.getOperator();
        SqlOperator operator1 = call.getOperator();
        if (Swappable.COMMON_IDENTIFIER_MAP.containsKey(operator)) {
            operator = Swappable.COMMON_IDENTIFIER_MAP.get(operator);       //  it replaces the operator name of operator that we are mapping
            RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder rexBuilder = new RexBuilder(typeFactory);
            List<RexNode> operands = call.getOperands();

            boolean isSwappable = Swappable.isInSwappable(operator1.getName());


            if (isSwappable){

                Swappable swappable = Swappable.valueOf(operator1.getName());

                List<Integer> swapOrderList = swappable.getSwapOrder();
                List<RexNode> swappedOperands = new ArrayList<>();
                if(operator1.getName().equals(SqlStdOperatorTablePlus.DATE_DIFF.getName()) && operands.get(0).getType().getSqlTypeName().equals(SqlTypeName.CHAR)){
                    for (Integer integer : INDEXSEQMAP.get(4)) {
                        swappedOperands.add(operands.get(integer));
                    }
                    return rexBuilder.makeCall(operator,swappedOperands);
                }
                for (Integer integer : swapOrderList) {
                    swappedOperands.add(operands.get(integer));
                }
                return rexBuilder.makeCall(operator,swappedOperands);

            }


            return rexBuilder.makeCall(operator, operands);
        }
        return super.visitCall(call);
    }
}

