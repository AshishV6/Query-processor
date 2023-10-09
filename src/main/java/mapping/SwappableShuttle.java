package mapping;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

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

                if (swapOrderList.isEmpty()){
                    // this part of trimming , I don't see point of this
                    if(operands.size()==2){
                        List<RexNode> correctedOperands = new ArrayList<>();
                        correctedOperands.add(operands.get(0));
                        return rexBuilder.makeCall(operator,correctedOperands);
                    }

                    if (operands.size() == 3) {
                        List<RexNode> correctedOperands = new ArrayList<>();
                        correctedOperands.add(operands.get(0));
                        correctedOperands.add(operands.get(1));
                        return rexBuilder.makeCall(operator,correctedOperands);
                    }
                }else {
                    List<RexNode> swappedOperands = new ArrayList<>();

                    for(int i =0; i < swapOrderList.size(); i++){
                        swappedOperands.add(operands.get(swapOrderList.get(i)));
                    }
                    return rexBuilder.makeCall(operator,swappedOperands);
                }


            }


            return rexBuilder.makeCall(operator, operands);
        }
        return super.visitCall(call);
    }
}
