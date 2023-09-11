package relToPhysicalPlan;




public class ConstNameNode extends ConditionNode {

private final Object m_Value;

public ConstNameNode(Object value){
    m_Value = value;
}

}
