package relToPhysicalPlan;

public class CustomValueOperator extends AbstractNode{

    protected TupleNode m_Tuples;

    public void setTuples(TupleNode Tuples) {
        m_Tuples = Tuples;
    }
}
