package mapping;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.*;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;


import java.util.ArrayList;
import java.util.List;



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

         node = node.accept(new SwappableShuttle());
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



//    method for query tree traversal
    private boolean containsCommonOperator(SqlNode node) {
        try {
            SqlVisitor<Void> visitor = new SqlBasicVisitor<>() {
                @Override
                public Void visit(SqlCall call) {
                    if (Swappable.COMMON_IDENTIFIER_MAP.containsKey(call.getOperator())) {
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
