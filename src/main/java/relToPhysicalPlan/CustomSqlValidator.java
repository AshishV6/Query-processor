//package relToPhysicalPlan;
//
//import org.apache.calcite.prepare.CalciteCatalogReader;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.sql.*;
//import org.apache.calcite.sql.validate.SqlValidatorImpl;
//import org.apache.calcite.sql.validate.SqlValidatorScope;
//
//public class CustomSqlValidator extends SqlValidatorImpl {
//    public CustomSqlValidator(SqlOperatorTable opTab, CalciteCatalogReader catalogReader, RelDataTypeFactory typeFactory, Config config) {
//        super(opTab, catalogReader, typeFactory, config);
//    }
//
//    @Override
//    public void validateScopedExpression(SqlValidatorScope scope, SqlNode node) {
//        // Implement your logic for validating a scoped expression
//        // You can use various methods from SqlValidatorScope and SqlNode classes
//
//        // For example:
//        if (node instanceof SqlCall) {
//            SqlCall call = (SqlCall) node;
//            for (SqlNode operand : call.getOperandList()) {
//                validate(operand);
//            }
//            // Additional validation logic based on the call
//        } else if (node instanceof SqlIdentifier) {
//            // Validate identifier
//        }
//        // Add more cases based on your requirements
//    }
//
//    @Override
//    public void validateQuery(SqlNode node, SqlValidatorScope scope, SqlValidatorScope operandScope) {
//        // Implement your logic for validating a query
//
//        // For example:
//        if (node instanceof SqlSelect) {
//            validateSelect((SqlSelect) node, scope);
//        } else if (node instanceof SqlInsert) {
//            validateInsert((SqlInsert) node, scope);
//        }
//        // Add more cases based on your requirements
//    }
//
//    // Implement other required methods similarly
//
//    private void validateSelect(SqlSelect select, SqlValidatorScope scope) {
//        // Implement validation logic for SELECT statements
//    }
//
//    private void validateInsert(SqlInsert insert, SqlValidatorScope scope) {
//        // Implement validation logic for INSERT statements
//    }
//}
