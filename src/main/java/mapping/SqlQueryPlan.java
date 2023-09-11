package mapping;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.time.Duration;

import static mapping.HepSqlPlanner.*;

public class SqlQueryPlan {
    public void getQueryPlan(String sSchemaName, String sQueryString)
    {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        SqlParserPlus parser = getPlanner(schema, sSchemaName);
        QueryExecutionNode logPlan = parser.parse("", sSchemaName, sQueryString);

    }

    private SqlParserPlus getPlanner(CalciteSchema rootSchema, String defaultSchema)
    {
        SqlParserPlus parser;
        parser = HepSqlPlanner.create(rootSchema, defaultSchema);

        return parser;
    }


    public void parse(String sQueryId, String schemaName, String query)
    {
        try
        {
            SqlNode sqlNode = parseX(query);
            SqlKind sqlKind = sqlNode.getKind();
            if (!isDdl(sqlKind))
            {
                RelNode optimizedPhase2 = getOptimizedRelNode(sqlNode);
                System.out.println("Plan is : " + optimizedPhase2.explain());
            }

        } catch (UnsupportedOperationException ex)
        {
            String message = ex.getMessage() == null ? "Unsupported operation in sql" : ex.getMessage();
            throw new RuntimeException(message, ex);
        }
        catch(Throwable ex)
        {
            String message = ex.getMessage() == null ? "Error parsing query" : ex.getMessage();
            throw new RuntimeException(message, ex);
        }
    }

    private boolean isDdl(SqlKind sqlKind) {
        return false;
    }

    private SqlNode parseX(String query) {
        return null;
    }


    public RelNode getOptimizedRelNode(SqlNode sqlNode)
    {
        RelNode converted = convert(sqlNode);
        RelNode optimized = optimize(converted);
        QueryCountMetrics.hep();
        return optimized;
    }

    private RelNode optimize(RelNode converted) {
        return converted;
    }

    private RelNode convert(SqlNode sqlNode) {
        return null;
    }

    private class Timeout {
        public Timeout(Duration duration) {
        }
    }
}
