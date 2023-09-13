
import mapping.SqlQueryPlan;
import mapping.SqlStdOperatorTablePlus;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.apache.calcite.sql.parser.SqlParser.config;

public class LengthMappingTest {

    @Test
    public void Lengthmapping() throws Exception {

        String sqlQuery = "SELECT Len('aks', 5), CharLen(5, 'AKA')";
        System.out.println("[Input query]");
        System.out.println(sqlQuery);

        CalciteSchema schema = CalciteSchema.createRootSchema(true);

        SqlParser parser = SqlParser.create(sqlQuery, config());
        SqlNode sqlNode = parser.parseQuery();
        System.out.println("[Parsed query]");
        System.out.println(sqlNode);
        System.out.println();



        SqlQueryPlan queryPlanner = new SqlQueryPlan();
        queryPlanner.getQueryPlan(schema.name,sqlQuery);





    }

}