
import mapping.SqlQueryPlan;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

import static org.apache.calcite.sql.parser.SqlParser.config;

public class LengthMappingTest {

    @Test
    public void Lengthmapping() throws Exception {

        String sqlQuery = "SELECT from_utc_timestamp('2021-02-28 12:00:00', 'Asia/Seoul')";
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